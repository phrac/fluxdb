use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::cluster::health::NodeStatus;
use crate::cluster::peer::PeerClient;
use crate::cluster::ring::HashRing;
use crate::config::ClusterConfig;
use crate::database::Database;

/// Routes commands to the correct node in the cluster.
///
/// Point operations (get, insert, update, delete) are routed by hashing
/// `(collection, doc_id)` to find the owner node. Scatter-gather operations
/// (find, count, list_collections) fan out to all nodes in parallel.
///
/// Consults the health status before forwarding — refuses to route to
/// unhealthy nodes, returning an error to the client.
pub struct ClusterRouter {
    local_node_id: String,
    local_db: Arc<Database>,
    ring: Arc<HashRing>,
    peers: HashMap<String, Arc<PeerClient>>,
    health_status: Arc<RwLock<HashMap<String, NodeStatus>>>,
}

impl ClusterRouter {
    /// Create a new router from cluster config and local database.
    pub fn new(config: &ClusterConfig, local_db: Arc<Database>) -> Self {
        let ring = Arc::new(HashRing::new(config.nodes.clone()));

        let mut peers = HashMap::new();
        for node in &config.nodes {
            if node.id != config.node_id {
                peers.insert(
                    node.id.clone(),
                    Arc::new(PeerClient::new(node.client_addr.clone())),
                );
            }
        }

        // Initialize health status (all healthy until checker says otherwise)
        let mut status = HashMap::new();
        for node in &config.nodes {
            if node.id != config.node_id {
                status.insert(
                    node.id.clone(),
                    NodeStatus {
                        healthy: true,
                        last_seen: None,
                        consecutive_failures: 0,
                    },
                );
            }
        }

        ClusterRouter {
            local_node_id: config.node_id.clone(),
            local_db,
            ring,
            peers,
            health_status: Arc::new(RwLock::new(status)),
        }
    }

    /// Set the health status handle (called after health checker is created).
    pub fn set_health_status(&mut self, status: Arc<RwLock<HashMap<String, NodeStatus>>>) {
        self.health_status = status;
    }

    /// Check if a node is currently healthy.
    async fn is_node_healthy(&self, node_id: &str) -> bool {
        if node_id == self.local_node_id {
            return true; // local node is always "healthy"
        }
        let status = self.health_status.read().await;
        status.get(node_id).map_or(false, |s| s.healthy)
    }

    /// Route a command. Returns the response JSON.
    ///
    /// If the command targets a specific document, routes to the owner node.
    /// If it's a scatter-gather operation, fans out to all nodes.
    /// DDL commands are broadcast to all nodes.
    pub async fn route(&self, request: &Value) -> Value {
        let cmd = match request["cmd"].as_str() {
            Some(c) => c,
            None => return json!({"ok": false, "error": "missing 'cmd' field"}),
        };

        match cmd {
            // Point operations — route by (collection, doc_id)
            "get" | "update" | "delete" => self.route_point(request).await,

            "insert" => self.route_insert(request).await,

            // Scatter-gather — fan out to all nodes
            "find" => self.scatter_find(request).await,
            "count" => self.scatter_count(request).await,
            "list_collections" => self.scatter_list_collections(request).await,

            // Broadcast — execute on all nodes
            "create_collection" | "drop_collection" | "create_index" | "drop_index" => {
                self.broadcast(request).await
            }

            // Local-only operations
            "compact" | "stats" | "list_indexes" | "cluster_status" | "flush" => {
                self.execute_local(request)
            }

            _ => json!({"ok": false, "error": format!("unknown command: {cmd}")}),
        }
    }

    /// Route a point operation to the node that owns the document.
    async fn route_point(&self, request: &Value) -> Value {
        let collection = match request["collection"].as_str() {
            Some(c) => c,
            None => return json!({"ok": false, "error": "missing 'collection' field"}),
        };
        let id = match request["id"].as_str() {
            Some(i) => i,
            None => return json!({"ok": false, "error": "missing 'id' field"}),
        };

        match self.ring.owner(collection, id) {
            Some(node) if node.id == self.local_node_id => self.execute_local(request),
            Some(node) => {
                if !self.is_node_healthy(&node.id).await {
                    return json!({"ok": false, "error": format!("node {} is unhealthy", node.id)});
                }
                self.forward_to(&node.id, request).await
            }
            None => json!({"ok": false, "error": "no nodes in cluster"}),
        }
    }

    /// Route an insert — generate ID first if needed, then route.
    async fn route_insert(&self, request: &Value) -> Value {
        let collection = match request["collection"].as_str() {
            Some(c) => c,
            None => return json!({"ok": false, "error": "missing 'collection' field"}),
        };

        let doc_id = request
            .get("document")
            .and_then(|d| d.get("_id"))
            .and_then(|id| id.as_str());

        // If no _id provided, generate one so we can route deterministically
        let (request, id) = if let Some(id) = doc_id {
            (request.clone(), id.to_string())
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            let mut req = request.clone();
            if let Some(doc) = req.get_mut("document").and_then(|d| d.as_object_mut()) {
                doc.insert("_id".to_string(), Value::String(id.clone()));
            }
            (req, id)
        };

        match self.ring.owner(collection, &id) {
            Some(node) if node.id == self.local_node_id => self.execute_local(&request),
            Some(node) => {
                if !self.is_node_healthy(&node.id).await {
                    return json!({"ok": false, "error": format!("node {} is unhealthy", node.id)});
                }
                self.forward_to(&node.id, &request).await
            }
            None => json!({"ok": false, "error": "no nodes in cluster"}),
        }
    }

    /// Scatter a find query to all nodes and merge results.
    async fn scatter_find(&self, request: &Value) -> Value {
        let limit = request["limit"].as_u64().map(|n| n as usize);
        let skip = request["skip"].as_u64().map(|n| n as usize);

        // Each node returns up to (limit + skip) results so we can merge correctly
        let per_node_limit = match (limit, skip) {
            (Some(l), Some(s)) => Some(l + s),
            (Some(l), None) => Some(l),
            _ => None,
        };

        let mut scatter_req = request.clone();
        if let Some(obj) = scatter_req.as_object_mut() {
            // Remove skip from per-node request — we apply it after merge
            obj.remove("skip");
            if let Some(pnl) = per_node_limit {
                obj.insert("limit".to_string(), json!(pnl));
            }
        }

        let responses = self.scatter(&scatter_req).await;

        // Merge results
        let mut all_docs = Vec::new();
        for resp in responses {
            if let Some(docs) = resp.get("documents").and_then(|d| d.as_array()) {
                all_docs.extend(docs.iter().cloned());
            }
        }

        // Apply skip and limit on merged results
        let skip_n = skip.unwrap_or(0);
        let skipped: Vec<Value> = all_docs.into_iter().skip(skip_n).collect();
        let final_docs: Vec<Value> = match limit {
            Some(l) => skipped.into_iter().take(l).collect(),
            None => skipped,
        };

        let count = final_docs.len();
        json!({"ok": true, "documents": final_docs, "count": count})
    }

    /// Scatter a count query and sum the results.
    async fn scatter_count(&self, request: &Value) -> Value {
        let responses = self.scatter(request).await;
        let total: u64 = responses
            .iter()
            .filter_map(|r| r.get("count").and_then(|c| c.as_u64()))
            .sum();
        json!({"ok": true, "count": total})
    }

    /// Scatter list_collections and union the results.
    async fn scatter_list_collections(&self, request: &Value) -> Value {
        let responses = self.scatter(request).await;
        let mut all_collections: Vec<String> = Vec::new();
        for resp in &responses {
            if let Some(cols) = resp.get("collections").and_then(|c| c.as_array()) {
                for col in cols {
                    if let Some(name) = col.as_str() {
                        if !all_collections.contains(&name.to_string()) {
                            all_collections.push(name.to_string());
                        }
                    }
                }
            }
        }
        all_collections.sort();
        json!({"ok": true, "collections": all_collections})
    }

    /// Broadcast a command to all nodes. Returns success if all succeed.
    async fn broadcast(&self, request: &Value) -> Value {
        let responses = self.scatter(request).await;

        // Return the first error, if any
        for resp in &responses {
            if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
                return resp.clone();
            }
        }
        json!({"ok": true})
    }

    /// Fan out a request to all nodes (local + healthy peers) in parallel.
    async fn scatter(&self, request: &Value) -> Vec<Value> {
        let mut handles = Vec::with_capacity(self.peers.len() + 1);

        // Local execution
        let local_result = self.execute_local(request);

        // Remote executions in parallel (only healthy nodes)
        let health = self.health_status.read().await;
        for (node_id, peer) in &self.peers {
            let is_healthy = health.get(node_id).map_or(false, |s| s.healthy);
            if !is_healthy {
                eprintln!("fluxdb: skipping unhealthy node {node_id} in scatter");
                continue;
            }
            let peer = Arc::clone(peer);
            let req = request.clone();
            let node_id = node_id.clone();
            handles.push(tokio::spawn(async move {
                match peer.forward(req).await {
                    Ok(resp) => resp,
                    Err(e) => json!({"ok": false, "error": format!("node {node_id}: {e}")}),
                }
            }));
        }
        drop(health); // Release read lock before awaiting

        let mut results = vec![local_result];
        for handle in handles {
            match handle.await {
                Ok(resp) => results.push(resp),
                Err(e) => results.push(json!({"ok": false, "error": format!("task join: {e}")})),
            }
        }

        results
    }

    /// Forward a request to a specific peer node.
    async fn forward_to(&self, node_id: &str, request: &Value) -> Value {
        match self.peers.get(node_id) {
            Some(peer) => match peer.forward(request.clone()).await {
                Ok(resp) => resp,
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            },
            None => json!({"ok": false, "error": format!("unknown node: {node_id}")}),
        }
    }

    /// Execute a command on the local database (blocking).
    fn execute_local(&self, request: &Value) -> Value {
        crate::server::process_command(&self.local_db, request)
    }

    /// Get the hash ring (for status/debugging).
    pub fn ring(&self) -> &HashRing {
        &self.ring
    }

    /// Get the local node ID.
    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Get peer clients (for health checks).
    pub fn peers(&self) -> &HashMap<String, Arc<PeerClient>> {
        &self.peers
    }

    /// Get health status handle.
    pub fn health_status(&self) -> Arc<RwLock<HashMap<String, NodeStatus>>> {
        Arc::clone(&self.health_status)
    }
}
