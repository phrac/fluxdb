use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::config::Config;
use crate::database::Database;

/// TCP server that accepts JSON commands over newline-delimited protocol.
pub struct Server {
    db: Arc<Database>,
    addr: String,
    #[cfg(feature = "cluster")]
    router: Option<Arc<crate::cluster::router::ClusterRouter>>,
}

impl Server {
    /// Create a server from a Config.
    pub fn from_config(config: &Config) -> crate::error::Result<Self> {
        let db = Database::open_with_config(config, None)?;
        let db = Arc::new(db);

        #[cfg(feature = "cluster")]
        let router = if config.cluster.enabled {
            Some(Arc::new(crate::cluster::router::ClusterRouter::new(
                &config.cluster,
                Arc::clone(&db),
            )))
        } else {
            None
        };

        Ok(Server {
            db,
            addr: config.listen.clone(),
            #[cfg(feature = "cluster")]
            router,
        })
    }

    /// Get a shared reference to the underlying database.
    pub fn db(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }

    /// Start the health checker background task (cluster mode only).
    #[cfg(feature = "cluster")]
    pub fn start_health_checker(&self) {
        if let Some(ref router) = self.router {
            let checker = crate::cluster::health::HealthChecker::new(router.peers().clone());
            tokio::spawn(checker.run());
        }
    }

    pub async fn run(&self) -> crate::error::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        eprintln!("FluxDB listening on {}", self.addr);

        #[cfg(feature = "cluster")]
        if self.router.is_some() {
            self.start_health_checker();
        }

        loop {
            let (stream, peer) = listener.accept().await?;
            eprintln!("Client connected: {peer}");
            let db = Arc::clone(&self.db);
            #[cfg(feature = "cluster")]
            let router = self.router.clone();
            tokio::spawn(async move {
                #[cfg(feature = "cluster")]
                let result = handle_client(stream, db, router).await;
                #[cfg(not(feature = "cluster"))]
                let result = handle_client(stream, db).await;
                if let Err(e) = result {
                    eprintln!("Client error: {e}");
                }
            });
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    db: Arc<Database>,
    #[cfg(feature = "cluster")] router: Option<Arc<crate::cluster::router::ClusterRouter>>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => {
                // In cluster mode: route unless the request is already routed to us
                #[cfg(feature = "cluster")]
                {
                    let is_routed = request
                        .get("_routed")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    if !is_routed {
                        if let Some(ref router) = router {
                            router.route(&request).await
                        } else {
                            let db = Arc::clone(&db);
                            tokio::task::spawn_blocking(move || process_command(&db, &request))
                                .await
                                .unwrap_or_else(|e| {
                                    json!({"ok": false, "error": format!("internal error: {e}")})
                                })
                        }
                    } else {
                        // Already routed — execute locally
                        let db = Arc::clone(&db);
                        tokio::task::spawn_blocking(move || process_command(&db, &request))
                            .await
                            .unwrap_or_else(|e| {
                                json!({"ok": false, "error": format!("internal error: {e}")})
                            })
                    }
                }

                #[cfg(not(feature = "cluster"))]
                {
                    let db = Arc::clone(&db);
                    tokio::task::spawn_blocking(move || process_command(&db, &request))
                        .await
                        .unwrap_or_else(|e| {
                            json!({"ok": false, "error": format!("internal error: {e}")})
                        })
                }
            }
            Err(e) => json!({"ok": false, "error": format!("invalid JSON: {e}")}),
        };

        let mut response_str = serde_json::to_string(&response)?;
        response_str.push('\n');
        writer.write_all(response_str.as_bytes()).await?;
        writer.flush().await?;
    }

    Ok(())
}

pub fn process_command(db: &Database, request: &Value) -> Value {
    let cmd = match request["cmd"].as_str() {
        Some(c) => c,
        None => return json!({"ok": false, "error": "missing 'cmd' field"}),
    };

    match cmd {
        "create_collection" => {
            let name = match request["name"].as_str() {
                Some(n) => n,
                None => return json!({"ok": false, "error": "missing 'name' field"}),
            };
            match db.create_collection(name) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "drop_collection" => {
            let name = match request["name"].as_str() {
                Some(n) => n,
                None => return json!({"ok": false, "error": "missing 'name' field"}),
            };
            match db.drop_collection(name) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "list_collections" => {
            let collections = db.list_collections();
            json!({"ok": true, "collections": collections})
        }

        "insert" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let document = match request.get("document") {
                Some(d) if d.is_object() => d.clone(),
                _ => {
                    return json!({"ok": false, "error": "missing or invalid 'document' field"})
                }
            };
            match db.insert(collection, document) {
                Ok(id) => json!({"ok": true, "id": id}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "get" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let id = match request["id"].as_str() {
                Some(i) => i,
                None => return json!({"ok": false, "error": "missing 'id' field"}),
            };
            match db.get(collection, id) {
                Ok(doc) => json!({"ok": true, "document": doc}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "update" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let id = match request["id"].as_str() {
                Some(i) => i,
                None => return json!({"ok": false, "error": "missing 'id' field"}),
            };
            let document = match request.get("document") {
                Some(d) if d.is_object() => d.clone(),
                _ => {
                    return json!({"ok": false, "error": "missing or invalid 'document' field"})
                }
            };
            match db.update(collection, id, document) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "delete" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let id = match request["id"].as_str() {
                Some(i) => i,
                None => return json!({"ok": false, "error": "missing 'id' field"}),
            };
            match db.delete(collection, id) {
                Ok(deleted) => json!({"ok": true, "deleted": deleted}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "find" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let filter = request.get("filter").cloned().unwrap_or(json!({}));
            let projection = request.get("projection").cloned();
            let limit = request["limit"].as_u64().map(|n| n as usize);
            let skip = request["skip"].as_u64().map(|n| n as usize);

            match db.find(collection, filter, projection, limit, skip) {
                Ok(docs) => json!({"ok": true, "documents": docs, "count": docs.len()}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "count" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let filter = request.get("filter").cloned().unwrap_or(json!({}));

            match db.count(collection, filter) {
                Ok(count) => json!({"ok": true, "count": count}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "create_index" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let field = match request["field"].as_str() {
                Some(f) => f,
                None => return json!({"ok": false, "error": "missing 'field' field"}),
            };
            match db.create_index(collection, field) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "drop_index" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            let field = match request["field"].as_str() {
                Some(f) => f,
                None => return json!({"ok": false, "error": "missing 'field' field"}),
            };
            match db.drop_index(collection, field) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "list_indexes" => {
            let collection = match request["collection"].as_str() {
                Some(c) => c,
                None => return json!({"ok": false, "error": "missing 'collection' field"}),
            };
            match db.list_indexes(collection) {
                Ok(indexes) => json!({"ok": true, "indexes": indexes}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "compact" => match db.compact() {
            Ok(()) => json!({"ok": true}),
            Err(e) => json!({"ok": false, "error": e.to_string()}),
        },

        "stats" => {
            let stats = db.stats();
            json!({"ok": true, "stats": stats})
        }

        "cluster_status" => {
            #[cfg(feature = "cluster")]
            {
                json!({
                    "ok": true,
                    "cluster_enabled": true,
                    "node": db.name,
                })
            }
            #[cfg(not(feature = "cluster"))]
            {
                json!({"ok": true, "cluster_enabled": false})
            }
        }

        other => {
            json!({"ok": false, "error": format!("unknown command: {other}")})
        }
    }
}
