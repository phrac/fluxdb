use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::database::Database;

/// TCP server that accepts JSON commands over newline-delimited protocol.
pub struct Server {
    db: Arc<Database>,
    addr: String,
    max_connections: usize,
    auth_token: Option<String>,
    max_document_bytes: usize,
    request_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    slow_query_ms: u64,
    #[cfg(feature = "cluster")]
    router: Option<Arc<crate::cluster::router::ClusterRouter>>,
    #[cfg(feature = "cluster")]
    cluster_health_check_interval_secs: u64,
    #[cfg(feature = "cluster")]
    cluster_unhealthy_threshold: u32,
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

        let auth_token = if config.auth.enabled {
            Some(config.auth.token.clone())
        } else {
            None
        };

        let request_timeout = if config.server.request_timeout_secs > 0 {
            Some(Duration::from_secs(config.server.request_timeout_secs))
        } else {
            None
        };
        let idle_timeout = if config.server.idle_timeout_secs > 0 {
            Some(Duration::from_secs(config.server.idle_timeout_secs))
        } else {
            None
        };

        Ok(Server {
            db,
            addr: config.listen.clone(),
            max_connections: config.limits.max_connections,
            auth_token,
            max_document_bytes: config.limits.max_document_bytes,
            request_timeout,
            idle_timeout,
            slow_query_ms: config.log.slow_query_ms,
            #[cfg(feature = "cluster")]
            router,
            #[cfg(feature = "cluster")]
            cluster_health_check_interval_secs: config.cluster.health_check_interval_secs,
            #[cfg(feature = "cluster")]
            cluster_unhealthy_threshold: config.cluster.unhealthy_threshold,
        })
    }

    /// Get a shared reference to the underlying database.
    pub fn db(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }

    /// Get the auth token (for Redis server to share).
    pub fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_deref()
    }

    /// Get max connections setting.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Start the health checker background task (cluster mode only).
    #[cfg(feature = "cluster")]
    pub fn start_health_checker(&self) {
        if let Some(ref router) = self.router {
            let checker = crate::cluster::health::HealthChecker::new(
                router.peers().clone(),
                self.cluster_health_check_interval_secs,
                self.cluster_unhealthy_threshold,
            );
            tokio::spawn(checker.run());
        }
    }

    pub async fn run(&self, mut shutdown: tokio::sync::broadcast::Receiver<()>) -> crate::error::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        eprintln!("FluxDB listening on {}", self.addr);

        #[cfg(feature = "cluster")]
        if self.router.is_some() {
            self.start_health_checker();
        }

        // Connection limiter (0 = unlimited, use a very high permit count)
        let max_conn = if self.max_connections > 0 {
            self.max_connections
        } else {
            usize::MAX >> 1
        };
        let semaphore = Arc::new(Semaphore::new(max_conn));

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result?;

                    // Acquire connection permit
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            eprintln!("Connection limit reached, rejecting {peer}");
                            // Write error and close
                            let stream = stream;
                            let msg = serde_json::to_string(&json!({"ok": false, "error": "too many connections"})).unwrap();
                            let _ = stream.try_write(format!("{msg}\n").as_bytes());
                            continue;
                        }
                    };

                    eprintln!("Client connected: {peer}");
                    let db = Arc::clone(&self.db);
                    let auth_token = self.auth_token.clone();
                    let max_doc_bytes = self.max_document_bytes;
                    let request_timeout = self.request_timeout;
                    let idle_timeout = self.idle_timeout;
                    let slow_query_ms = self.slow_query_ms;
                    #[cfg(feature = "cluster")]
                    let router = self.router.clone();

                    tokio::spawn(async move {
                        #[cfg(feature = "cluster")]
                        let result = handle_client(stream, db, router, auth_token.as_deref(), max_doc_bytes, request_timeout, idle_timeout, slow_query_ms).await;
                        #[cfg(not(feature = "cluster"))]
                        let result = handle_client(stream, db, auth_token.as_deref(), max_doc_bytes, request_timeout, idle_timeout, slow_query_ms).await;
                        if let Err(e) = result {
                            eprintln!("Client error: {e}");
                        }
                        drop(permit); // Release connection slot
                    });
                }
                _ = shutdown.recv() => {
                    eprintln!("Shutting down server...");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_client(
    stream: TcpStream,
    db: Arc<Database>,
    #[cfg(feature = "cluster")] router: Option<Arc<crate::cluster::router::ClusterRouter>>,
    auth_token: Option<&str>,
    max_document_bytes: usize,
    request_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    slow_query_ms: u64,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut authenticated = auth_token.is_none(); // If no auth required, start authenticated

    loop {
        // Wait for next line with optional idle timeout
        let line = if let Some(timeout) = idle_timeout {
            match tokio::time::timeout(timeout, lines.next_line()).await {
                Ok(result) => result?,
                Err(_) => {
                    let msg = json!({"ok": false, "error": "idle timeout"});
                    let mut s = serde_json::to_string(&msg)?;
                    s.push('\n');
                    writer.write_all(s.as_bytes()).await?;
                    writer.flush().await?;
                    return Ok(());
                }
            }
        } else {
            lines.next_line().await?
        };
        let line = match line {
            Some(l) => l,
            None => break,
        };
        if line.is_empty() {
            continue;
        }

        // Reject oversized requests early
        if line.len() > max_document_bytes + 1024 {
            let response = json!({"ok": false, "error": "request too large"});
            let mut response_str = serde_json::to_string(&response)?;
            response_str.push('\n');
            writer.write_all(response_str.as_bytes()).await?;
            writer.flush().await?;
            continue;
        }

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => {
                // Handle auth command
                if !authenticated {
                    if let Some(cmd) = request["cmd"].as_str() {
                        if cmd == "auth" {
                            if let Some(token) = request["token"].as_str() {
                                if Some(token) == auth_token {
                                    authenticated = true;
                                    json!({"ok": true})
                                } else {
                                    json!({"ok": false, "error": "invalid auth token"})
                                }
                            } else {
                                json!({"ok": false, "error": "missing 'token' field"})
                            }
                        } else {
                            json!({"ok": false, "error": "authentication required: send {\"cmd\":\"auth\",\"token\":\"...\"}"})
                        }
                    } else {
                        json!({"ok": false, "error": "authentication required"})
                    }
                } else {
                    let cmd_name = request["cmd"].as_str().unwrap_or("?").to_string();
                    let start = Instant::now();

                    let execute = async {
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
                    };

                    let result = if let Some(timeout) = request_timeout {
                        match tokio::time::timeout(timeout, execute).await {
                            Ok(resp) => resp,
                            Err(_) => json!({"ok": false, "error": "request timeout"}),
                        }
                    } else {
                        execute.await
                    };

                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    if slow_query_ms > 0 && elapsed_ms >= slow_query_ms {
                        eprintln!(
                            "fluxdb: slow query: cmd={} elapsed={}ms",
                            cmd_name, elapsed_ms
                        );
                    }

                    result
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
            let sort = request.get("sort").cloned();

            match db.find(collection, filter, projection, limit, skip, sort) {
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

        "flush" => match db.flush() {
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
