use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::database::Database;

/// TCP server that accepts JSON commands over newline-delimited protocol.
///
/// Each request is a single JSON object on one line. Each response is a single
/// JSON object on one line.
///
/// Commands:
///   {"cmd": "create_collection", "name": "users"}
///   {"cmd": "drop_collection", "name": "users"}
///   {"cmd": "list_collections"}
///   {"cmd": "insert", "collection": "users", "document": {...}}
///   {"cmd": "get", "collection": "users", "id": "abc"}
///   {"cmd": "update", "collection": "users", "id": "abc", "document": {...}}
///   {"cmd": "delete", "collection": "users", "id": "abc"}
///   {"cmd": "find", "collection": "users", "filter": {...}, "projection": {...}, "limit": 10, "skip": 0}
///   {"cmd": "count", "collection": "users", "filter": {...}}
///   {"cmd": "compact"}
///   {"cmd": "stats"}
pub struct Server {
    db: Arc<Mutex<Database>>,
    addr: String,
}

impl Server {
    pub fn new(data_dir: PathBuf, addr: &str) -> crate::error::Result<Self> {
        let db = Database::open(&data_dir)?;
        Ok(Server {
            db: Arc::new(Mutex::new(db)),
            addr: addr.to_string(),
        })
    }

    pub async fn run(&self) -> crate::error::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        eprintln!("FluxDB listening on {}", self.addr);

        loop {
            let (stream, peer) = listener.accept().await?;
            eprintln!("Client connected: {peer}");
            let db = Arc::clone(&self.db);
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, db).await {
                    eprintln!("Client error: {e}");
                }
            });
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    db: Arc<Mutex<Database>>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => process_command(&db, &request).await,
            Err(e) => json!({"ok": false, "error": format!("invalid JSON: {e}")}),
        };

        let mut response_str = serde_json::to_string(&response)?;
        response_str.push('\n');
        writer.write_all(response_str.as_bytes()).await?;
        writer.flush().await?;
    }

    Ok(())
}

async fn process_command(db: &Arc<Mutex<Database>>, request: &Value) -> Value {
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
            let mut db = db.lock().await;
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
            let mut db = db.lock().await;
            match db.drop_collection(name) {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "list_collections" => {
            let db = db.lock().await;
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
                _ => return json!({"ok": false, "error": "missing or invalid 'document' field"}),
            };
            let mut db = db.lock().await;
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
            let db = db.lock().await;
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
                _ => return json!({"ok": false, "error": "missing or invalid 'document' field"}),
            };
            let mut db = db.lock().await;
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
            let mut db = db.lock().await;
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

            let db = db.lock().await;
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

            let db = db.lock().await;
            match db.count(collection, filter) {
                Ok(count) => json!({"ok": true, "count": count}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "compact" => {
            let mut db = db.lock().await;
            match db.compact() {
                Ok(()) => json!({"ok": true}),
                Err(e) => json!({"ok": false, "error": e.to_string()}),
            }
        }

        "stats" => {
            let db = db.lock().await;
            let stats = db.stats();
            json!({"ok": true, "stats": stats})
        }

        other => {
            json!({"ok": false, "error": format!("unknown command: {other}")})
        }
    }
}

