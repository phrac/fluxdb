use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::database::Database;

/// Redis-compatible server that speaks the RESP protocol.
///
/// Maps Redis commands to FluxDB operations:
/// - SET/GET/DEL: store and retrieve documents by key
/// - HSET/HGET/HGETALL/HDEL: manipulate document fields
/// - SELECT: switch the active collection
/// - KEYS/EXISTS/DBSIZE: query key space
/// - PING/INFO/COMMAND: server management
/// - AUTH: token-based authentication
pub struct RedisServer {
    db: Arc<Database>,
    addr: String,
    max_connections: usize,
    auth_token: Option<String>,
    max_bulk_bytes: usize,
    max_array_len: usize,
}

impl RedisServer {
    pub fn new(
        db: Arc<Database>,
        addr: String,
        max_connections: usize,
        auth_token: Option<String>,
        max_bulk_bytes: usize,
        max_array_len: usize,
    ) -> Self {
        RedisServer {
            db,
            addr,
            max_connections,
            auth_token,
            max_bulk_bytes,
            max_array_len,
        }
    }

    pub async fn run(&self, shutdown: &mut tokio::sync::broadcast::Receiver<()>) -> crate::error::Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        eprintln!("FluxDB Redis-compatible server on {}", self.addr);

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
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(p) => p,
                        Err(_) => {
                            eprintln!("Redis connection limit reached, rejecting {peer}");
                            let stream = stream;
                            let _ = stream.try_write(b"-ERR too many connections\r\n");
                            continue;
                        }
                    };

                    eprintln!("Redis client connected: {peer}");
                    let db = Arc::clone(&self.db);
                    let auth_token = self.auth_token.clone();
                    let max_bulk = self.max_bulk_bytes;
                    let max_array = self.max_array_len;
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(stream, db, auth_token, max_bulk, max_array).await {
                            eprintln!("Redis client error: {e}");
                        }
                        drop(permit);
                    });
                }
                _ = shutdown.recv() => {
                    eprintln!("Redis server shutting down...");
                    break;
                }
            }
        }
        Ok(())
    }
}

// ── RESP types ──────────────────────────────────────────────────────────────

enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{s}\r\n").into_bytes(),
            RespValue::Error(s) => format!("-{s}\r\n").into_bytes(),
            RespValue::Integer(n) => format!(":{n}\r\n").into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(data)) => {
                let mut buf = format!("${}\r\n", data.len()).into_bytes();
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut buf = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    buf.extend(item.serialize());
                }
                buf
            }
        }
    }

    fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    fn err(msg: impl Into<String>) -> Self {
        RespValue::Error(format!("ERR {}", msg.into()))
    }

    fn bulk(s: impl AsRef<[u8]>) -> Self {
        RespValue::BulkString(Some(s.as_ref().to_vec()))
    }

    fn null_bulk() -> Self {
        RespValue::BulkString(None)
    }
}

// ── RESP parser ─────────────────────────────────────────────────────────────

struct RespLimits {
    max_bulk_bytes: usize,
    max_array_len: usize,
}

fn read_resp<'a, R: AsyncBufReadExt + AsyncReadExt + Unpin + Send + 'a>(
    reader: &'a mut R,
    limits: &'a RespLimits,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<Option<RespValue>>> + Send + 'a>> {
    Box::pin(read_resp_inner(reader, limits))
}

async fn read_resp_inner<R: AsyncBufReadExt + AsyncReadExt + Unpin + Send>(
    reader: &mut R,
    limits: &RespLimits,
) -> std::io::Result<Option<RespValue>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None); // connection closed
    }
    let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

    if line.is_empty() {
        return Ok(Some(RespValue::SimpleString(String::new())));
    }

    let prefix = line.as_bytes()[0];
    let body = &line[1..];

    match prefix {
        b'+' => Ok(Some(RespValue::SimpleString(body.to_string()))),
        b'-' => Ok(Some(RespValue::Error(body.to_string()))),
        b':' => {
            let n: i64 = body.parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP integer")
            })?;
            Ok(Some(RespValue::Integer(n)))
        }
        b'$' => {
            let len: i64 = body.parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP bulk string length")
            })?;
            if len < 0 {
                return Ok(Some(RespValue::BulkString(None)));
            }
            let len = len as usize;
            if len > limits.max_bulk_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("RESP bulk string size {len} exceeds limit {}", limits.max_bulk_bytes),
                ));
            }
            let mut buf = vec![0u8; len + 2]; // +2 for \r\n
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            Ok(Some(RespValue::BulkString(Some(buf))))
        }
        b'*' => {
            let count: i64 = body.parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid RESP array count")
            })?;
            if count < 0 {
                return Ok(Some(RespValue::Array(None)));
            }
            let count = count as usize;
            if count > limits.max_array_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("RESP array count {count} exceeds limit {}", limits.max_array_len),
                ));
            }
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                match read_resp(reader, limits).await? {
                    Some(v) => items.push(v),
                    None => return Ok(None),
                }
            }
            Ok(Some(RespValue::Array(Some(items))))
        }
        // Inline command (no prefix) — treat the whole line as space-separated args
        _ => {
            let full = format!("{}{}", prefix as char, body);
            let parts: Vec<RespValue> = full
                .split_whitespace()
                .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                .collect();
            Ok(Some(RespValue::Array(Some(parts))))
        }
    }
}

fn resp_to_args(value: RespValue) -> Option<Vec<Vec<u8>>> {
    match value {
        RespValue::Array(Some(items)) => {
            let mut args = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    RespValue::BulkString(Some(data)) => args.push(data),
                    RespValue::SimpleString(s) => args.push(s.into_bytes()),
                    _ => return None,
                }
            }
            Some(args)
        }
        _ => None,
    }
}

// ── Client handler ──────────────────────────────────────────────────────────

async fn handle_client(
    stream: TcpStream,
    db: Arc<Database>,
    auth_token: Option<String>,
    max_bulk: usize,
    max_array: usize,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut collection = "default".to_string();
    let mut authenticated = auth_token.is_none();
    let limits = RespLimits {
        max_bulk_bytes: max_bulk,
        max_array_len: max_array,
    };

    // Auto-create the default collection
    let _ = db.create_collection("default");

    loop {
        let value = match read_resp(&mut reader, &limits).await {
            Ok(Some(v)) => v,
            Ok(None) => break, // client disconnected
            Err(e) => {
                // Protocol error (e.g. oversized bulk string)
                let resp = RespValue::err(format!("protocol error: {e}"));
                writer.write_all(&resp.serialize()).await?;
                writer.flush().await?;
                break; // Close connection on protocol violation
            }
        };

        let args = match resp_to_args(value) {
            Some(a) if !a.is_empty() => a,
            _ => {
                writer.write_all(&RespValue::err("invalid command").serialize()).await?;
                continue;
            }
        };

        let cmd = match std::str::from_utf8(&args[0]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => {
                writer.write_all(&RespValue::err("invalid UTF-8 in command").serialize()).await?;
                continue;
            }
        };

        // Handle AUTH before anything else
        if cmd == "AUTH" {
            if auth_token.is_none() {
                writer.write_all(&RespValue::err("Client sent AUTH, but no password is set").serialize()).await?;
            } else if args.len() < 2 {
                writer.write_all(&RespValue::err("wrong number of arguments for 'auth' command").serialize()).await?;
            } else {
                let token = String::from_utf8_lossy(&args[1]);
                if Some(token.as_ref()) == auth_token.as_deref() {
                    authenticated = true;
                    writer.write_all(&RespValue::ok().serialize()).await?;
                } else {
                    writer.write_all(&RespValue::err("invalid password").serialize()).await?;
                }
            }
            writer.flush().await?;
            continue;
        }

        // Allow PING and QUIT without auth
        if !authenticated && cmd != "PING" && cmd != "QUIT" {
            writer.write_all(&RespValue::err("NOAUTH Authentication required.").serialize()).await?;
            writer.flush().await?;
            continue;
        }

        let is_quit = cmd == "QUIT";

        // Handle SELECT locally (needs to update connection state)
        if cmd == "SELECT" && args.len() >= 2 {
            let new_col = match std::str::from_utf8(&args[1]) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    writer.write_all(&RespValue::err("invalid UTF-8 in collection name").serialize()).await?;
                    writer.flush().await?;
                    continue;
                }
            };
            let _ = db.create_collection(&new_col);
            collection = new_col;
            writer.write_all(&RespValue::ok().serialize()).await?;
            writer.flush().await?;
            continue;
        }

        let db_clone = Arc::clone(&db);
        let col = collection.clone();

        let response = tokio::task::spawn_blocking(move || {
            dispatch_command(&db_clone, &col, &cmd, &args[1..])
        })
        .await
        .unwrap_or_else(|e| RespValue::err(format!("internal error: {e}")));

        writer.write_all(&response.serialize()).await?;
        writer.flush().await?;

        if is_quit {
            break;
        }
    }

    Ok(())
}

// ── Command dispatch ────────────────────────────────────────────────────────

fn dispatch_command(
    db: &Database,
    collection: &str,
    cmd: &str,
    args: &[Vec<u8>],
) -> RespValue {
    match cmd {
        "PING" => {
            if args.is_empty() {
                RespValue::SimpleString("PONG".to_string())
            } else {
                RespValue::bulk(&args[0])
            }
        }

        "ECHO" => {
            if args.is_empty() {
                return RespValue::err("wrong number of arguments for 'echo' command");
            }
            RespValue::bulk(&args[0])
        }

        "SET" => cmd_set(db, collection, args),
        "GET" => cmd_get(db, collection, args),
        "DEL" => cmd_del(db, collection, args),
        "EXISTS" => cmd_exists(db, collection, args),
        "KEYS" => cmd_keys(db, collection, args),
        "MSET" => cmd_mset(db, collection, args),
        "MGET" => cmd_mget(db, collection, args),
        "HSET" => cmd_hset(db, collection, args),
        "HGET" => cmd_hget(db, collection, args),
        "HGETALL" => cmd_hgetall(db, collection, args),
        "HDEL" => cmd_hdel(db, collection, args),
        "HLEN" => cmd_hlen(db, collection, args),

        "DBSIZE" => {
            match db.count(collection, json!({})) {
                Ok(n) => RespValue::Integer(n as i64),
                Err(e) => RespValue::err(e.to_string()),
            }
        }

        "FLUSHDB" => {
            let _ = db.drop_collection(collection);
            let _ = db.create_collection(collection);
            RespValue::ok()
        }

        "INFO" => {
            let stats = db.stats();
            let info = format!(
                "# Server\r\nfluxdb_version:{}\r\n# Keyspace\r\n{}\r\n",
                env!("CARGO_PKG_VERSION"),
                serde_json::to_string_pretty(&stats).unwrap_or_default()
            );
            RespValue::bulk(info)
        }

        "COMMAND" => {
            // Minimal stub for client compatibility
            if args.first().map(|a| String::from_utf8_lossy(a).to_uppercase()) == Some("DOCS".into()) {
                RespValue::Array(Some(vec![]))
            } else if args.first().map(|a| String::from_utf8_lossy(a).to_uppercase()) == Some("COUNT".into()) {
                RespValue::Integer(16)
            } else {
                RespValue::Array(Some(vec![]))
            }
        }

        "QUIT" => RespValue::ok(),

        other => RespValue::err(format!("unknown command '{other}'")),
    }
}

// ── String commands ─────────────────────────────────────────────────────────

fn cmd_set(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::err("wrong number of arguments for 'set' command");
    }
    let key = String::from_utf8_lossy(&args[0]).to_string();
    let value = String::from_utf8_lossy(&args[1]).to_string();

    // Delete existing doc if present, then insert
    let _ = db.delete(collection, &key);
    let doc = json!({"_id": key, "_value": value});
    match db.insert(collection, doc) {
        Ok(_) => RespValue::ok(),
        Err(e) => RespValue::err(e.to_string()),
    }
}

fn cmd_get(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::err("wrong number of arguments for 'get' command");
    }
    let key = String::from_utf8_lossy(&args[0]);

    match db.get(collection, &key) {
        Ok(doc) => {
            // If doc has _value field, return that string; otherwise return JSON
            if let Some(v) = doc.get("_value") {
                if let Some(s) = v.as_str() {
                    return RespValue::bulk(s);
                }
                return RespValue::bulk(v.to_string());
            }
            // Return full doc as JSON (excluding _id)
            let mut obj = doc.as_object().cloned().unwrap_or_default();
            obj.remove("_id");
            RespValue::bulk(serde_json::to_string(&Value::Object(obj)).unwrap_or_default())
        }
        Err(_) => RespValue::null_bulk(),
    }
}

fn cmd_del(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    let mut deleted = 0i64;
    for arg in args {
        let key = String::from_utf8_lossy(arg);
        match db.delete(collection, &key) {
            Ok(true) => deleted += 1,
            _ => {}
        }
    }
    RespValue::Integer(deleted)
}

fn cmd_exists(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    let mut count = 0i64;
    for arg in args {
        let key = String::from_utf8_lossy(arg);
        if db.get(collection, &key).is_ok() {
            count += 1;
        }
    }
    RespValue::Integer(count)
}

fn cmd_keys(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    let pattern = if args.is_empty() {
        "*".to_string()
    } else {
        String::from_utf8_lossy(&args[0]).to_string()
    };

    // Fetch all docs and extract IDs
    let docs = match db.find(collection, json!({}), Some(json!({"_id": 1})), None, None, None) {
        Ok(d) => d,
        Err(e) => return RespValue::err(e.to_string()),
    };

    let keys: Vec<RespValue> = docs
        .iter()
        .filter_map(|d| d.get("_id").and_then(|v| v.as_str()).map(String::from))
        .filter(|id| glob_match(&pattern, id))
        .map(|id| RespValue::bulk(id))
        .collect();

    RespValue::Array(Some(keys))
}

fn cmd_mset(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 || args.len() % 2 != 0 {
        return RespValue::err("wrong number of arguments for 'mset' command");
    }
    for pair in args.chunks(2) {
        let key = String::from_utf8_lossy(&pair[0]).to_string();
        let value = String::from_utf8_lossy(&pair[1]).to_string();
        let _ = db.delete(collection, &key);
        if let Err(e) = db.insert(collection, json!({"_id": key, "_value": value})) {
            return RespValue::err(e.to_string());
        }
    }
    RespValue::ok()
}

fn cmd_mget(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    let mut results = Vec::with_capacity(args.len());
    for arg in args {
        let key = String::from_utf8_lossy(arg);
        match db.get(collection, &key) {
            Ok(doc) => {
                if let Some(v) = doc.get("_value").and_then(|v| v.as_str()) {
                    results.push(RespValue::bulk(v));
                } else {
                    let mut obj = doc.as_object().cloned().unwrap_or_default();
                    obj.remove("_id");
                    results.push(RespValue::bulk(
                        serde_json::to_string(&Value::Object(obj)).unwrap_or_default(),
                    ));
                }
            }
            Err(_) => results.push(RespValue::null_bulk()),
        }
    }
    RespValue::Array(Some(results))
}

// ── Hash commands ───────────────────────────────────────────────────────────

fn cmd_hset(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return RespValue::err("wrong number of arguments for 'hset' command");
    }
    let key = String::from_utf8_lossy(&args[0]).to_string();
    let mut fields_added = 0i64;

    // Get existing doc or start fresh
    let mut obj = match db.get(collection, &key) {
        Ok(doc) => {
            let mut m = doc.as_object().cloned().unwrap_or_default();
            m.remove("_id");
            m
        }
        Err(_) => serde_json::Map::new(),
    };

    for pair in args[1..].chunks(2) {
        let field = String::from_utf8_lossy(&pair[0]).to_string();
        let value_str = String::from_utf8_lossy(&pair[1]).to_string();
        // Try to parse as JSON value, fall back to string
        let value: Value = serde_json::from_str(&value_str).unwrap_or(Value::String(value_str));
        if !obj.contains_key(&field) {
            fields_added += 1;
        }
        obj.insert(field, value);
    }

    // Remove _value field if switching from string to hash semantics
    obj.remove("_value");

    let _ = db.delete(collection, &key);
    let mut full = serde_json::Map::new();
    full.insert("_id".to_string(), Value::String(key));
    for (k, v) in obj {
        full.insert(k, v);
    }
    match db.insert(collection, Value::Object(full)) {
        Ok(_) => RespValue::Integer(fields_added),
        Err(e) => RespValue::err(e.to_string()),
    }
}

fn cmd_hget(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::err("wrong number of arguments for 'hget' command");
    }
    let key = String::from_utf8_lossy(&args[0]);
    let field = String::from_utf8_lossy(&args[1]);

    match db.get(collection, &key) {
        Ok(doc) => match doc.get(field.as_ref()) {
            Some(Value::String(s)) => RespValue::bulk(s.as_str()),
            Some(v) => RespValue::bulk(v.to_string()),
            None => RespValue::null_bulk(),
        },
        Err(_) => RespValue::null_bulk(),
    }
}

fn cmd_hgetall(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::err("wrong number of arguments for 'hgetall' command");
    }
    let key = String::from_utf8_lossy(&args[0]);

    match db.get(collection, &key) {
        Ok(doc) => {
            let obj = doc.as_object().cloned().unwrap_or_default();
            let mut items = Vec::new();
            for (k, v) in &obj {
                if k == "_id" {
                    continue;
                }
                items.push(RespValue::bulk(k.as_str()));
                match v {
                    Value::String(s) => items.push(RespValue::bulk(s.as_str())),
                    other => items.push(RespValue::bulk(other.to_string())),
                }
            }
            RespValue::Array(Some(items))
        }
        Err(_) => RespValue::Array(Some(vec![])),
    }
}

fn cmd_hdel(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.len() < 2 {
        return RespValue::err("wrong number of arguments for 'hdel' command");
    }
    let key = String::from_utf8_lossy(&args[0]).to_string();

    let mut obj = match db.get(collection, &key) {
        Ok(doc) => {
            let mut m = doc.as_object().cloned().unwrap_or_default();
            m.remove("_id");
            m
        }
        Err(_) => return RespValue::Integer(0),
    };

    let mut removed = 0i64;
    for field_arg in &args[1..] {
        let field = String::from_utf8_lossy(field_arg);
        if obj.remove(field.as_ref()).is_some() {
            removed += 1;
        }
    }

    if removed > 0 {
        let _ = db.update(collection, &key, Value::Object(obj));
    }

    RespValue::Integer(removed)
}

fn cmd_hlen(db: &Database, collection: &str, args: &[Vec<u8>]) -> RespValue {
    if args.is_empty() {
        return RespValue::err("wrong number of arguments for 'hlen' command");
    }
    let key = String::from_utf8_lossy(&args[0]);

    match db.get(collection, &key) {
        Ok(doc) => {
            let count = doc
                .as_object()
                .map(|m| m.keys().filter(|k| *k != "_id").count())
                .unwrap_or(0);
            RespValue::Integer(count as i64)
        }
        Err(_) => RespValue::Integer(0),
    }
}

// ── Utilities ───────────────────────────────────────────────────────────────

/// Simple glob matching supporting * and ? wildcards.
fn glob_match(pattern: &str, s: &str) -> bool {
    let p = pattern.as_bytes();
    let s = s.as_bytes();
    let mut pi = 0;
    let mut si = 0;
    let mut star_p = usize::MAX;
    let mut star_s = 0;

    while si < s.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star_p = pi;
            star_s = si;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_s += 1;
            si = star_s;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }

    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "order:123"));
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(glob_match("*name*", "username"));
        assert!(glob_match("", ""));
        assert!(!glob_match("", "x"));
    }

    #[test]
    fn test_resp_serialize() {
        assert_eq!(
            RespValue::SimpleString("OK".into()).serialize(),
            b"+OK\r\n"
        );
        assert_eq!(RespValue::Integer(42).serialize(), b":42\r\n");
        assert_eq!(RespValue::null_bulk().serialize(), b"$-1\r\n");
        assert_eq!(
            RespValue::bulk("hello").serialize(),
            b"$5\r\nhello\r\n"
        );
    }
}
