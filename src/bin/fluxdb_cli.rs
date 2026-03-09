use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use clap::Parser;
use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::hint::HistoryHinter;
use rustyline::{CompletionType, Config, EditMode, Editor};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use fluxdb::database::Database;
use fluxdb::server::process_command;

#[derive(Parser)]
#[command(name = "fluxdb-cli", version, about = "FluxDB command-line client")]
struct CliArgs {
    /// Open a data directory directly (read-only, no server needed)
    #[arg(short, long)]
    data_dir: Option<PathBuf>,

    /// Server host (network mode)
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Server port (network mode)
    #[arg(short, long, default_value_t = 5148)]
    port: u16,

    /// Execute a single command and exit (raw JSON or shorthand)
    #[arg(short, long)]
    cmd: Option<String>,

    /// Authentication token (network mode)
    #[arg(long)]
    auth_token: Option<String>,

    /// Output raw JSON instead of pretty-printed
    #[arg(long)]
    raw: bool,
}

// ── REPL display state ───────────────────────────────────────────────────────

struct DisplayState {
    expanded: bool,
    timing: bool,
    raw: bool,
}

// ── Backend ──────────────────────────────────────────────────────────────────

const READ_ONLY_CMDS: &[&str] = &[
    "get",
    "find",
    "count",
    "list_collections",
    "list_indexes",
    "stats",
    "cluster_status",
];

enum Backend {
    Network(NetworkBackend),
    Local(LocalBackend),
}

impl Backend {
    async fn execute(&mut self, request: &Value) -> Result<Value, String> {
        match self {
            Backend::Network(n) => n.send(request).await,
            Backend::Local(l) => l.execute(request),
        }
    }

    fn is_local(&self) -> bool {
        matches!(self, Backend::Local(_))
    }
}

// ── Network backend ──────────────────────────────────────────────────────────

struct NetworkBackend {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl NetworkBackend {
    async fn connect(host: &str, port: u16) -> Result<Self, String> {
        let addr = format!("{host}:{port}");
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| format!("failed to connect to {addr}: {e}"))?;
        let (reader, writer) = stream.into_split();
        Ok(NetworkBackend {
            reader: BufReader::new(reader),
            writer,
        })
    }

    async fn send(&mut self, request: &Value) -> Result<Value, String> {
        let mut line = serde_json::to_string(request).map_err(|e| e.to_string())?;
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(|e| format!("write error: {e}"))?;
        self.writer
            .flush()
            .await
            .map_err(|e| format!("flush error: {e}"))?;

        let mut response = String::new();
        self.reader
            .read_line(&mut response)
            .await
            .map_err(|e| format!("read error: {e}"))?;

        if response.is_empty() {
            return Err("server closed connection".into());
        }

        serde_json::from_str(&response).map_err(|e| format!("invalid response: {e}"))
    }

    async fn authenticate(&mut self, token: &str) -> Result<(), String> {
        let resp = self.send(&json!({"cmd": "auth", "token": token})).await?;
        if resp.get("ok").and_then(|v| v.as_bool()) == Some(true) {
            Ok(())
        } else {
            let err = resp["error"].as_str().unwrap_or("authentication failed");
            Err(err.to_string())
        }
    }
}

// ── Local backend ────────────────────────────────────────────────────────────

struct LocalBackend {
    db: Database,
}

impl LocalBackend {
    fn open(data_dir: &std::path::Path) -> Result<Self, String> {
        let db = Database::open_readonly(data_dir)
            .map_err(|e| format!("failed to open {}: {e}", data_dir.display()))?;
        Ok(LocalBackend { db })
    }

    fn execute(&self, request: &Value) -> Result<Value, String> {
        if let Some(cmd) = request["cmd"].as_str() {
            if !READ_ONLY_CMDS.contains(&cmd) {
                return Ok(
                    json!({"ok": false, "error": format!("read-only mode: '{cmd}' is not allowed")}),
                );
            }
        }
        Ok(process_command(&self.db, request))
    }
}

// ── Shorthand parser ─────────────────────────────────────────────────────────

fn parse_shorthand(input: &str) -> Result<Value, String> {
    let trimmed = input.trim();

    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"));
    }

    let (tokens, json_blob) = split_tokens(trimmed);

    if tokens.is_empty() {
        return Err("empty command".into());
    }

    match tokens[0].as_str() {
        "stats" => Ok(json!({"cmd": "stats"})),
        "compact" => Ok(json!({"cmd": "compact"})),
        "flush" => Ok(json!({"cmd": "flush"})),
        "cluster_status" => Ok(json!({"cmd": "cluster_status"})),

        "auth" => {
            let token = tokens.get(1).ok_or("usage: auth <token>")?;
            Ok(json!({"cmd": "auth", "token": token}))
        }

        "list" => match tokens.get(1).map(|s| s.as_str()) {
            Some("collections") => Ok(json!({"cmd": "list_collections"})),
            Some("indexes") => {
                let col = tokens.get(2).ok_or("usage: list indexes <collection>")?;
                Ok(json!({"cmd": "list_indexes", "collection": col}))
            }
            _ => Err("usage: list collections | list indexes <collection>".into()),
        },

        "create" => match tokens.get(1).map(|s| s.as_str()) {
            Some("collection") => {
                let name = tokens.get(2).ok_or("usage: create collection <name>")?;
                Ok(json!({"cmd": "create_collection", "name": name}))
            }
            Some("index") => {
                let col = tokens.get(2).ok_or("usage: create index <collection> <field>")?;
                let field = tokens.get(3).ok_or("usage: create index <collection> <field>")?;
                Ok(json!({"cmd": "create_index", "collection": col, "field": field}))
            }
            _ => Err(
                "usage: create collection <name> | create index <collection> <field>".into(),
            ),
        },

        "drop" => match tokens.get(1).map(|s| s.as_str()) {
            Some("collection") => {
                let name = tokens.get(2).ok_or("usage: drop collection <name>")?;
                Ok(json!({"cmd": "drop_collection", "name": name}))
            }
            Some("index") => {
                let col = tokens.get(2).ok_or("usage: drop index <collection> <field>")?;
                let field = tokens.get(3).ok_or("usage: drop index <collection> <field>")?;
                Ok(json!({"cmd": "drop_index", "collection": col, "field": field}))
            }
            _ => Err("usage: drop collection <name> | drop index <collection> <field>".into()),
        },

        "insert" => {
            let col = tokens.get(1).ok_or("usage: insert <collection> <json>")?;
            let doc_str = json_blob.ok_or("usage: insert <collection> <json>")?;
            let doc: Value =
                serde_json::from_str(&doc_str).map_err(|e| format!("invalid document: {e}"))?;
            Ok(json!({"cmd": "insert", "collection": col, "document": doc}))
        }

        "get" => {
            let col = tokens.get(1).ok_or("usage: get <collection> <id>")?;
            let id = tokens.get(2).ok_or("usage: get <collection> <id>")?;
            Ok(json!({"cmd": "get", "collection": col, "id": id}))
        }

        "update" => {
            let col = tokens.get(1).ok_or("usage: update <collection> <id> <json>")?;
            let id = tokens.get(2).ok_or("usage: update <collection> <id> <json>")?;
            let doc_str = json_blob.ok_or("usage: update <collection> <id> <json>")?;
            let doc: Value =
                serde_json::from_str(&doc_str).map_err(|e| format!("invalid document: {e}"))?;
            Ok(json!({"cmd": "update", "collection": col, "id": id, "document": doc}))
        }

        "delete" => {
            let col = tokens.get(1).ok_or("usage: delete <collection> <id>")?;
            let id = tokens.get(2).ok_or("usage: delete <collection> <id>")?;
            Ok(json!({"cmd": "delete", "collection": col, "id": id}))
        }

        "find" => {
            // find needs special parsing to handle multiple JSON objects
            // (filter + sort) and interleaved flags
            let col_str = tokens.get(1).ok_or("usage: find <collection> [filter] [--limit N] [--skip N] [--sort {json}]")?;
            let mut cmd = json!({"cmd": "find", "collection": col_str});

            // Find where the collection name ends in the original input
            // and pass everything after it to the balanced parser
            let after_cmd = trimmed.strip_prefix("find").unwrap().trim_start();
            let tail = after_cmd.strip_prefix(col_str.as_str()).unwrap_or("").trim_start();
            if !tail.is_empty() {
                parse_find_tail(tail, &mut cmd)?;
            }

            Ok(cmd)
        }

        "count" => {
            let col_str = tokens.get(1).ok_or("usage: count <collection> [filter_json]")?;
            let mut cmd = json!({"cmd": "count", "collection": col_str});

            let after_cmd = trimmed.strip_prefix("count").unwrap().trim_start();
            let tail = after_cmd.strip_prefix(col_str.as_str()).unwrap_or("").trim_start();
            if !tail.is_empty() {
                parse_find_tail(tail, &mut cmd)?;
            }

            Ok(cmd)
        }

        other => Err(format!("unknown command: {other}. Type \\? for help.")),
    }
}

fn split_tokens(input: &str) -> (Vec<String>, Option<String>) {
    if let Some(brace_pos) = input.find('{') {
        let before = &input[..brace_pos];
        let json_part = input[brace_pos..].trim().to_string();
        let tokens: Vec<String> = before.split_whitespace().map(String::from).collect();
        (tokens, Some(json_part))
    } else {
        let tokens: Vec<String> = input.split_whitespace().map(String::from).collect();
        (tokens, None)
    }
}

/// Extract a balanced JSON object from `s` starting at `start`.
/// Returns `(json_string, end_position)` or None if not valid.
fn extract_balanced_json(s: &str, start: usize) -> Option<(String, usize)> {
    let bytes = s.as_bytes();
    if start >= bytes.len() || bytes[start] != b'{' {
        return None;
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for i in start..bytes.len() {
        if escape {
            escape = false;
            continue;
        }
        match bytes[i] {
            b'\\' if in_string => escape = true,
            b'"' => in_string = !in_string,
            b'{' if !in_string => depth += 1,
            b'}' if !in_string => {
                depth -= 1;
                if depth == 0 {
                    return Some((s[start..=i].to_string(), i + 1));
                }
            }
            _ => {}
        }
    }
    None
}

/// Parse the tail of a `find` or `count` command, which can contain an optional
/// filter JSON, and for find: `--limit N`, `--skip N`, `--sort {json}`.
///
/// Handles: `find col {"f":1} --limit 10 --sort {"ts":-1}`
///          `find col --limit 10 --sort {"ts":-1}`
///          `find col {"f":1}`
fn parse_find_tail(tail: &str, cmd: &mut Value) -> Result<(), String> {
    let mut pos = 0;
    let bytes = tail.as_bytes();

    // Skip leading whitespace
    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
        pos += 1;
    }

    // If the first non-ws char is '{', it's the filter
    if pos < bytes.len() && bytes[pos] == b'{' {
        let (json_str, end) = extract_balanced_json(tail, pos)
            .ok_or_else(|| "unterminated JSON object in filter".to_string())?;
        let filter: Value =
            serde_json::from_str(&json_str).map_err(|e| format!("invalid filter: {e}"))?;
        cmd["filter"] = filter;
        pos = end;
    }

    // Now parse flags: --limit N, --skip N, --sort {json}
    while pos < bytes.len() {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        // Read a token
        let token_start = pos;
        while pos < bytes.len() && !bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        let token = &tail[token_start..pos];

        match token {
            "--limit" => {
                let n = next_word(tail, &mut pos).ok_or("--limit requires a number")?;
                let n: u64 = n.parse().map_err(|_| "--limit must be a number")?;
                cmd["limit"] = json!(n);
            }
            "--skip" => {
                let n = next_word(tail, &mut pos).ok_or("--skip requires a number")?;
                let n: u64 = n.parse().map_err(|_| "--skip must be a number")?;
                cmd["skip"] = json!(n);
            }
            "--sort" => {
                // Skip whitespace to find the JSON object
                while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                if pos < bytes.len() && bytes[pos] == b'{' {
                    let (json_str, end) = extract_balanced_json(tail, pos)
                        .ok_or_else(|| "unterminated JSON object in --sort".to_string())?;
                    let sort: Value = serde_json::from_str(&json_str)
                        .map_err(|e| format!("invalid sort: {e}"))?;
                    cmd["sort"] = sort;
                    pos = end;
                } else {
                    return Err("--sort requires a JSON object like {\"field\":1}".into());
                }
            }
            _ => {
                // Ignore unknown tokens
            }
        }
    }

    Ok(())
}

/// Read the next whitespace-delimited word from `s` starting at `*pos`,
/// advancing `*pos` past it.
fn next_word<'a>(s: &'a str, pos: &mut usize) -> Option<&'a str> {
    let bytes = s.as_bytes();
    while *pos < bytes.len() && bytes[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
    if *pos >= bytes.len() {
        return None;
    }
    let start = *pos;
    while *pos < bytes.len() && !bytes[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
    Some(&s[start..*pos])
}

// ── Table formatting ─────────────────────────────────────────────────────────

/// Format a list of JSON documents as an aligned table (psql-style).
fn format_table(docs: &[Value]) -> String {
    if docs.is_empty() {
        return "(0 rows)\n".to_string();
    }

    // Collect all unique keys in stable order (_id first, then alphabetical)
    let mut keys = BTreeSet::new();
    for doc in docs {
        if let Some(obj) = doc.as_object() {
            for k in obj.keys() {
                keys.insert(k.clone());
            }
        }
    }

    // Put _id first
    let mut columns: Vec<String> = Vec::with_capacity(keys.len());
    if keys.contains("_id") {
        columns.push("_id".to_string());
    }
    for k in &keys {
        if k != "_id" {
            columns.push(k.clone());
        }
    }

    // Build cell values
    let mut rows: Vec<Vec<String>> = Vec::with_capacity(docs.len());
    for doc in docs {
        let mut row = Vec::with_capacity(columns.len());
        for col in &columns {
            let val = doc.get(col).unwrap_or(&Value::Null);
            row.push(format_cell(val));
        }
        rows.push(row);
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    let mut out = String::new();

    // Header
    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!(" {:<width$} ", c, width = widths[i]))
        .collect();
    out.push_str(&header.join("|"));
    out.push('\n');

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(w + 2)).collect();
    out.push_str(&sep.join("+"));
    out.push('\n');

    // Rows
    for row in &rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| format!(" {:<width$} ", cell, width = widths[i]))
            .collect();
        out.push_str(&cells.join("|"));
        out.push('\n');
    }

    // Row count
    let n = rows.len();
    out.push_str(&format!("({n} {})\n", if n == 1 { "row" } else { "rows" }));

    out
}

/// Format a single document in expanded (vertical) display.
fn format_expanded(docs: &[Value]) -> String {
    if docs.is_empty() {
        return "(0 rows)\n".to_string();
    }

    let mut out = String::new();

    for (idx, doc) in docs.iter().enumerate() {
        out.push_str(&format!(
            "{}\n",
            format!("-[ RECORD {} ]", idx + 1).bold()
        ));

        if let Some(obj) = doc.as_object() {
            let max_key = obj.keys().map(|k| k.len()).max().unwrap_or(0);

            // _id first
            if let Some(val) = obj.get("_id") {
                out.push_str(&format!(
                    "{:<width$} | {}\n",
                    "_id",
                    format_cell(val),
                    width = max_key
                ));
            }

            for (k, v) in obj {
                if k == "_id" {
                    continue;
                }
                out.push_str(&format!(
                    "{:<width$} | {}\n",
                    k,
                    format_cell(v),
                    width = max_key
                ));
            }
        }
    }

    let n = docs.len();
    out.push_str(&format!("({n} {})\n", if n == 1 { "row" } else { "rows" }));
    out
}

/// Format a JSON value as a table cell string.
fn format_cell(val: &Value) -> String {
    match val {
        Value::Null => "".to_string(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "t" } else { "f" }.to_string(),
        Value::Array(a) => {
            let items: Vec<String> = a.iter().map(|v| format_cell(v)).collect();
            format!("{{{}}}", items.join(","))
        }
        Value::Object(_) => serde_json::to_string(val).unwrap_or_default(),
    }
}

/// Format a list_collections response as a table.
fn format_collections_table(collections: &[Value]) -> String {
    if collections.is_empty() {
        return "(0 collections)\n".to_string();
    }

    let mut out = String::new();
    let max_len = collections
        .iter()
        .filter_map(|c| c.as_str())
        .map(|s| s.len())
        .max()
        .unwrap_or(4)
        .max(4);

    out.push_str(&format!(" {:<width$} \n", "Name", width = max_len));
    out.push_str(&format!("{}\n", "-".repeat(max_len + 2)));

    for col in collections {
        if let Some(name) = col.as_str() {
            out.push_str(&format!(" {:<width$} \n", name, width = max_len));
        }
    }

    let n = collections.len();
    out.push_str(&format!(
        "({n} {})\n",
        if n == 1 { "collection" } else { "collections" }
    ));
    out
}

/// Format a list_indexes response as a table.
fn format_indexes_table(indexes: &[Value]) -> String {
    if indexes.is_empty() {
        return "(0 indexes)\n".to_string();
    }

    let mut out = String::new();
    let max_len = indexes
        .iter()
        .filter_map(|i| i.as_str())
        .map(|s| s.len())
        .max()
        .unwrap_or(5)
        .max(5);

    out.push_str(&format!(" {:<width$} \n", "Field", width = max_len));
    out.push_str(&format!("{}\n", "-".repeat(max_len + 2)));

    for idx in indexes {
        if let Some(field) = idx.as_str() {
            out.push_str(&format!(" {:<width$} \n", field, width = max_len));
        }
    }

    let n = indexes.len();
    out.push_str(&format!(
        "({n} {})\n",
        if n == 1 { "index" } else { "indexes" }
    ));
    out
}

/// Format a stats response as key-value pairs.
fn format_stats(stats: &Value) -> String {
    let mut out = String::new();

    if let Some(s) = stats.get("stats").and_then(|v| v.as_object()) {
        if let Some(name) = s.get("database").and_then(|v| v.as_str()) {
            out.push_str(&format!("Database:    {name}\n"));
        }
        if let Some(n) = s.get("collections").and_then(|v| v.as_u64()) {
            out.push_str(&format!("Collections: {n}\n"));
        }
        if let Some(n) = s.get("total_documents").and_then(|v| v.as_u64()) {
            out.push_str(&format!("Documents:   {n}\n"));
        }

        if let Some(details) = s.get("collection_details").and_then(|v| v.as_object()) {
            if !details.is_empty() {
                out.push('\n');
                // Build a table of collection details
                let mut col_width = 10usize;
                let mut idx_details: Vec<(&str, u64, Vec<&str>)> = Vec::new();

                for (name, info) in details {
                    col_width = col_width.max(name.len());
                    let doc_count = info
                        .get("documents")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let indexes: Vec<&str> = info
                        .get("indexes")
                        .and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                        .unwrap_or_default();
                    idx_details.push((name.as_str(), doc_count, indexes));
                }

                out.push_str(&format!(
                    " {:<cw$} | {:>9} | Indexes\n",
                    "Collection",
                    "Documents",
                    cw = col_width
                ));
                out.push_str(&format!(
                    "{}+{}+{}\n",
                    "-".repeat(col_width + 2),
                    "-".repeat(11),
                    "-".repeat(20)
                ));

                for (name, docs, indexes) in &idx_details {
                    let idx_str = if indexes.is_empty() {
                        "none".to_string()
                    } else {
                        indexes.join(", ")
                    };
                    out.push_str(&format!(
                        " {:<cw$} | {:>9} | {idx_str}\n",
                        name,
                        docs,
                        cw = col_width
                    ));
                }
            }
        }
    }

    out
}

// ── Response formatting ──────────────────────────────────────────────────────

fn format_response(resp: &Value, cmd: Option<&str>, display: &DisplayState) -> String {
    // Error responses always show as JSON
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        if let Some(err) = resp.get("error").and_then(|v| v.as_str()) {
            return format!("{}\n", format!("ERROR: {err}").red());
        }
        return format_json(resp);
    }

    if display.raw {
        return format_json(resp);
    }

    match cmd {
        Some("find") => {
            if let Some(docs) = resp.get("documents").and_then(|v| v.as_array()) {
                if display.expanded {
                    format_expanded(docs)
                } else {
                    format_table(docs)
                }
            } else {
                format_json(resp)
            }
        }
        Some("get") => {
            if let Some(doc) = resp.get("document") {
                if display.expanded {
                    format_expanded(&[doc.clone()])
                } else {
                    format_table(&[doc.clone()])
                }
            } else {
                format_json(resp)
            }
        }
        Some("count") => {
            let n = resp.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
            format!(" count \n-------\n {n:<5} \n(1 row)\n")
        }
        Some("list_collections") => {
            if let Some(cols) = resp.get("collections").and_then(|v| v.as_array()) {
                format_collections_table(cols)
            } else {
                format_json(resp)
            }
        }
        Some("list_indexes") => {
            if let Some(idxs) = resp.get("indexes").and_then(|v| v.as_array()) {
                format_indexes_table(idxs)
            } else {
                format_json(resp)
            }
        }
        Some("stats") => format_stats(resp),
        Some("insert") => {
            if let Some(id) = resp.get("id").and_then(|v| v.as_str()) {
                format!("INSERT 1\nid: {id}\n")
            } else {
                "INSERT 1\n".to_string()
            }
        }
        Some("update") => "UPDATE 1\n".to_string(),
        Some("delete") => {
            let deleted = resp.get("deleted").and_then(|v| v.as_bool()).unwrap_or(false);
            if deleted {
                "DELETE 1\n".to_string()
            } else {
                "DELETE 0\n".to_string()
            }
        }
        Some("create_collection") => "CREATE COLLECTION\n".to_string(),
        Some("drop_collection") => "DROP COLLECTION\n".to_string(),
        Some("create_index") => "CREATE INDEX\n".to_string(),
        Some("drop_index") => "DROP INDEX\n".to_string(),
        Some("compact") => "COMPACT\n".to_string(),
        Some("flush") => "FLUSH\n".to_string(),
        Some("auth") => "OK\n".to_string(),
        _ => format_json(resp),
    }
}

fn format_json(value: &Value) -> String {
    let mut s = serde_json::to_string_pretty(value).unwrap_or_default();
    s.push('\n');
    s
}

fn format_timing(elapsed: std::time::Duration) -> String {
    let ms = elapsed.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("Time: {:.3} ms\n", ms)
    } else {
        format!("Time: {:.1} ms\n", ms)
    }
}

/// Extract the command name from a request JSON for display formatting.
fn cmd_name(request: &Value) -> Option<String> {
    request.get("cmd").and_then(|v| v.as_str()).map(String::from)
}

// ── Backslash commands ───────────────────────────────────────────────────────

/// Handle a backslash meta-command. Returns Some(request) if it maps to a
/// server command, or None if it was handled locally.
fn handle_backslash(input: &str, display: &mut DisplayState, is_local: bool) -> Option<Value> {
    let parts: Vec<&str> = input.split_whitespace().collect();
    let cmd = parts.first().map(|s| *s).unwrap_or("");

    match cmd {
        "\\?" | "\\h" | "\\help" => {
            print_help(is_local);
            None
        }
        "\\q" | "\\quit" => {
            process::exit(0);
        }
        "\\dt" => Some(json!({"cmd": "list_collections"})),
        "\\di" => {
            if let Some(col) = parts.get(1) {
                Some(json!({"cmd": "list_indexes", "collection": col}))
            } else {
                eprintln!("{}", "usage: \\di <collection>".red());
                None
            }
        }
        "\\d" => {
            // \d <collection> — show collection info (doc count + indexes)
            if let Some(col) = parts.get(1) {
                // We'll use stats and filter, but simpler to use list_indexes
                // and let the display handle it. Return a special marker.
                Some(json!({"cmd": "list_indexes", "collection": col}))
            } else {
                // \d with no args = list collections
                Some(json!({"cmd": "list_collections"}))
            }
        }
        "\\x" => {
            display.expanded = !display.expanded;
            println!(
                "Expanded display is {}.",
                if display.expanded { "on" } else { "off" }
            );
            None
        }
        "\\timing" => {
            display.timing = !display.timing;
            println!(
                "Timing is {}.",
                if display.timing { "on" } else { "off" }
            );
            None
        }
        "\\raw" => {
            display.raw = !display.raw;
            println!(
                "Raw JSON output is {}.",
                if display.raw { "on" } else { "off" }
            );
            None
        }
        "\\conninfo" => {
            if is_local {
                println!("Local read-only mode.");
            } else {
                println!("Network mode (use --host / --port to configure).");
            }
            None
        }
        _ => {
            eprintln!(
                "{}",
                format!("Invalid command: {cmd}. Try \\? for help.").red()
            );
            None
        }
    }
}

// ── Help text ────────────────────────────────────────────────────────────────

fn print_help(is_local: bool) {
    let mode = if is_local {
        "Read-only local mode. Only query commands are available.\n\n"
    } else {
        ""
    };

    println!(
        "{mode}{}",
        r#"General
  \?                 Show this help
  \q                 Quit
  \x                 Toggle expanded display (vertical)
  \timing            Toggle query timing
  \raw               Toggle raw JSON output
  \conninfo          Show connection info

Informational
  \dt                List collections
  \di <collection>   List indexes on a collection
  \d                 List collections
  \d  <collection>   Describe a collection (indexes)

Commands
  stats                                   Show database statistics
  get <collection> <id>                   Get a document by ID
  find <collection> [filter] [options]    Query documents
    Options: --limit N  --skip N  --sort {"field": 1}
  count <collection> [filter]             Count matching documents
  list collections                        List all collections
  list indexes <collection>               List indexes"#
    );

    if !is_local {
        println!(
            "{}",
            r#"
  insert <collection> <json>              Insert a document
  update <collection> <id> <json>         Replace a document
  delete <collection> <id>                Delete a document
  create collection <name>                Create a collection
  drop collection <name>                  Drop a collection
  create index <collection> <field>       Create an index
  drop index <collection> <field>         Drop an index
  compact                                 Compact the WAL
  flush                                   Flush WAL to disk
  auth <token>                            Authenticate"#
        );
    }

    println!(
        "\n{}",
        "Raw JSON is also accepted: {\"cmd\":\"find\",\"collection\":\"users\"}"
    );
}

// ── Tab completion ───────────────────────────────────────────────────────────

static COMMANDS: &[&str] = &[
    "stats",
    "compact",
    "flush",
    "cluster_status",
    "list collections",
    "list indexes",
    "create collection",
    "create index",
    "drop collection",
    "drop index",
    "insert",
    "get",
    "update",
    "delete",
    "find",
    "count",
    "auth",
    "\\dt",
    "\\di",
    "\\d",
    "\\x",
    "\\timing",
    "\\raw",
    "\\conninfo",
    "\\q",
    "\\?",
    "help",
    "exit",
    "quit",
];

/// Commands that take a collection name as the word right after the command.
const COLLECTION_ARG_CMDS: &[&str] = &[
    "find", "insert", "get", "update", "delete", "count",
];

/// Two-word commands where the third word is a collection name.
const COLLECTION_ARG3_PREFIXES: &[&str] = &[
    "create index", "drop index", "drop collection", "list indexes",
];

/// Second-word completions for multi-word commands.
const CREATE_SUBCOMMANDS: &[&str] = &["collection", "index"];
const DROP_SUBCOMMANDS: &[&str] = &["collection", "index"];
const LIST_SUBCOMMANDS: &[&str] = &["collections", "indexes"];

#[derive(rustyline::Helper)]
struct CliHelper {
    hinter: HistoryHinter,
    collections: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
}

impl CliHelper {
    fn get_collections(&self) -> Vec<String> {
        self.collections.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl rustyline::completion::Completer for CliHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<String>)> {
        let prefix = &line[..pos];

        // Split into words for context-aware completion
        let words: Vec<&str> = prefix.split_whitespace().collect();
        let trailing_space = prefix.ends_with(' ');
        let completing_word = if trailing_space { "" } else { words.last().copied().unwrap_or("") };
        let complete_words = if trailing_space { words.len() } else { words.len().saturating_sub(1) };

        // Word 0: complete command names
        if complete_words == 0 {
            let matches: Vec<String> = COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(completing_word))
                .map(|cmd| cmd.to_string())
                .collect();
            return Ok((0, matches));
        }

        let first = words[0];

        // Word 1 for multi-word commands: complete subcommands
        if complete_words == 1 {
            let subs = match first {
                "create" => Some(CREATE_SUBCOMMANDS),
                "drop" => Some(DROP_SUBCOMMANDS),
                "list" => Some(LIST_SUBCOMMANDS),
                _ => None,
            };
            if let Some(subs) = subs {
                let word_start = prefix.len() - completing_word.len();
                let matches: Vec<String> = subs
                    .iter()
                    .filter(|s| s.starts_with(completing_word))
                    .map(|s| s.to_string())
                    .collect();
                return Ok((word_start, matches));
            }
        }

        // Word 1 for single-word commands that take a collection
        if complete_words == 1 && COLLECTION_ARG_CMDS.contains(&first) {
            let word_start = prefix.len() - completing_word.len();
            let matches: Vec<String> = self.get_collections()
                .into_iter()
                .filter(|c| c.starts_with(completing_word))
                .collect();
            return Ok((word_start, matches));
        }

        // Word 2 for two-word commands that take a collection
        if complete_words == 2 {
            let two_word = format!("{} {}", words[0], words[1]);
            if COLLECTION_ARG3_PREFIXES.iter().any(|p| two_word == *p) {
                let word_start = prefix.len() - completing_word.len();
                let matches: Vec<String> = self.get_collections()
                    .into_iter()
                    .filter(|c| c.starts_with(completing_word))
                    .collect();
                return Ok((word_start, matches));
            }
        }

        Ok((pos, vec![]))
    }
}

impl rustyline::hint::Hinter for CliHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &rustyline::Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

impl rustyline::highlight::Highlighter for CliHelper {}
impl rustyline::validate::Validator for CliHelper {}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let args = CliArgs::parse();

    let mut backend = if let Some(ref data_dir) = args.data_dir {
        match LocalBackend::open(data_dir) {
            Ok(local) => Backend::Local(local),
            Err(e) => {
                eprintln!("{}", e.red().bold());
                process::exit(1);
            }
        }
    } else {
        let mut net = match NetworkBackend::connect(&args.host, args.port).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{}", e.red().bold());
                process::exit(1);
            }
        };

        if let Some(ref token) = args.auth_token {
            if let Err(e) = net.authenticate(token).await {
                eprintln!("{}", format!("Authentication failed: {e}").red().bold());
                process::exit(1);
            }
        }

        Backend::Network(net)
    };

    let mut display = DisplayState {
        expanded: false,
        timing: false,
        raw: args.raw,
    };

    // One-shot mode
    if let Some(ref cmd_str) = args.cmd {
        let request = if cmd_str.starts_with('\\') {
            match handle_backslash(cmd_str, &mut display, backend.is_local()) {
                Some(r) => r,
                None => process::exit(0),
            }
        } else {
            match parse_shorthand(cmd_str) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("{}", e.red());
                    process::exit(1);
                }
            }
        };

        let cmd = cmd_name(&request);
        let start = Instant::now();

        match backend.execute(&request).await {
            Ok(resp) => {
                let elapsed = start.elapsed();
                print!(
                    "{}",
                    format_response(&resp, cmd.as_deref(), &display)
                );
                if display.timing {
                    print!("{}", format_timing(elapsed));
                }
                let ok = resp.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                process::exit(if ok { 0 } else { 1 });
            }
            Err(e) => {
                eprintln!("{}", e.red().bold());
                process::exit(1);
            }
        }
    }

    // REPL mode
    run_repl(&mut backend, &args, &mut display).await;
}

/// Commands that modify the collection list (trigger a refresh of completions).
const COLLECTION_MUTATING_CMDS: &[&str] = &[
    "create_collection", "drop_collection",
];

/// Fetch collection names from the backend for tab completion.
async fn fetch_collections(backend: &mut Backend) -> Vec<String> {
    let req = json!({"cmd": "list_collections"});
    match backend.execute(&req).await {
        Ok(resp) => {
            if let Some(arr) = resp.get("collections").and_then(|v| v.as_array()) {
                let mut names: Vec<String> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                names.sort();
                names
            } else {
                Vec::new()
            }
        }
        Err(_) => Vec::new(),
    }
}

async fn run_repl(backend: &mut Backend, args: &CliArgs, display: &mut DisplayState) {
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();

    let collections = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let helper = CliHelper {
        hinter: HistoryHinter::new(),
        collections: collections.clone(),
    };

    let mut rl = Editor::with_config(config).expect("failed to create editor");
    rl.set_helper(Some(helper));

    let history_path = dirs_home().join(".fluxdb_cli_history");
    let _ = rl.load_history(&history_path);

    // Seed collection list for tab completion
    {
        let names = fetch_collections(backend).await;
        *collections.lock().unwrap() = names;
    }

    // psql-style banner
    if backend.is_local() {
        println!(
            "fluxdb-cli (read-only) — {}",
            args.data_dir.as_ref().unwrap().display()
        );
    } else {
        println!("fluxdb-cli — {}:{}", args.host, args.port);
    }
    println!("Type \\? for help.\n");

    let prompt = if backend.is_local() {
        "fluxdb (ro)> "
    } else {
        "fluxdb> "
    };

    loop {
        let readline = rl.readline(prompt);
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                match trimmed {
                    "exit" | "quit" => break,
                    "help" => {
                        print_help(backend.is_local());
                        continue;
                    }
                    _ => {}
                }

                // Backslash commands
                if trimmed.starts_with('\\') {
                    let request =
                        match handle_backslash(trimmed, display, backend.is_local()) {
                            Some(r) => r,
                            None => continue,
                        };

                    let cmd = cmd_name(&request);
                    let start = Instant::now();
                    match backend.execute(&request).await {
                        Ok(resp) => {
                            let elapsed = start.elapsed();
                            print!(
                                "{}",
                                format_response(&resp, cmd.as_deref(), display)
                            );
                            if display.timing {
                                print!("{}", format_timing(elapsed));
                            }
                            // Refresh collection list if this command changed it
                            if cmd.as_deref().map_or(false, |c| COLLECTION_MUTATING_CMDS.contains(&c)) {
                                *collections.lock().unwrap() = fetch_collections(backend).await;
                            }
                        }
                        Err(e) => {
                            eprintln!("{}", format!("ERROR: {e}").red().bold());
                        }
                    }
                    continue;
                }

                // Regular commands
                let request = match parse_shorthand(trimmed) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("{}", e.red());
                        continue;
                    }
                };

                let cmd = cmd_name(&request);
                let start = Instant::now();

                match backend.execute(&request).await {
                    Ok(resp) => {
                        let elapsed = start.elapsed();
                        print!(
                            "{}",
                            format_response(&resp, cmd.as_deref(), display)
                        );
                        if display.timing {
                            print!("{}", format_timing(elapsed));
                        }
                        if cmd.as_deref().map_or(false, |c| COLLECTION_MUTATING_CMDS.contains(&c)) {
                            *collections.lock().unwrap() = fetch_collections(backend).await;
                        }
                    }
                    Err(e) => {
                        eprintln!("{}", format!("ERROR: {e}").red().bold());
                        if !backend.is_local() {
                            eprintln!("Attempting to reconnect...");
                            match NetworkBackend::connect(&args.host, args.port).await {
                                Ok(mut new_net) => {
                                    if let Some(ref token) = args.auth_token {
                                        if let Err(e) = new_net.authenticate(token).await {
                                            eprintln!(
                                                "{}",
                                                format!("Re-auth failed: {e}").red().bold()
                                            );
                                            break;
                                        }
                                    }
                                    *backend = Backend::Network(new_net);
                                    println!("{}", "Reconnected.".green());
                                }
                                Err(e) => {
                                    eprintln!(
                                        "{}",
                                        format!("Reconnect failed: {e}").red().bold()
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(e) => {
                eprintln!("{}", format!("Input error: {e}").red());
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    println!("Bye.");
}

fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
}
