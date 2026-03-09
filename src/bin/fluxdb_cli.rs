use std::path::PathBuf;
use std::process;

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
    #[arg(short, long, default_value_t = 7654)]
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

// ── Backend trait ────────────────────────────────────────────────────────────

/// Commands that are read-only and allowed in local mode.
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
        // Enforce read-only
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

    // Raw JSON pass-through
    if trimmed.starts_with('{') {
        return serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"));
    }

    // Split into tokens, but treat everything from the first '{' onward as a single JSON blob
    let (tokens, json_blob) = split_tokens(trimmed);

    if tokens.is_empty() {
        return Err("empty command".into());
    }

    match tokens[0].as_str() {
        // No-arg commands
        "stats" => Ok(json!({"cmd": "stats"})),
        "compact" => Ok(json!({"cmd": "compact"})),
        "flush" => Ok(json!({"cmd": "flush"})),
        "cluster_status" => Ok(json!({"cmd": "cluster_status"})),

        // Auth
        "auth" => {
            let token = tokens.get(1).ok_or("usage: auth <token>")?;
            Ok(json!({"cmd": "auth", "token": token}))
        }

        // List
        "list" => match tokens.get(1).map(|s| s.as_str()) {
            Some("collections") => Ok(json!({"cmd": "list_collections"})),
            Some("indexes") => {
                let col = tokens.get(2).ok_or("usage: list indexes <collection>")?;
                Ok(json!({"cmd": "list_indexes", "collection": col}))
            }
            _ => Err("usage: list collections | list indexes <collection>".into()),
        },

        // Create
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

        // Drop
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

        // Insert
        "insert" => {
            let col = tokens.get(1).ok_or("usage: insert <collection> <json>")?;
            let doc_str = json_blob.ok_or("usage: insert <collection> <json>")?;
            let doc: Value =
                serde_json::from_str(&doc_str).map_err(|e| format!("invalid document: {e}"))?;
            Ok(json!({"cmd": "insert", "collection": col, "document": doc}))
        }

        // Get
        "get" => {
            let col = tokens.get(1).ok_or("usage: get <collection> <id>")?;
            let id = tokens.get(2).ok_or("usage: get <collection> <id>")?;
            Ok(json!({"cmd": "get", "collection": col, "id": id}))
        }

        // Update
        "update" => {
            let col = tokens.get(1).ok_or("usage: update <collection> <id> <json>")?;
            let id = tokens.get(2).ok_or("usage: update <collection> <id> <json>")?;
            let doc_str = json_blob.ok_or("usage: update <collection> <id> <json>")?;
            let doc: Value =
                serde_json::from_str(&doc_str).map_err(|e| format!("invalid document: {e}"))?;
            Ok(json!({"cmd": "update", "collection": col, "id": id, "document": doc}))
        }

        // Delete
        "delete" => {
            let col = tokens.get(1).ok_or("usage: delete <collection> <id>")?;
            let id = tokens.get(2).ok_or("usage: delete <collection> <id>")?;
            Ok(json!({"cmd": "delete", "collection": col, "id": id}))
        }

        // Find
        "find" => {
            let col = tokens.get(1).ok_or("usage: find <collection> [filter_json]")?;
            let mut cmd = json!({"cmd": "find", "collection": col});

            if let Some(blob) = json_blob {
                let filter: Value =
                    serde_json::from_str(&blob).map_err(|e| format!("invalid filter: {e}"))?;
                cmd["filter"] = filter;
            }

            // Parse trailing --limit, --skip, --sort from tokens after the collection
            let rest: Vec<&str> = tokens[2..].iter().map(|s| s.as_str()).collect();
            let mut i = 0;
            while i < rest.len() {
                match rest[i] {
                    "--limit" => {
                        let n = rest
                            .get(i + 1)
                            .ok_or("--limit requires a number")?
                            .parse::<u64>()
                            .map_err(|_| "--limit must be a number")?;
                        cmd["limit"] = json!(n);
                        i += 2;
                    }
                    "--skip" => {
                        let n = rest
                            .get(i + 1)
                            .ok_or("--skip requires a number")?
                            .parse::<u64>()
                            .map_err(|_| "--skip must be a number")?;
                        cmd["skip"] = json!(n);
                        i += 2;
                    }
                    "--sort" => {
                        let s = rest.get(i + 1).ok_or("--sort requires a JSON object")?;
                        let sort: Value =
                            serde_json::from_str(s).map_err(|e| format!("invalid sort: {e}"))?;
                        cmd["sort"] = sort;
                        i += 2;
                    }
                    _ => i += 1,
                }
            }

            Ok(cmd)
        }

        // Count
        "count" => {
            let col = tokens.get(1).ok_or("usage: count <collection> [filter_json]")?;
            let mut cmd = json!({"cmd": "count", "collection": col});
            if let Some(blob) = json_blob {
                let filter: Value =
                    serde_json::from_str(&blob).map_err(|e| format!("invalid filter: {e}"))?;
                cmd["filter"] = filter;
            }
            Ok(cmd)
        }

        other => Err(format!("unknown command: {other}. Type 'help' for usage.")),
    }
}

/// Split input into whitespace-separated tokens (before first '{') and an optional
/// JSON blob (everything from the first '{' to end of line).
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

// ── Output formatting ────────────────────────────────────────────────────────

fn format_response(value: &Value, raw: bool) -> String {
    if raw {
        return serde_json::to_string(value).unwrap_or_default();
    }

    let pretty = serde_json::to_string_pretty(value).unwrap_or_default();
    colorize_json(&pretty, value)
}

fn colorize_json(pretty: &str, value: &Value) -> String {
    let ok = value.get("ok").and_then(|v| v.as_bool());
    let mut result = String::new();

    for line in pretty.lines() {
        let colored_line = if line.contains("\"ok\": true") {
            line.replace("true", &"true".green().to_string())
        } else if line.contains("\"ok\": false") {
            line.replace("false", &"false".red().to_string())
        } else if line.contains("\"error\":") {
            line.red().to_string()
        } else {
            line.to_string()
        };
        result.push_str(&colored_line);
        result.push('\n');
    }

    // Add a status line for quick scanning
    match ok {
        Some(true) => {
            // Don't add extra noise for success
        }
        Some(false) => {
            if let Some(err) = value.get("error").and_then(|v| v.as_str()) {
                result.push_str(&format!("{}", format!("Error: {err}").red().bold()));
                result.push('\n');
            }
        }
        None => {}
    }

    result
}

// ── Help text ────────────────────────────────────────────────────────────────

fn print_help(local: bool) {
    if local {
        println!(
            "{}",
            r#"
Read-only local mode. Only query commands are available.

Commands:
  stats                                   Show database statistics
  list collections                        List all collections
  list indexes <collection>               List indexes on a collection

  get <collection> <id>                   Get a document by ID
  find <collection> [filter] [options]    Query documents
    Options: --limit N  --skip N  --sort {"field": 1}
  count <collection> [filter]             Count matching documents

  help                                    Show this help
  exit / quit / Ctrl-D                    Disconnect

Raw JSON is also accepted:
  {"cmd":"find","collection":"users","filter":{"age":{"$gte":25}}}
"#
            .trim()
        );
    } else {
        println!(
            "{}",
            r#"
Commands:
  stats                                   Show database statistics
  compact                                 Compact the WAL
  flush                                   Flush WAL to disk
  cluster_status                          Show cluster status

  list collections                        List all collections
  create collection <name>                Create a collection
  drop collection <name>                  Drop a collection

  insert <collection> <json>              Insert a document
  get <collection> <id>                   Get a document by ID
  update <collection> <id> <json>         Replace a document
  delete <collection> <id>                Delete a document

  find <collection> [filter] [options]    Query documents
    Options: --limit N  --skip N  --sort {"field": 1}
  count <collection> [filter]             Count matching documents

  create index <collection> <field>       Create a secondary index
  drop index <collection> <field>         Drop an index
  list indexes <collection>               List indexes on a collection

  auth <token>                            Authenticate with the server
  help                                    Show this help
  exit / quit / Ctrl-D                    Disconnect

Raw JSON is also accepted:
  {"cmd":"find","collection":"users","filter":{"age":{"$gte":25}}}
"#
            .trim()
        );
    }
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
    "help",
    "exit",
    "quit",
];

#[derive(rustyline::Helper)]
struct CliHelper {
    hinter: HistoryHinter,
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
        let matches: Vec<String> = COMMANDS
            .iter()
            .filter(|cmd| cmd.starts_with(prefix))
            .map(|cmd| cmd.to_string())
            .collect();
        Ok((0, matches))
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
        // Local read-only mode
        match LocalBackend::open(data_dir) {
            Ok(local) => Backend::Local(local),
            Err(e) => {
                eprintln!("{}", e.red().bold());
                process::exit(1);
            }
        }
    } else {
        // Network mode
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

    // One-shot mode
    if let Some(ref cmd) = args.cmd {
        let request = match parse_shorthand(cmd) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", e.red());
                process::exit(1);
            }
        };

        match backend.execute(&request).await {
            Ok(resp) => {
                print!("{}", format_response(&resp, args.raw));
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
    run_repl(&mut backend, &args).await;
}

async fn run_repl(backend: &mut Backend, args: &CliArgs) {
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();

    let helper = CliHelper {
        hinter: HistoryHinter::new(),
    };

    let mut rl = Editor::with_config(config).expect("failed to create editor");
    rl.set_helper(Some(helper));

    let history_path = dirs_home().join(".fluxdb_cli_history");
    let _ = rl.load_history(&history_path);

    if backend.is_local() {
        println!(
            "Opened {} (read-only). Type {} for commands.",
            args.data_dir.as_ref().unwrap().display(),
            "help".bold()
        );
    } else {
        println!(
            "Connected to {}:{}. Type {} for commands.",
            args.host,
            args.port,
            "help".bold()
        );
    }

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

                let request = match parse_shorthand(trimmed) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("{}", e.red());
                        continue;
                    }
                };

                match backend.execute(&request).await {
                    Ok(resp) => {
                        print!("{}", format_response(&resp, args.raw));
                    }
                    Err(e) => {
                        eprintln!("{}", format!("Error: {e}").red().bold());
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
