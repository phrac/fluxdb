use std::fs;
use std::path::{Path, PathBuf};

use clap::Parser;
use serde::{Deserialize, Serialize};

/// FluxDB — a document-oriented NoSQL database.
#[derive(Parser, Debug)]
#[command(name = "fluxdb", version, about)]
pub struct Cli {
    /// Path to config file
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Data directory
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    /// Listen address (host:port)
    #[arg(short, long)]
    pub listen: Option<String>,

    /// WAL batch size (number of entries before flush)
    #[arg(long)]
    pub wal_batch_size: Option<usize>,

    /// WAL batch bytes threshold before flush
    #[arg(long)]
    pub wal_batch_bytes: Option<usize>,

    /// Enable Redis-compatible protocol server
    #[arg(long)]
    pub redis: Option<Option<String>>,

    /// Generate a default config file and exit
    #[arg(long)]
    pub init_config: Option<PathBuf>,

    /// Authentication token (required for client connections when set)
    #[arg(long)]
    pub auth_token: Option<String>,
}

/// Configuration loaded from TOML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    #[serde(default = "default_listen")]
    pub listen: String,

    #[serde(default)]
    pub wal: WalConfig,

    #[serde(default)]
    pub redis: RedisConfig,

    #[serde(default)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    pub limits: LimitsConfig,

    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    /// Enable the Redis-compatible protocol server.
    #[serde(default)]
    pub enabled: bool,

    /// Address and port for the Redis protocol server.
    #[serde(default = "default_redis_listen")]
    pub listen: String,
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Require token-based authentication for all connections.
    #[serde(default)]
    pub enabled: bool,

    /// The shared secret token clients must provide.
    #[serde(default)]
    pub token: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            enabled: false,
            token: String::new(),
        }
    }
}

/// Resource limits for protecting against DoS and OOM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    /// Maximum concurrent client connections (0 = unlimited).
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Maximum document size in bytes.
    #[serde(default = "default_max_document_bytes")]
    pub max_document_bytes: usize,

    /// Maximum number of documents returned by a single find() query.
    #[serde(default = "default_max_result_count")]
    pub max_result_count: usize,

    /// Maximum collection name length.
    #[serde(default = "default_max_collection_name_len")]
    pub max_collection_name_len: usize,

    /// Maximum RESP bulk string size (Redis protocol).
    #[serde(default = "default_max_resp_bulk_bytes")]
    pub max_resp_bulk_bytes: usize,

    /// Maximum RESP array elements (Redis protocol).
    #[serde(default = "default_max_resp_array_len")]
    pub max_resp_array_len: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        LimitsConfig {
            max_connections: default_max_connections(),
            max_document_bytes: default_max_document_bytes(),
            max_result_count: default_max_result_count(),
            max_collection_name_len: default_max_collection_name_len(),
            max_resp_bulk_bytes: default_max_resp_bulk_bytes(),
            max_resp_array_len: default_max_resp_array_len(),
        }
    }
}

fn default_max_connections() -> usize { 1024 }
fn default_max_document_bytes() -> usize { 16 * 1024 * 1024 } // 16 MB
fn default_max_result_count() -> usize { 100_000 }
fn default_max_collection_name_len() -> usize { 128 }
fn default_max_resp_bulk_bytes() -> usize { 64 * 1024 * 1024 } // 64 MB
fn default_max_resp_array_len() -> usize { 100_000 }

/// Cluster configuration for multi-node deployments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Enable cluster mode.
    #[serde(default)]
    pub enabled: bool,

    /// Unique identifier for this node.
    #[serde(default)]
    pub node_id: String,

    /// Address for peer-to-peer communication.
    #[serde(default = "default_peer_listen")]
    pub peer_listen: String,

    /// All nodes in the cluster.
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
}

/// A single node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier.
    pub id: String,
    /// Peer-to-peer address (for inter-node communication).
    pub peer_addr: String,
    /// Client-facing address (for routing information).
    pub client_addr: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            enabled: false,
            node_id: String::new(),
            peer_listen: default_peer_listen(),
            nodes: Vec::new(),
        }
    }
}

fn default_peer_listen() -> String {
    "127.0.0.1:7655".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Number of entries to buffer before flushing to disk.
    #[serde(default = "default_wal_batch_size")]
    pub batch_size: usize,

    /// Byte threshold for the WAL buffer before flushing.
    #[serde(default = "default_wal_batch_bytes")]
    pub batch_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: default_data_dir(),
            listen: default_listen(),
            wal: WalConfig::default(),
            redis: RedisConfig::default(),
            cluster: ClusterConfig::default(),
            limits: LimitsConfig::default(),
            auth: AuthConfig::default(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        RedisConfig {
            enabled: false,
            listen: default_redis_listen(),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig {
            batch_size: default_wal_batch_size(),
            batch_bytes: default_wal_batch_bytes(),
        }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./fluxdb_data")
}

fn default_listen() -> String {
    "127.0.0.1:7654".to_string()
}

fn default_wal_batch_size() -> usize {
    64
}

fn default_wal_batch_bytes() -> usize {
    64 * 1024
}

fn default_redis_listen() -> String {
    "127.0.0.1:6379".to_string()
}

impl Config {
    /// Load config by merging: defaults < config file < CLI args.
    pub fn load(cli: &Cli) -> Result<Self, String> {
        let mut config = if let Some(path) = &cli.config {
            Self::from_file(path)?
        } else {
            // Try default locations
            let candidates = ["fluxdb.toml", "/etc/fluxdb/fluxdb.toml"];
            let mut found = Config::default();
            for path in &candidates {
                let p = Path::new(path);
                if p.exists() {
                    found = Self::from_file(p)?;
                    eprintln!("Loaded config from {path}");
                    break;
                }
            }
            found
        };

        // CLI overrides
        if let Some(dir) = &cli.data_dir {
            config.data_dir = dir.clone();
        }
        if let Some(addr) = &cli.listen {
            config.listen = addr.clone();
        }
        if let Some(size) = cli.wal_batch_size {
            config.wal.batch_size = size;
        }
        if let Some(bytes) = cli.wal_batch_bytes {
            config.wal.batch_bytes = bytes;
        }
        if let Some(ref redis_opt) = cli.redis {
            config.redis.enabled = true;
            if let Some(addr) = redis_opt {
                config.redis.listen = addr.clone();
            }
        }
        if let Some(ref token) = cli.auth_token {
            config.auth.enabled = true;
            config.auth.token = token.clone();
        }

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration values.
    fn validate(&self) -> Result<(), String> {
        // Validate listen address format
        if !self.listen.contains(':') {
            return Err(format!("invalid listen address '{}': must be host:port", self.listen));
        }

        // Validate WAL settings
        if self.wal.batch_size == 0 {
            return Err("wal.batch_size must be > 0".into());
        }
        if self.wal.batch_size > 100_000 {
            return Err("wal.batch_size must be <= 100000".into());
        }
        if self.wal.batch_bytes == 0 {
            return Err("wal.batch_bytes must be > 0".into());
        }
        if self.wal.batch_bytes > 256 * 1024 * 1024 {
            return Err("wal.batch_bytes must be <= 256MB".into());
        }

        // Validate limits
        if self.limits.max_document_bytes == 0 {
            return Err("limits.max_document_bytes must be > 0".into());
        }
        if self.limits.max_result_count == 0 {
            return Err("limits.max_result_count must be > 0".into());
        }

        // Validate Redis config
        if self.redis.enabled && !self.redis.listen.contains(':') {
            return Err(format!(
                "invalid redis listen address '{}': must be host:port",
                self.redis.listen
            ));
        }

        // Validate cluster config
        if self.cluster.enabled {
            if self.cluster.node_id.is_empty() {
                return Err("cluster.node_id must be set when cluster is enabled".into());
            }
            if self.cluster.nodes.is_empty() {
                return Err("cluster.nodes must not be empty when cluster is enabled".into());
            }
            if !self.cluster.peer_listen.contains(':') {
                return Err(format!(
                    "invalid cluster peer_listen '{}': must be host:port",
                    self.cluster.peer_listen
                ));
            }
            // Validate this node exists in the node list
            let found = self.cluster.nodes.iter().any(|n| n.id == self.cluster.node_id);
            if !found {
                return Err(format!(
                    "cluster.node_id '{}' not found in cluster.nodes",
                    self.cluster.node_id
                ));
            }
        }

        // Auth validation
        if self.auth.enabled && self.auth.token.is_empty() {
            return Err("auth.token must be set when auth is enabled".into());
        }

        // Warn about binding to all interfaces
        if self.listen.starts_with("0.0.0.0") && !self.auth.enabled {
            eprintln!(
                "WARNING: Listening on all interfaces (0.0.0.0) without authentication. \
                 Consider enabling auth for production use."
            );
        }

        Ok(())
    }

    fn from_file(path: &Path) -> Result<Self, String> {
        let contents = fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
        toml::from_str(&contents)
            .map_err(|e| format!("failed to parse config file {}: {e}", path.display()))
    }

    /// Serialize to TOML with comments.
    pub fn to_toml_string(&self) -> String {
        let mut out = String::new();
        out.push_str("# FluxDB configuration\n\n");
        out.push_str(&format!(
            "# Directory where database files are stored.\n\
             data_dir = {:?}\n\n",
            self.data_dir.display().to_string()
        ));
        out.push_str(&format!(
            "# Address and port to listen on.\n\
             listen = {:?}\n\n",
            self.listen
        ));
        out.push_str("[wal]\n");
        out.push_str(&format!(
            "# Number of WAL entries to buffer before flushing to disk.\n\
             batch_size = {}\n\n",
            self.wal.batch_size
        ));
        out.push_str(&format!(
            "# Byte threshold for the WAL write buffer before flushing.\n\
             batch_bytes = {}\n\n",
            self.wal.batch_bytes
        ));
        out.push_str("[redis]\n");
        out.push_str(&format!(
            "# Enable Redis-compatible protocol server.\n\
             enabled = {}\n\n",
            self.redis.enabled
        ));
        out.push_str(&format!(
            "# Address and port for the Redis protocol server.\n\
             listen = {:?}\n\n",
            self.redis.listen
        ));
        out.push_str("[limits]\n");
        out.push_str(&format!(
            "# Maximum concurrent client connections (0 = unlimited).\n\
             max_connections = {}\n\n",
            self.limits.max_connections
        ));
        out.push_str(&format!(
            "# Maximum document size in bytes.\n\
             max_document_bytes = {}\n\n",
            self.limits.max_document_bytes
        ));
        out.push_str(&format!(
            "# Maximum documents returned by a single find() query.\n\
             max_result_count = {}\n\n",
            self.limits.max_result_count
        ));
        out.push_str("[auth]\n");
        out.push_str(&format!(
            "# Require token authentication for connections.\n\
             enabled = {}\n\n",
            self.auth.enabled
        ));
        out.push_str(
            "# Shared secret token.\n\
             # token = \"your-secret-token-here\"\n\n",
        );
        out.push_str("[cluster]\n");
        out.push_str(&format!(
            "# Enable distributed cluster mode.\n\
             enabled = {}\n\n",
            self.cluster.enabled
        ));
        out.push_str(&format!(
            "# Unique identifier for this node.\n\
             node_id = {:?}\n\n",
            self.cluster.node_id
        ));
        out.push_str(&format!(
            "# Address for peer-to-peer communication between nodes.\n\
             peer_listen = {:?}\n\n",
            self.cluster.peer_listen
        ));
        out.push_str(
            "# List all nodes in the cluster.\n\
             # [[cluster.nodes]]\n\
             # id = \"node-0\"\n\
             # peer_addr = \"127.0.0.1:7655\"\n\
             # client_addr = \"127.0.0.1:7654\"\n",
        );
        out
    }
}
