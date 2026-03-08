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

        Ok(config)
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
             listen = {:?}\n",
            self.redis.listen
        ));
        out
    }
}
