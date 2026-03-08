use std::fs;

use clap::Parser;

use fluxdb::config::{Cli, Config};
use fluxdb::server::Server;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Handle --init-config
    if let Some(path) = &cli.init_config {
        let config = Config::default();
        let toml_str = config.to_toml_string();
        if let Err(e) = fs::write(path, &toml_str) {
            eprintln!("Failed to write config: {e}");
            std::process::exit(1);
        }
        eprintln!("Wrote default config to {}", path.display());
        return;
    }

    let config = match Config::load(&cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {e}");
            std::process::exit(1);
        }
    };

    eprintln!("FluxDB v{}", env!("CARGO_PKG_VERSION"));
    eprintln!("Data directory: {}", config.data_dir.display());
    eprintln!("Listening on:   {}", config.listen);
    eprintln!(
        "WAL batch:      {} entries / {} bytes",
        config.wal.batch_size, config.wal.batch_bytes
    );

    let server = match Server::from_config(&config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize database: {e}");
            std::process::exit(1);
        }
    };

    // Optionally start the Redis-compatible protocol server
    #[cfg(feature = "redis")]
    if config.redis.enabled {
        let redis_addr = config.redis.listen.clone();
        let db = server.db();
        eprintln!("Redis protocol: {}", redis_addr);
        tokio::spawn(async move {
            let redis_server = fluxdb::redis::RedisServer::new(db, redis_addr);
            if let Err(e) = redis_server.run().await {
                eprintln!("Redis server error: {e}");
            }
        });
    }

    if let Err(e) = server.run().await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}
