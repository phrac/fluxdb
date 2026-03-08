use std::fs;

use clap::Parser;
use tokio::sync::broadcast;

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
    if config.limits.max_connections > 0 {
        eprintln!("Max connections: {}", config.limits.max_connections);
    }
    if config.auth.enabled {
        eprintln!("Authentication: enabled");
    }

    #[cfg(feature = "cluster")]
    if config.cluster.enabled {
        eprintln!(
            "Cluster mode:   node={}, {} nodes, peer={}",
            config.cluster.node_id,
            config.cluster.nodes.len(),
            config.cluster.peer_listen
        );
    }

    let server = match Server::from_config(&config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize database: {e}");
            std::process::exit(1);
        }
    };

    // Create shutdown broadcast channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Optionally start the Redis-compatible protocol server
    #[cfg(feature = "redis")]
    if config.redis.enabled {
        let redis_addr = config.redis.listen.clone();
        let db = server.db();
        let max_connections = server.max_connections();
        let auth_token = server.auth_token().map(String::from);
        let max_bulk = config.limits.max_resp_bulk_bytes;
        let max_array = config.limits.max_resp_array_len;
        let mut redis_shutdown = shutdown_tx.subscribe();
        eprintln!("Redis protocol: {}", redis_addr);
        tokio::spawn(async move {
            let redis_server = fluxdb::redis::RedisServer::new(
                db,
                redis_addr,
                max_connections,
                auth_token,
                max_bulk,
                max_array,
            );
            if let Err(e) = redis_server.run(&mut redis_shutdown).await {
                eprintln!("Redis server error: {e}");
            }
        });
    }

    // Spawn the main server with a shutdown receiver
    let shutdown_rx = shutdown_tx.subscribe();
    let db = server.db();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run(shutdown_rx).await {
            eprintln!("Server error: {e}");
        }
    });

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\nReceived Ctrl+C, shutting down gracefully...");
        }
        _ = server_handle => {
            // Server exited on its own
        }
    }

    // Signal all tasks to shut down
    let _ = shutdown_tx.send(());

    // Flush WAL before exit
    eprintln!("Flushing WAL...");
    if let Err(e) = db.flush() {
        eprintln!("WARNING: Failed to flush WAL: {e}");
    }
    eprintln!("Shutdown complete.");
}
