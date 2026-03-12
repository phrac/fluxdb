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
    let sync_mode_str = match config.wal.sync_mode {
        fluxdb::wal::SyncMode::EveryFlush => "every_flush",
        fluxdb::wal::SyncMode::None => "none",
    };
    eprintln!("WAL sync:       {}", sync_mode_str);
    if config.limits.max_connections > 0 {
        eprintln!("Max connections: {}", config.limits.max_connections);
    }
    if config.server.request_timeout_secs > 0 {
        eprintln!("Request timeout: {}s", config.server.request_timeout_secs);
    }
    if config.server.idle_timeout_secs > 0 {
        eprintln!("Idle timeout:    {}s", config.server.idle_timeout_secs);
    }
    if config.compaction.auto {
        eprintln!(
            "Auto-compaction: every {}s, max WAL {}MB",
            config.compaction.interval_secs,
            config.compaction.max_wal_bytes / (1024 * 1024)
        );
    }
    if config.log.slow_query_ms > 0 {
        eprintln!("Slow query log:  {}ms", config.log.slow_query_ms);
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

    // Optionally start auto-compaction background task
    if config.compaction.auto {
        let compact_db = server.db();
        let compact_notify = compact_db.compact_notify();
        let interval = std::time::Duration::from_secs(config.compaction.interval_secs);
        let mut compact_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            tick.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = compact_notify.notified() => {
                        eprintln!("fluxdb: WAL size threshold exceeded, triggering compaction");
                    }
                    _ = compact_shutdown.recv() => break,
                }
                let db = compact_db.clone();
                let result = tokio::task::spawn_blocking(move || db.compact()).await;
                match result {
                    Ok(Ok(())) => eprintln!("fluxdb: auto-compaction completed"),
                    Ok(Err(e)) => eprintln!("fluxdb: auto-compaction failed: {e}"),
                    Err(e) => eprintln!("fluxdb: auto-compaction task panic: {e}"),
                }
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

    // Flush WAL before exit (uses spawn_blocking because db.flush() calls
    // blocking_send on the WAL channel, which cannot run in an async context)
    eprintln!("Flushing WAL...");
    let flush_result = tokio::task::spawn_blocking(move || db.flush()).await;
    match flush_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("WARNING: Failed to flush WAL: {e}"),
        Err(e) => eprintln!("WARNING: WAL flush task panicked: {e}"),
    }
    eprintln!("Shutdown complete.");
}
