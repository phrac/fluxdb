use std::path::PathBuf;

use fluxdb::server::Server;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    let data_dir = args
        .get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./fluxdb_data"));

    let addr = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("127.0.0.1:7654");

    eprintln!("FluxDB v{}", env!("CARGO_PKG_VERSION"));
    eprintln!("Data directory: {}", data_dir.display());

    let server = match Server::new(data_dir, addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize database: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = server.run().await {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}
