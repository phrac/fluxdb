use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::{FluxError, Result};

/// Client for communicating with a remote FluxDB node.
///
/// Holds a persistent TCP connection (reconnects on failure).
/// Uses the same JSON line-delimited protocol as client connections.
pub struct PeerClient {
    addr: String,
    conn: Mutex<Option<PeerConn>>,
    connect_timeout: Duration,
    request_timeout: Duration,
}

struct PeerConn {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl PeerClient {
    pub fn new(addr: String, connect_timeout_secs: u64, request_timeout_secs: u64) -> Self {
        PeerClient {
            addr,
            conn: Mutex::new(None),
            connect_timeout: Duration::from_secs(connect_timeout_secs),
            request_timeout: Duration::from_secs(request_timeout_secs),
        }
    }

    /// Send a request to the remote node and return the response.
    ///
    /// Automatically marks the request as `_routed: true` to prevent
    /// the remote node from re-routing it.
    pub async fn forward(&self, mut request: Value) -> Result<Value> {
        // Mark as routed to prevent forwarding loops
        if let Some(obj) = request.as_object_mut() {
            obj.insert("_routed".to_string(), Value::Bool(true));
        }

        let request_bytes = {
            let mut s = serde_json::to_string(&request)
                .map_err(|e| FluxError::ClusterError(format!("serialize: {e}")))?;
            s.push('\n');
            s.into_bytes()
        };

        let mut conn = self.conn.lock().await;

        // Try to send on existing connection, reconnect on failure
        for attempt in 0..2 {
            if conn.is_none() {
                *conn = self.connect().await.ok();
            }

            if let Some(ref mut c) = *conn {
                match self.send_recv(c, &request_bytes).await {
                    Ok(response) => return Ok(response),
                    Err(_) if attempt == 0 => {
                        // Connection stale — drop and retry
                        *conn = None;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                return Err(FluxError::NodeUnreachable(self.addr.clone()));
            }
        }

        Err(FluxError::NodeUnreachable(self.addr.clone()))
    }

    async fn connect(&self) -> Result<PeerConn> {
        let stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| FluxError::NodeUnreachable(format!("{}: connect timeout", self.addr)))?
            .map_err(|e| FluxError::NodeUnreachable(format!("{}: {e}", self.addr)))?;

        let (read_half, write_half) = stream.into_split();
        Ok(PeerConn {
            reader: BufReader::new(read_half),
            writer: write_half,
        })
    }

    async fn send_recv(&self, conn: &mut PeerConn, request: &[u8]) -> Result<Value> {
        conn.writer
            .write_all(request)
            .await
            .map_err(|e| FluxError::NodeUnreachable(format!("{}: write: {e}", self.addr)))?;
        conn.writer
            .flush()
            .await
            .map_err(|e| FluxError::NodeUnreachable(format!("{}: flush: {e}", self.addr)))?;

        let mut line = String::new();
        tokio::time::timeout(self.request_timeout, conn.reader.read_line(&mut line))
            .await
            .map_err(|_| FluxError::NodeUnreachable(format!("{}: read timeout", self.addr)))?
            .map_err(|e| FluxError::NodeUnreachable(format!("{}: read: {e}", self.addr)))?;

        serde_json::from_str(&line)
            .map_err(|e| FluxError::ClusterError(format!("bad response from {}: {e}", self.addr)))
    }

    /// Check if the peer is reachable by sending a ping.
    pub async fn ping(&self) -> bool {
        let request = serde_json::json!({"cmd": "stats"});
        self.forward(request).await.is_ok()
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}
