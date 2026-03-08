use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::cluster::peer::PeerClient;

const HEALTH_INTERVAL: Duration = Duration::from_secs(5);
const UNHEALTHY_THRESHOLD: u32 = 3;

/// Tracks the health of all peer nodes.
pub struct HealthChecker {
    peers: HashMap<String, Arc<PeerClient>>,
    status: Arc<RwLock<HashMap<String, NodeStatus>>>,
}

#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub healthy: bool,
    pub last_seen: Option<Instant>,
    pub consecutive_failures: u32,
}

impl HealthChecker {
    pub fn new(peers: HashMap<String, Arc<PeerClient>>) -> Self {
        let mut status = HashMap::new();
        for node_id in peers.keys() {
            status.insert(
                node_id.clone(),
                NodeStatus {
                    healthy: true, // assume healthy until proven otherwise
                    last_seen: None,
                    consecutive_failures: 0,
                },
            );
        }

        HealthChecker {
            peers,
            status: Arc::new(RwLock::new(status)),
        }
    }

    /// Get a shared handle to the status map (for reading from other tasks).
    pub fn status_handle(&self) -> Arc<RwLock<HashMap<String, NodeStatus>>> {
        Arc::clone(&self.status)
    }

    /// Run the health check loop. This never returns.
    pub async fn run(self) {
        loop {
            for (node_id, peer) in &self.peers {
                let reachable = peer.ping().await;
                let mut status = self.status.write().await;
                if let Some(s) = status.get_mut(node_id) {
                    if reachable {
                        s.healthy = true;
                        s.last_seen = Some(Instant::now());
                        s.consecutive_failures = 0;
                    } else {
                        s.consecutive_failures += 1;
                        if s.consecutive_failures >= UNHEALTHY_THRESHOLD {
                            if s.healthy {
                                eprintln!("fluxdb: node {node_id} marked unhealthy");
                            }
                            s.healthy = false;
                        }
                    }
                }
            }
            tokio::time::sleep(HEALTH_INTERVAL).await;
        }
    }
}
