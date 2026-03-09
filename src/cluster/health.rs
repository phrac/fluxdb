use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::cluster::peer::PeerClient;

/// Tracks the health of all peer nodes.
pub struct HealthChecker {
    peers: HashMap<String, Arc<PeerClient>>,
    status: Arc<RwLock<HashMap<String, NodeStatus>>>,
    health_interval: Duration,
    unhealthy_threshold: u32,
}

#[derive(Debug, Clone)]
pub struct NodeStatus {
    pub healthy: bool,
    pub last_seen: Option<Instant>,
    pub consecutive_failures: u32,
}

impl HealthChecker {
    pub fn new(
        peers: HashMap<String, Arc<PeerClient>>,
        health_check_interval_secs: u64,
        unhealthy_threshold: u32,
    ) -> Self {
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
            health_interval: Duration::from_secs(health_check_interval_secs),
            unhealthy_threshold,
        }
    }

    /// Get a shared handle to the status map (for reading from other tasks).
    pub fn status_handle(&self) -> Arc<RwLock<HashMap<String, NodeStatus>>> {
        Arc::clone(&self.status)
    }

    /// Run the health check loop. This never returns.
    pub async fn run(self) {
        loop {
            // Check all peers concurrently instead of serially
            let mut handles = Vec::with_capacity(self.peers.len());
            for (node_id, peer) in &self.peers {
                let peer = Arc::clone(peer);
                let node_id = node_id.clone();
                handles.push(tokio::spawn(async move {
                    let reachable = peer.ping().await;
                    (node_id, reachable)
                }));
            }

            // Collect results and update status
            for handle in handles {
                if let Ok((node_id, reachable)) = handle.await {
                    let mut status = self.status.write().await;
                    if let Some(s) = status.get_mut(&node_id) {
                        if reachable {
                            if !s.healthy {
                                eprintln!("fluxdb: node {node_id} recovered, marking healthy");
                            }
                            s.healthy = true;
                            s.last_seen = Some(Instant::now());
                            s.consecutive_failures = 0;
                        } else {
                            s.consecutive_failures += 1;
                            if s.consecutive_failures >= self.unhealthy_threshold {
                                if s.healthy {
                                    eprintln!("fluxdb: node {node_id} marked unhealthy after {} failures", s.consecutive_failures);
                                }
                                s.healthy = false;
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(self.health_interval).await;
        }
    }
}
