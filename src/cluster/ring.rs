use std::collections::BTreeMap;

use crate::config::NodeConfig;

const VIRTUAL_NODES: u32 = 128;

/// Consistent hash ring for distributing documents across cluster nodes.
///
/// Uses CRC32 with virtual nodes to ensure even distribution.
/// The shard key is `"{collection}/{doc_id}"`.
pub struct HashRing {
    ring: BTreeMap<u32, String>, // ring position -> node_id
    nodes: Vec<NodeConfig>,
}

impl HashRing {
    /// Build a hash ring from the given node configs.
    pub fn new(nodes: Vec<NodeConfig>) -> Self {
        let mut ring = BTreeMap::new();

        for node in &nodes {
            for vn in 0..VIRTUAL_NODES {
                let key = format!("{}:{}", node.id, vn);
                let hash = crc32fast::hash(key.as_bytes());
                ring.insert(hash, node.id.clone());
            }
        }

        HashRing { ring, nodes }
    }

    /// Find the node that owns a given document.
    pub fn owner(&self, collection: &str, doc_id: &str) -> Option<&NodeConfig> {
        if self.ring.is_empty() {
            return None;
        }
        let key = format!("{collection}/{doc_id}");
        let hash = crc32fast::hash(key.as_bytes());
        let node_id = self.find_node(hash);
        self.node_by_id(node_id)
    }

    /// Find which node owns a given ring position (clockwise walk).
    fn find_node(&self, hash: u32) -> &str {
        // Walk clockwise: find the first key >= hash
        if let Some((_, node_id)) = self.ring.range(hash..).next() {
            return node_id;
        }
        // Wrap around to the first entry
        self.ring.values().next().unwrap()
    }

    /// Get all nodes (for scatter-gather operations).
    pub fn all_nodes(&self) -> &[NodeConfig] {
        &self.nodes
    }

    /// Get a node config by ID.
    pub fn node_by_id(&self, id: &str) -> Option<&NodeConfig> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Number of physical nodes in the ring.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_nodes(n: usize) -> Vec<NodeConfig> {
        (0..n)
            .map(|i| NodeConfig {
                id: format!("node-{i}"),
                peer_addr: format!("127.0.0.1:{}", 7700 + i),
                client_addr: format!("127.0.0.1:{}", 7600 + i),
            })
            .collect()
    }

    #[test]
    fn test_ring_deterministic() {
        let nodes = test_nodes(3);
        let ring = HashRing::new(nodes.clone());

        let owner1 = ring.owner("users", "doc-1").unwrap();
        let owner2 = ring.owner("users", "doc-1").unwrap();
        assert_eq!(owner1.id, owner2.id);
    }

    #[test]
    fn test_ring_distributes_across_nodes() {
        let nodes = test_nodes(3);
        let ring = HashRing::new(nodes);

        let mut counts = std::collections::HashMap::new();
        for i in 0..1000 {
            let owner = ring.owner("bench", &format!("doc-{i}")).unwrap();
            *counts.entry(owner.id.clone()).or_insert(0) += 1;
        }

        // Each node should have at least 20% of the docs (expect ~33%)
        for (node_id, count) in &counts {
            assert!(
                *count >= 200,
                "node {node_id} only got {count}/1000 docs — distribution is too uneven"
            );
        }
        assert_eq!(counts.len(), 3);
    }

    #[test]
    fn test_ring_single_node() {
        let nodes = test_nodes(1);
        let ring = HashRing::new(nodes);

        // Everything should go to the one node
        for i in 0..100 {
            let owner = ring.owner("col", &format!("d-{i}")).unwrap();
            assert_eq!(owner.id, "node-0");
        }
    }

    #[test]
    fn test_ring_minimal_disruption_on_add() {
        let nodes3 = test_nodes(3);
        let ring3 = HashRing::new(nodes3);

        let nodes4 = test_nodes(4);
        let ring4 = HashRing::new(nodes4);

        let mut moved = 0;
        for i in 0..1000 {
            let key = format!("doc-{i}");
            let owner3 = ring3.owner("col", &key).unwrap();
            let owner4 = ring4.owner("col", &key).unwrap();
            if owner3.id != owner4.id {
                moved += 1;
            }
        }

        // Ideal: ~25% move when adding a 4th node. Allow up to 40%.
        assert!(
            moved < 400,
            "too many docs moved: {moved}/1000 — consistent hashing not working"
        );
    }

    #[test]
    fn test_ring_empty() {
        let ring = HashRing::new(vec![]);
        assert!(ring.owner("col", "doc").is_none());
    }
}
