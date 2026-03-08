#![cfg(feature = "cluster")]

use std::sync::Arc;

use fluxdb::cluster::ring::HashRing;
use fluxdb::cluster::router::ClusterRouter;
use fluxdb::config::{ClusterConfig, NodeConfig};
use fluxdb::database::Database;
use serde_json::json;

fn make_cluster_config(node_id: &str) -> (ClusterConfig, Vec<NodeConfig>) {
    let nodes = vec![
        NodeConfig {
            id: "node-0".into(),
            peer_addr: "127.0.0.1:17700".into(),
            client_addr: "127.0.0.1:17600".into(),
        },
        NodeConfig {
            id: "node-1".into(),
            peer_addr: "127.0.0.1:17701".into(),
            client_addr: "127.0.0.1:17601".into(),
        },
        NodeConfig {
            id: "node-2".into(),
            peer_addr: "127.0.0.1:17702".into(),
            client_addr: "127.0.0.1:17602".into(),
        },
    ];

    let config = ClusterConfig {
        enabled: true,
        node_id: node_id.into(),
        peer_listen: "unused".into(),
        nodes: nodes.clone(),
    };

    (config, nodes)
}

#[test]
fn test_hash_ring_routing_is_consistent() {
    let (_, nodes) = make_cluster_config("node-0");
    let ring = HashRing::new(nodes);

    // The same (collection, id) always routes to the same node
    let owner1 = ring.owner("users", "alice").unwrap();
    let owner2 = ring.owner("users", "alice").unwrap();
    assert_eq!(owner1.id, owner2.id);

    // Different docs can route to different nodes
    let mut owners = std::collections::HashSet::new();
    for i in 0..100 {
        let owner = ring.owner("users", &format!("user-{i}")).unwrap();
        owners.insert(owner.id.clone());
    }
    // With 100 docs and 3 nodes, all nodes should get some docs
    assert_eq!(owners.len(), 3);
}

#[tokio::test]
async fn test_router_local_insert_and_get() {
    let (config, _) = make_cluster_config("node-0");
    let db = Arc::new(Database::open_memory());

    // Create collection locally (since broadcast won't work without real peers)
    db.create_collection("test").unwrap();

    let router = ClusterRouter::new(&config, db);

    // Find a doc ID that hashes to node-0 (our local node)
    let ring = HashRing::new(config.nodes.clone());
    let mut local_id = String::new();
    for i in 0..1000 {
        let id = format!("doc-{i}");
        if ring.owner("test", &id).unwrap().id == "node-0" {
            local_id = id;
            break;
        }
    }
    assert!(!local_id.is_empty(), "couldn't find a doc that hashes to node-0");

    // Insert locally-routed doc
    let resp = router
        .route(&json!({
            "cmd": "insert",
            "collection": "test",
            "document": {"_id": local_id, "name": "Alice"}
        }))
        .await;
    assert_eq!(resp["ok"], true, "insert failed: {resp}");

    // Get it back
    let resp = router
        .route(&json!({
            "cmd": "get",
            "collection": "test",
            "id": local_id
        }))
        .await;
    assert_eq!(resp["ok"], true, "get failed: {resp}");
    assert_eq!(resp["document"]["name"], "Alice");
}

#[tokio::test]
async fn test_router_scatter_count_local_only() {
    // With no real peers, scatter will only count local docs
    let (config, _) = make_cluster_config("node-0");
    let db = Arc::new(Database::open_memory());
    db.create_collection("items").unwrap();

    // Insert some docs directly (bypass routing)
    for i in 0..10 {
        db.insert("items", json!({"_id": format!("item-{i}"), "val": i}))
            .unwrap();
    }

    let router = ClusterRouter::new(&config, db);
    let resp = router.route(&json!({"cmd": "count", "collection": "items"})).await;

    // Should count at least the local docs (peers will fail, but local works)
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["count"], 10);
}

#[tokio::test]
async fn test_router_broadcast_create_collection() {
    let (config, _) = make_cluster_config("node-0");
    let db = Arc::new(Database::open_memory());
    let router = ClusterRouter::new(&config, Arc::clone(&db));

    // Broadcast create_collection — local node should succeed, peers will fail
    // but local should still work
    let resp = router
        .route(&json!({"cmd": "create_collection", "name": "users"}))
        .await;

    // Peers are unreachable, so broadcast returns an error, but local was created
    // Check that the collection exists locally
    let collections = db.list_collections();
    assert!(collections.contains(&"users".to_string()));
}

#[test]
fn test_ring_minimal_movement_on_node_change() {
    let nodes3: Vec<NodeConfig> = (0..3)
        .map(|i| NodeConfig {
            id: format!("node-{i}"),
            peer_addr: format!("127.0.0.1:{}", 7700 + i),
            client_addr: format!("127.0.0.1:{}", 7600 + i),
        })
        .collect();
    let ring3 = HashRing::new(nodes3);

    let nodes4: Vec<NodeConfig> = (0..4)
        .map(|i| NodeConfig {
            id: format!("node-{i}"),
            peer_addr: format!("127.0.0.1:{}", 7700 + i),
            client_addr: format!("127.0.0.1:{}", 7600 + i),
        })
        .collect();
    let ring4 = HashRing::new(nodes4);

    let mut moved = 0;
    let total = 10_000;
    for i in 0..total {
        let key = format!("doc-{i}");
        let o3 = ring3.owner("data", &key).unwrap();
        let o4 = ring4.owner("data", &key).unwrap();
        if o3.id != o4.id {
            moved += 1;
        }
    }

    let pct = (moved as f64 / total as f64) * 100.0;
    // With consistent hashing, only ~25% should move (1/N where N=4)
    assert!(
        pct < 35.0,
        "too many docs moved: {moved}/{total} ({pct:.1}%) — expected ~25%"
    );
}
