# FluxDB

A fast, document-oriented NoSQL database written in Rust.

FluxDB stores JSON documents in named collections, supports MongoDB-style queries with secondary index acceleration, and persists data using a crash-safe write-ahead log. It can run as a standalone TCP server, a Redis-compatible server, an embedded library, or in a distributed cluster.

## Performance

Benchmarked against established storage engines on equivalent workloads (10,000 JSON documents, single-threaded, in-memory/WAL mode):

| Operation | FluxDB | SQLite (WAL) | RocksDB | redb | MongoDB |
|-----------|-------:|-------------:|--------:|-----:|--------:|
| **Insert** | **2.8 µs** | 12.4 µs | 2.7 µs | 4,730 µs | 55.3 µs |
| **Get** | **400 ns** | 1,230 ns | 444 ns | 639 ns | 58,517 ns |
| **Scan 10K** | **18 µs** | 1,053 µs | 818 µs | 353 µs | 10,802 µs |

FluxDB's zero-copy scan path is 19x faster than redb and 58x faster than SQLite. Point reads are 3x faster than SQLite, on par with RocksDB.

Run the benchmarks yourself:

```bash
cargo bench --bench comparison
```

## Features

- **Document storage** — JSON documents with auto-generated UUIDs or custom IDs
- **Collections** — logical grouping of documents
- **Query engine** — MongoDB-compatible filter syntax (`$eq`, `$gt`, `$in`, `$or`, `$and`, `$not`, `$exists`, etc.) with dot-notation for nested fields
- **Secondary indexes** — B-tree indexes on arbitrary fields for fast equality and range queries
- **Projections** — include/exclude specific fields from query results
- **Crash-safe WAL** — binary write-ahead log with CRC32 checksums, atomic compaction via temp-file + rename, and corruption recovery
- **Batched writes** — WAL entries are buffered and flushed in batches (configurable) for throughput
- **Zero-copy scans** — pre-serialized document cache enables bulk reads without per-document serialization
- **Per-collection concurrency** — RwLocks per collection allow parallel reads with minimal contention
- **TCP server** — async multi-client server with line-delimited JSON protocol
- **Redis protocol** — drop-in Redis compatibility (GET, SET, HSET, HGET, and more)
- **Horizontal scaling** — consistent-hash cluster mode with automatic sharding and scatter-gather queries
- **Pluggable storage** — `StorageBackend` trait for custom persistence (WAL, in-memory, or your own)
- **Embeddable** — use as a library with no runtime dependencies; works on `wasm32` targets
- **TOML configuration** — file-based config with CLI flag overrides
- **Backward compatible** — reads legacy JSON-format WAL files automatically

## Quick start

```bash
# Build and run with defaults (listens on 127.0.0.1:7654, data in ./fluxdb_data)
cargo run --release

# Or specify options
cargo run --release -- --listen 0.0.0.0:7654 --data-dir /var/lib/fluxdb

# Generate a default config file
cargo run --release -- --init-config fluxdb.toml
```

### Configuration

FluxDB loads config from `fluxdb.toml` (current directory) or `/etc/fluxdb/fluxdb.toml`. CLI flags override file values.

```toml
data_dir = "./fluxdb_data"
listen = "127.0.0.1:7654"

[wal]
batch_size = 64        # flush after N entries
batch_bytes = 65536    # flush after N bytes

[redis]
enabled = false
listen = "127.0.0.1:6379"

[cluster]
enabled = false
node_id = "node-0"
peer_listen = "127.0.0.1:7655"

[[cluster.nodes]]
id = "node-0"
peer_addr = "127.0.0.1:7655"
client_addr = "127.0.0.1:7654"
```

### Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `server` | yes | TCP server, CLI, TOML config (requires tokio) |
| `persistence` | yes (via server) | WAL-based disk persistence |
| `redis` | no | Redis-compatible protocol server |
| `cluster` | no | Distributed cluster mode |

To use FluxDB as an embedded library without the server:

```toml
[dependencies]
fluxdb = { version = "0.1", default-features = false }
```

## Protocol

FluxDB uses a line-delimited JSON protocol over TCP. Send one JSON object per line, receive one JSON response per line.

### Create a collection

```json
{"cmd": "create_collection", "name": "users"}
```

### Insert a document

```json
{"cmd": "insert", "collection": "users", "document": {"name": "Alice", "age": 30}}
```

Response:

```json
{"ok": true, "id": "550e8400-e29b-41d4-a716-446655440000"}
```

### Get a document by ID

```json
{"cmd": "get", "collection": "users", "id": "550e8400-e29b-41d4-a716-446655440000"}
```

### Find documents with a filter

```json
{"cmd": "find", "collection": "users", "filter": {"age": {"$gte": 25}}, "limit": 10, "skip": 0}
```

### Update a document

```json
{"cmd": "update", "collection": "users", "id": "550e8400-...", "document": {"name": "Alice", "age": 31}}
```

### Delete a document

```json
{"cmd": "delete", "collection": "users", "id": "550e8400-..."}
```

### Secondary indexes

```json
{"cmd": "create_index", "collection": "users", "field": "age"}
{"cmd": "list_indexes", "collection": "users"}
{"cmd": "drop_index", "collection": "users", "field": "age"}
```

Indexes accelerate equality (`$eq`), range (`$gt`, `$gte`, `$lt`, `$lte`), and membership (`$in`) queries on the indexed field.

### Other commands

```json
{"cmd": "count", "collection": "users", "filter": {"age": {"$gte": 25}}}
{"cmd": "list_collections"}
{"cmd": "drop_collection", "name": "users"}
{"cmd": "compact"}
{"cmd": "stats"}
```

## Query operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equal | `{"age": {"$eq": 30}}` |
| `$ne` | Not equal | `{"status": {"$ne": "inactive"}}` |
| `$gt` | Greater than | `{"age": {"$gt": 25}}` |
| `$gte` | Greater than or equal | `{"age": {"$gte": 25}}` |
| `$lt` | Less than | `{"age": {"$lt": 40}}` |
| `$lte` | Less than or equal | `{"age": {"$lte": 40}}` |
| `$in` | In array | `{"status": {"$in": ["active", "pending"]}}` |
| `$nin` | Not in array | `{"status": {"$nin": ["deleted"]}}` |
| `$exists` | Field exists | `{"email": {"$exists": true}}` |
| `$and` | Logical AND | `{"$and": [{"age": {"$gte": 25}}, {"status": "active"}]}` |
| `$or` | Logical OR | `{"$or": [{"age": 25}, {"age": 30}]}` |
| `$not` | Logical NOT | `{"$not": {"status": "inactive"}}` |

Implicit equality: `{"name": "Alice"}` is equivalent to `{"name": {"$eq": "Alice"}}`.

Dot notation for nested fields: `{"address.city": "Portland"}`.

## Redis compatibility

Enable the Redis protocol to use FluxDB with existing Redis clients:

```bash
cargo run --release --features redis -- --config fluxdb.toml
```

With `redis.enabled = true` in your config, FluxDB accepts Redis commands on port 6379:

```bash
redis-cli SET mykey "hello"
redis-cli GET mykey
redis-cli HSET user:1 name Alice age 30
redis-cli HGETALL user:1
```

Supported commands: `GET`, `SET`, `DEL`, `EXISTS`, `KEYS`, `MSET`, `MGET`, `HSET`, `HGET`, `HGETALL`, `HDEL`, `HLEN`, `SELECT`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `INFO`.

## Cluster mode

FluxDB supports horizontal scaling via consistent-hash sharding across multiple nodes.

```bash
cargo run --release --features cluster -- --config node0.toml
```

Each node gets a static configuration listing all cluster members. Documents are automatically routed to the correct node based on `hash(collection, doc_id)`:

- **Point operations** (get, insert, update, delete) are routed to the owning node
- **Queries** (find, count) use scatter-gather across all nodes
- **DDL** (create_collection, create_index) is broadcast to all nodes
- **Health monitoring** pings peers every 5 seconds with automatic failure detection

The consistent hash ring uses 128 virtual nodes per physical node, ensuring minimal data movement when nodes are added or removed (~25% redistribution per new node).

## Architecture

```
src/
  main.rs          — entry point, CLI parsing, server startup
  lib.rs           — module declarations, feature gates
  config.rs        — TOML config, CLI flags, defaults
  error.rs         — error types
  document.rs      — JSON document model with dot-notation field access
  collection.rs    — document storage, CRUD, index-accelerated queries, zero-copy scan
  index.rs         — B-tree secondary indexes with range/equality lookups
  query.rs         — query filter evaluation and projection
  wal.rs           — binary WAL with CRC32, atomic compaction, legacy reader
  storage.rs       — pluggable StorageBackend trait (WAL, memory)
  database.rs      — database engine, per-collection RwLocks, WAL replay
  server.rs        — async TCP server (tokio), command dispatch
  redis.rs         — Redis RESP protocol server
  cluster/
    ring.rs        — consistent hash ring (128 vnodes, CRC32)
    router.rs      — command routing, scatter-gather, broadcast
    peer.rs        — TCP peer client with auto-reconnect
    health.rs      — periodic health checks, failure detection
```

### Design

- **Per-collection RwLock** — reads on different collections never block each other; only writes acquire exclusive access
- **B-tree indexes** — equality and range queries skip full collection scans
- **Zero-copy scans** — documents cache their serialized bytes; bulk scans avoid per-document serialization
- **Batched WAL** — writes are buffered (64 entries or 64 KB by default), reducing fsync overhead
- **Atomic compaction** — WAL compaction writes to a temp file and atomically renames, ensuring crash safety
- **CRC32 recovery** — on startup, corrupted WAL entries are skipped while preserving all valid entries before the corruption point
- **spawn_blocking** — database operations run on tokio's blocking pool, keeping the async runtime responsive

## Testing

```bash
cargo test                    # 66 tests (core + server)
cargo test --features cluster # 76 tests (adds cluster integration)
```

Covers documents, queries, collections, indexes, WAL persistence, crash recovery, CRC corruption recovery, atomic compaction, concurrent access, hash ring distribution, and cluster routing.
