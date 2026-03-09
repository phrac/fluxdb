# FluxDB

> **Alpha software** — FluxDB is under active development. The on-disk data format (WAL) is not yet stable and may change between versions without a migration path. Do not use FluxDB for production data until the format is finalized (target: v1.0). Back up your data directory before upgrading.

A fast, document-oriented NoSQL database written in Rust.

FluxDB stores JSON documents in named collections, supports MongoDB-style queries with secondary index acceleration, and persists data using a crash-safe write-ahead log. It can run as a standalone TCP server, a Redis-compatible server, an embedded library, or in a distributed cluster.

## Performance

Benchmarked against established storage engines on equivalent workloads (10,000 JSON documents, single-threaded, in-memory/WAL mode):

| Operation | FluxDB | SQLite (WAL) | RocksDB | redb | MongoDB |
|-----------|-------:|-------------:|--------:|-----:|--------:|
| **Insert** | **2.89 µs** | 12.6 µs | 2.64 µs | 4,317 µs | 56.2 µs |
| **Get** | **421 ns** | 1,252 ns | 440 ns | 673 ns | 62,235 ns |
| **Scan 10K** | **26.0 µs** | 1,048 µs | 859 µs | 355 µs | 11,404 µs |

FluxDB's zero-copy scan path is 14x faster than redb and 40x faster than SQLite. Point reads are 3x faster than SQLite, on par with RocksDB. Inserts are 4x faster than SQLite and 19x faster than MongoDB.

Run the benchmarks yourself:

```bash
cargo bench --bench comparison
```

## Features

- **Document storage** — JSON documents with auto-generated UUIDs or custom IDs
- **Collections** — logical grouping of documents
- **Query engine** — MongoDB-compatible filter syntax (`$eq`, `$gt`, `$in`, `$or`, `$and`, `$not`, `$exists`, etc.) with dot-notation for nested fields and field-level `$not`
- **Sort** — `{"sort": {"field": 1}}` for ascending, `-1` for descending, multi-field supported
- **Secondary indexes** — B-tree indexes on arbitrary fields with type-unified numeric keys; `$or` and `$and` branches use indexes automatically with selectivity-first intersection
- **Projections** — include/exclude specific fields from query results
- **Deterministic pagination** — BTreeMap-backed storage ensures `skip`/`limit` always returns consistent results
- **Crash-safe WAL** — binary write-ahead log with CRC32 checksums, atomic compaction via temp-file + rename, corruption recovery, and native binary value encoding (no JSON-inside-bincode overhead)
- **Batched writes** — WAL entries are buffered and flushed in batches (configurable) for throughput
- **Zero-copy scans** — pre-serialized document cache enables bulk reads without per-document serialization
- **Per-collection concurrency** — RwLocks per collection allow parallel reads with minimal contention
- **TCP server** — async multi-client server with line-delimited JSON protocol
- **Redis protocol** — drop-in Redis compatibility (GET, SET, HSET, HGET, and more)
- **Horizontal scaling** — consistent-hash cluster mode with automatic sharding, scatter-gather queries, and health-aware routing
- **Authentication** — optional token-based auth for both JSON and Redis protocols
- **Connection limits** — configurable max connections with semaphore-based enforcement
- **Graceful shutdown** — `Ctrl-C` triggers coordinated shutdown with WAL flush before exit
- **Input validation** — collection name validation, document size limits, request size limits, query result caps
- **Lock poison recovery** — all mutex/RwLock acquisitions return errors instead of panicking
- **Pluggable storage** — `StorageBackend` trait for custom persistence (WAL, in-memory, or your own)
- **Embeddable** — use as a library with no runtime dependencies; works on `wasm32` targets
- **TOML configuration** — file-based config with CLI flag overrides and startup validation
- **Backward compatible** — reads legacy JSON-format WAL files automatically

## Getting started

### Docker (recommended)

```bash
# Single node
docker compose up -d

# Verify it's running
echo '{"cmd":"stats"}' | nc localhost 7654
```

This starts FluxDB with the JSON protocol on port 7654 and Redis protocol on port 6379. Data is persisted in a Docker volume.

To run a **3-node cluster**, edit `docker-compose.yml` — uncomment the cluster services and comment out the single-node service, then:

```bash
docker compose up -d
```

Pre-built cluster configs are in the `cluster/` directory using Docker networking (service names as hostnames).

### Docker (manual)

```bash
docker build -t fluxdb .
docker run -d -p 7654:7654 -v fluxdb-data:/var/lib/fluxdb fluxdb
```

### From source

```bash
cargo build --release
./target/release/fluxdb

# Or with Redis protocol
cargo build --release --features redis
./target/release/fluxdb --redis

# Build the CLI client
cargo build --release --features cli

# Generate a config file
./target/release/fluxdb --init-config fluxdb.toml
```

Defaults to `./fluxdb_data` for data and `127.0.0.1:7654` for the JSON protocol.

### Try it out

```bash
# Interactive REPL
fluxdb-cli
fluxdb> create collection users
fluxdb> insert users {"name": "Alice", "age": 30}
fluxdb> find users {"age": {"$gte": 25}}
fluxdb> count users
fluxdb> list collections

# One-shot commands
fluxdb-cli --cmd 'insert users {"name":"Bob","age":25}'
fluxdb-cli --cmd 'find users {"age":{"$gte":20}} --limit 10 --sort {"age":-1}'
fluxdb-cli --cmd stats --raw | jq .

# With authentication
fluxdb-cli --auth-token my-secret --cmd stats

# Connect to a remote server
fluxdb-cli -H 10.0.0.1 -p 7654

# Open a data directory directly (read-only, no server needed)
fluxdb-cli --data-dir ./fluxdb_data
fluxdb-cli --data-dir ./fluxdb_data --cmd 'find users {"age":{"$gte":25}}'

# Or use Redis protocol
redis-cli SET hello world
redis-cli GET hello
```

### Configuration

FluxDB loads config from three layers, each overriding the previous:

1. **Defaults** — sensible out-of-the-box values
2. **TOML file** — `fluxdb.toml` (current directory), `/etc/fluxdb/fluxdb.toml`, or `--config <path>`
3. **CLI flags** — override any file or default value

Generate a complete config file with all settings and comments:

```bash
cargo run --release -- --init-config fluxdb.toml
```

#### TOML reference

```toml
# Directory where database files (WAL, collections) are stored.
data_dir = "./fluxdb_data"

# Address and port for the JSON protocol server.
listen = "127.0.0.1:7654"

[server]
# Timeout for processing a single client request, in seconds (0 = disabled).
request_timeout_secs = 0
# Disconnect clients after this many seconds of inactivity (0 = disabled).
idle_timeout_secs = 0

[wal]
# Number of WAL entries to buffer before flushing to disk.
batch_size = 64
# Byte threshold for the WAL write buffer before flushing.
batch_bytes = 65536
# When to fsync ("every_flush" = safest default, "none" = fastest).
sync_mode = "every_flush"

[compaction]
# Enable automatic WAL compaction on a timer.
auto = false
# Interval between automatic compaction runs, in seconds (minimum 60).
interval_secs = 3600

[limits]
# Maximum concurrent client connections (0 = unlimited).
max_connections = 1024
# Maximum document size in bytes (default 16 MB).
max_document_bytes = 16777216
# Maximum number of documents returned by a single query (default 100K).
max_result_count = 100000

[auth]
# Enable token-based authentication.
enabled = false
# Shared secret token (required when auth is enabled).
token = ""

[log]
# Log queries that take longer than this many milliseconds (0 = disabled).
slow_query_ms = 0

[redis]
# Enable Redis-compatible protocol server.
enabled = false
# Address and port for the Redis protocol server.
listen = "127.0.0.1:6379"

[cluster]
# Enable distributed cluster mode.
enabled = false
# Unique identifier for this node.
node_id = "node-0"
# Address for peer-to-peer communication between nodes.
peer_listen = "127.0.0.1:7655"
# Seconds between health check pings to peer nodes.
health_check_interval_secs = 5
# Consecutive health check failures before marking a node unhealthy.
unhealthy_threshold = 3
# Timeout in seconds for establishing a connection to a peer.
connect_timeout_secs = 5
# Timeout in seconds for a request/response cycle with a peer.
request_timeout_secs = 10

# List all nodes in the cluster.
[[cluster.nodes]]
id = "node-0"
peer_addr = "127.0.0.1:7655"
client_addr = "127.0.0.1:7654"
```

#### CLI flags

| Flag | TOML equivalent | Description |
|------|-----------------|-------------|
| `--config <path>` | — | Path to TOML config file |
| `--data-dir <path>` | `data_dir` | Data directory |
| `--listen <addr>` | `listen` | Server listen address |
| `--wal-batch-size <n>` | `wal.batch_size` | WAL entries before flush |
| `--wal-batch-bytes <n>` | `wal.batch_bytes` | WAL bytes before flush |
| `--redis [addr]` | `redis.enabled` + `redis.listen` | Enable Redis protocol (optional address) |
| `--auth-token <token>` | `auth.token` | Enable auth with the given token |
| `--init-config <path>` | — | Write default config to file and exit |

### Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `server` | yes | TCP server, CLI, TOML config (requires tokio) |
| `persistence` | yes (via server) | WAL-based disk persistence |
| `redis` | no | Redis-compatible protocol server |
| `cluster` | no | Distributed cluster mode |
| `cli` | no | Interactive CLI client (`fluxdb-cli` binary) |

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
{"cmd": "find", "collection": "users", "filter": {"age": {"$gte": 25}}, "sort": {"age": -1}, "limit": 10, "skip": 0}
```

The `sort`, `limit`, and `skip` parameters are all optional. Sort values are `1` (ascending) or `-1` (descending). Multi-field sort is supported.

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
{"cmd": "auth", "token": "my-secret-token"}
{"cmd": "count", "collection": "users", "filter": {"age": {"$gte": 25}}}
{"cmd": "list_collections"}
{"cmd": "drop_collection", "name": "users"}
{"cmd": "compact"}
{"cmd": "flush"}
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

FluxDB supports horizontal scaling via consistent-hash sharding across multiple nodes. Any node in the cluster can accept any request — it will automatically route the operation to the correct owner.

### Starting a cluster

Build with the `cluster` feature and give each node its own config:

```bash
cargo run --release --features cluster -- --config node0.toml
cargo run --release --features cluster -- --config node1.toml
cargo run --release --features cluster -- --config node2.toml
```

Each node's TOML config lists every member of the cluster:

```toml
# node0.toml
[cluster]
enabled = true
node_id = "node-0"
peer_listen = "10.0.0.1:7655"

[[cluster.nodes]]
id = "node-0"
peer_addr = "10.0.0.1:7655"
client_addr = "10.0.0.1:7654"

[[cluster.nodes]]
id = "node-1"
peer_addr = "10.0.0.2:7655"
client_addr = "10.0.0.2:7654"

[[cluster.nodes]]
id = "node-2"
peer_addr = "10.0.0.3:7655"
client_addr = "10.0.0.3:7654"
```

Clients connect to any node's `client_addr`. The cluster handles routing transparently.

### How sharding works

FluxDB uses a **consistent hash ring** to distribute documents across nodes. The shard key is `hash(collection + "/" + doc_id)` using CRC32:

```
                    ┌──────────────┐
               ┌────┤   Hash Ring   ├────┐
               │    └──────────────┘    │
               ▼                        ▼
         ┌──────────┐            ┌──────────┐
         │  node-0  │            │  node-1  │
         │  owns     │            │  owns     │
         │  sector A │            │  sector B │
         └──────────┘            └──────────┘
               ▲                        ▲
               │    ┌──────────────┐    │
               └────┤   node-2    ├────┘
                    │   owns       │
                    │   sector C   │
                    └──────────────┘
```

Each physical node gets **128 virtual nodes** spread around the ring. When a request arrives:

1. The router computes `crc32("collection/doc_id")`
2. Walks clockwise around the ring to find the first virtual node at or after that position
3. Maps the virtual node back to its physical node — that's the owner

This guarantees even distribution (~33% per node in a 3-node cluster) and minimal disruption when scaling: adding a 4th node only moves ~25% of documents (the theoretical minimum of 1/N).

### Operation routing

Different operations are handled differently across the cluster:

| Operation | Strategy | Description |
|-----------|----------|-------------|
| `get`, `update`, `delete` | **Point route** | Hashed to the owning node by `(collection, id)` |
| `insert` | **Point route** | If no `_id` is provided, a UUID is generated first so routing is deterministic |
| `find` | **Scatter-gather** | Sent to all nodes in parallel; results are merged with skip/limit applied after merge |
| `count` | **Scatter-gather** | Sent to all nodes in parallel; counts are summed |
| `list_collections` | **Scatter-gather** | Union of collections from all nodes |
| `create_collection`, `drop_collection` | **Broadcast** | Executed on every node |
| `create_index`, `drop_index` | **Broadcast** | Executed on every node |
| `compact`, `stats`, `flush` | **Local only** | Runs on the node that received the request |

For scatter-gather queries with `limit` and `skip`, each node returns up to `limit + skip` results. The router then merges all results, applies `skip`, and truncates to `limit` — ensuring correct pagination without missing documents.

### Peer communication

Nodes communicate over persistent TCP connections using the same JSON line-delimited protocol as clients. Requests forwarded between nodes carry a `_routed: true` flag to prevent infinite forwarding loops. Connections auto-reconnect on failure with a 5-second connect timeout and 10-second request timeout.

### Health monitoring

A background health checker pings every peer every 5 seconds using `stats` commands. A node is marked **unhealthy** after 3 consecutive failures and automatically recovers when it responds again. Cluster health is visible via the `cluster_status` command:

```json
{"cmd": "cluster_status"}
```

### Scaling considerations

- **Static membership** — all nodes share the same config file listing every member. To add a node, update the config on all nodes and restart. The consistent hash ring ensures only ~1/N of documents need to move.
- **No replication** — each document lives on exactly one node. Node failure means that shard's data is unavailable until the node recovers (data is still on disk via the WAL).
- **No rebalancing** — when nodes are added, existing data stays in place. New writes are correctly routed; old data can be migrated by re-inserting.
- **Network partition** — during a partition, each side continues serving its local data. Scatter-gather queries return partial results from reachable nodes.

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
  bin/fluxdb_cli.rs — interactive CLI client with REPL and shorthand commands
  cluster/
    ring.rs        — consistent hash ring (128 vnodes, CRC32)
    router.rs      — command routing, scatter-gather, broadcast
    peer.rs        — TCP peer client with auto-reconnect
    health.rs      — periodic health checks, failure detection
```

### Design

- **Per-collection RwLock** — reads on different collections never block each other; only writes acquire exclusive access
- **BTreeMap storage** — documents stored in sorted order by ID for deterministic iteration; pagination with `skip`/`limit` always returns consistent results
- **B-tree indexes** — equality and range queries skip full collection scans; unified numeric keys (integer and float share the same index bucket); `$or` unions and `$and` intersections are index-aware with selectivity-first ordering
- **Zero-copy scans** — documents cache their serialized bytes; bulk scans avoid per-document serialization
- **Batched WAL** — writes are buffered (64 entries or 64 KB by default), reducing fsync overhead
- **Atomic compaction** — WAL compaction writes to a temp file and atomically renames, ensuring crash safety
- **CRC32 recovery** — on startup, corrupted WAL entries are skipped while preserving all valid entries before the corruption point
- **spawn_blocking** — database operations run on tokio's blocking pool, keeping the async runtime responsive
- **Graceful shutdown** — `Ctrl-C` triggers coordinated shutdown via broadcast channel; WAL is flushed before exit
- **Lock poison recovery** — all lock acquisitions return `Result` instead of panicking, preventing cascading failures
- **Health-aware routing** — cluster router checks node health before forwarding; scatter operations skip unhealthy nodes

## Testing

```bash
cargo test                    # 79 tests (core + server)
cargo test --features cluster # 89 tests (adds cluster integration)
```

Covers documents, queries, projections, sort, field-level `$not`, collections, indexes (including cross-type numeric and `$or`/`$and` index usage), WAL persistence, crash recovery, CRC corruption recovery, atomic compaction, deterministic pagination, concurrent access, input validation, document size limits, config validation, hash ring distribution, and cluster routing.
