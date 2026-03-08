# FluxDB

A document-oriented NoSQL database written in Rust.

FluxDB stores JSON documents in named collections, supports MongoDB-style queries with secondary index acceleration, and persists data using a write-ahead log with CRC32 integrity checks.

## Features

- **Document storage** — JSON documents with auto-generated UUIDs or custom IDs
- **Collections** — logical grouping of documents (like tables)
- **Query engine** — MongoDB-compatible filter syntax with dot-notation field access
- **Secondary indexes** — B-tree indexes on arbitrary fields for fast equality and range queries
- **Persistence** — append-only WAL with binary format, CRC32 checksums, and crash recovery
- **Batched writes** — WAL entries are buffered and flushed in batches for throughput
- **Concurrent reads** — per-collection RwLocks allow parallel read access
- **Projections** — include/exclude fields from query results
- **TCP server** — async multi-client server with line-delimited JSON protocol
- **Compaction** — compact the WAL to reclaim space from deleted/updated documents
- **Backward compatible** — automatically reads legacy JSON WAL files

## Quick start

```bash
cargo build --release
./target/release/fluxdb [data_dir] [address]
```

Defaults to `./fluxdb_data` and `127.0.0.1:7654`.

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

Indexes accelerate equality (`$eq`), range (`$gt`, `$gte`, `$lt`, `$lte`), and membership (`$in`) queries on the indexed field. Queries that can use an index will automatically do so.

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

Implicit equality is supported — `{"name": "Alice"}` is equivalent to `{"name": {"$eq": "Alice"}}`.

Dot notation works for nested fields: `{"address.city": "Portland"}`.

## Example session

```bash
# Start the server
cargo run

# In another terminal
echo '{"cmd":"create_collection","name":"users"}' | nc localhost 7654
echo '{"cmd":"insert","collection":"users","document":{"name":"Alice","age":30}}' | nc localhost 7654
echo '{"cmd":"create_index","collection":"users","field":"age"}' | nc localhost 7654
echo '{"cmd":"find","collection":"users","filter":{"age":{"$gte":25}}}' | nc localhost 7654
echo '{"cmd":"stats"}' | nc localhost 7654
```

## Architecture

```
src/
  main.rs        — entry point, CLI argument parsing
  lib.rs         — module declarations
  error.rs       — error types
  document.rs    — JSON document model with dot-notation field access
  collection.rs  — document collection with CRUD, index-accelerated queries
  index.rs       — B-tree secondary indexes with range/equality lookups
  query.rs       — query filter evaluation and projection
  wal.rs         — binary write-ahead log with CRC32 checksums, batched writes
  database.rs    — database engine with per-collection RwLocks, WAL replay
  server.rs      — async TCP server (tokio) with spawn_blocking for DB ops
```

### Performance design

- **Per-collection RwLock**: reads on different collections never block each other; reads on the same collection run concurrently; only writes acquire exclusive access to the target collection
- **B-tree indexes**: equality and range queries on indexed fields skip full collection scans
- **Batched WAL**: writes are buffered and flushed to disk in batches (64 entries or 64KB), reducing fsync overhead
- **Binary WAL format**: length-prefixed binary entries with bincode serialization, faster to write and parse than JSON
- **spawn_blocking**: database operations run on tokio's blocking thread pool, keeping the async runtime responsive

## Testing

```bash
cargo test
```

56 unit tests covering documents, queries, collections, indexes, WAL persistence, crash recovery, compaction, and concurrent access.
