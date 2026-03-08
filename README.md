# FluxDB

A document-oriented NoSQL database written in Rust.

FluxDB stores JSON documents in named collections, supports MongoDB-style queries, and persists data using a write-ahead log with CRC32 integrity checks.

## Features

- **Document storage** — JSON documents with auto-generated UUIDs or custom IDs
- **Collections** — logical grouping of documents (like tables)
- **Query engine** — MongoDB-compatible filter syntax with dot-notation field access
- **Persistence** — append-only WAL with CRC32 checksums and crash recovery
- **Projections** — include/exclude fields from query results
- **TCP server** — async multi-client server with line-delimited JSON protocol
- **Compaction** — compact the WAL to reclaim space from deleted/updated documents

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
  collection.rs  — in-memory document collection with CRUD operations
  query.rs       — query filter evaluation and projection
  wal.rs         — write-ahead log with CRC32 checksums
  database.rs    — database engine, WAL replay, compaction
  server.rs      — async TCP server (tokio)
```

## Testing

```bash
cargo test
```

37 unit tests covering documents, queries, collections, WAL persistence, crash recovery, and compaction.
