//! Comparison benchmarks: FluxDB vs SQLite vs RocksDB vs redb vs MongoDB
//!
//! All engines store the same JSON documents serialized as bytes,
//! keyed by a string ID. This measures raw storage engine throughput
//! on equivalent workloads.
//!
//! MongoDB benchmarks require a running `mongod` on localhost:27017.
//! They are skipped automatically if the server is unreachable.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::json;
use tempfile::tempdir;

const DOC_COUNT: usize = 10_000;

fn make_doc(i: usize) -> String {
    serde_json::to_string(&json!({
        "name": format!("user{i}"),
        "age": (i % 80) + 18,
        "email": format!("user{i}@example.com"),
        "active": i % 3 != 0,
        "tags": ["rust", "database"]
    }))
    .unwrap()
}

fn key(i: usize) -> String {
    format!("doc-{i:06}")
}

// ── FluxDB ──────────────────────────────────────────────────────────────────

mod fluxdb_bench {
    use super::*;
    use fluxdb::database::Database;

    pub fn insert(c: &mut Criterion) {
        let db = Database::open_memory();
        db.create_collection("bench").unwrap();

        let mut i = 0usize;
        c.bench_function("fluxdb/insert", |b| {
            b.iter(|| {
                let doc_json: serde_json::Value =
                    serde_json::from_str(&make_doc(i)).unwrap();
                let mut obj = serde_json::Map::new();
                obj.insert("_id".to_string(), serde_json::Value::String(key(i)));
                if let serde_json::Value::Object(m) = doc_json {
                    obj.extend(m);
                }
                black_box(
                    db.insert("bench", serde_json::Value::Object(obj))
                        .unwrap(),
                );
                i += 1;
            })
        });
    }

    pub fn get(c: &mut Criterion) {
        let db = Database::open_memory();
        db.create_collection("bench").unwrap();
        for i in 0..DOC_COUNT {
            let doc_json: serde_json::Value = serde_json::from_str(&make_doc(i)).unwrap();
            let mut obj = serde_json::Map::new();
            obj.insert("_id".to_string(), serde_json::Value::String(key(i)));
            if let serde_json::Value::Object(m) = doc_json {
                obj.extend(m);
            }
            db.insert("bench", serde_json::Value::Object(obj)).unwrap();
        }

        let mut i = 0usize;
        c.bench_function("fluxdb/get", |b| {
            b.iter(|| {
                i = (i + 1) % DOC_COUNT;
                black_box(db.get("bench", &key(i)).unwrap());
            })
        });
    }

    pub fn scan(c: &mut Criterion) {
        let db = Database::open_memory();
        db.create_collection("bench").unwrap();
        for i in 0..DOC_COUNT {
            let doc_json: serde_json::Value = serde_json::from_str(&make_doc(i)).unwrap();
            let mut obj = serde_json::Map::new();
            obj.insert("_id".to_string(), serde_json::Value::String(key(i)));
            if let serde_json::Value::Object(m) = doc_json {
                obj.extend(m);
            }
            db.insert("bench", serde_json::Value::Object(obj)).unwrap();
        }

        // Zero-copy scan: iterate pre-serialized bytes (same as redb/rocksdb benchmarks)
        c.bench_function("fluxdb/scan_all", |b| {
            b.iter(|| {
                let mut count = 0;
                db.scan("bench", |id, bytes| {
                    black_box((id, bytes));
                    count += 1;
                })
                .unwrap();
                black_box(count);
            })
        });
    }
}

// ── SQLite ──────────────────────────────────────────────────────────────────

mod sqlite_bench {
    use super::*;
    use rusqlite::{params, Connection};

    fn setup_db(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path.join("bench.db")).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS docs (id TEXT PRIMARY KEY, data TEXT NOT NULL);",
        )
        .unwrap();
        conn
    }

    pub fn insert(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let conn = setup_db(dir.path());

        let mut i = 0usize;
        c.bench_function("sqlite/insert", |b| {
            b.iter(|| {
                conn.execute(
                    "INSERT OR REPLACE INTO docs (id, data) VALUES (?1, ?2)",
                    params![key(i), make_doc(i)],
                )
                .unwrap();
                i += 1;
            })
        });
    }

    pub fn get(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let conn = setup_db(dir.path());

        for i in 0..DOC_COUNT {
            conn.execute(
                "INSERT INTO docs (id, data) VALUES (?1, ?2)",
                params![key(i), make_doc(i)],
            )
            .unwrap();
        }

        let mut i = 0usize;
        c.bench_function("sqlite/get", |b| {
            b.iter(|| {
                i = (i + 1) % DOC_COUNT;
                let mut stmt = conn
                    .prepare_cached("SELECT data FROM docs WHERE id = ?1")
                    .unwrap();
                let val: String = stmt.query_row([key(i)], |row| row.get(0)).unwrap();
                black_box(val);
            })
        });
    }

    pub fn scan(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let conn = setup_db(dir.path());

        for i in 0..DOC_COUNT {
            conn.execute(
                "INSERT INTO docs (id, data) VALUES (?1, ?2)",
                params![key(i), make_doc(i)],
            )
            .unwrap();
        }

        c.bench_function("sqlite/scan_all", |b| {
            b.iter(|| {
                let mut stmt = conn
                    .prepare_cached("SELECT id, data FROM docs")
                    .unwrap();
                let rows: Vec<(String, String)> = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .unwrap()
                    .filter_map(|r| r.ok())
                    .collect();
                black_box(rows);
            })
        });
    }
}

// ── RocksDB ─────────────────────────────────────────────────────────────────

mod rocksdb_bench {
    use super::*;
    use rocksdb::DB;

    pub fn insert(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = DB::open_default(dir.path().join("rocks")).unwrap();

        let mut i = 0usize;
        c.bench_function("rocksdb/insert", |b| {
            b.iter(|| {
                db.put(key(i).as_bytes(), make_doc(i).as_bytes()).unwrap();
                i += 1;
            })
        });
    }

    pub fn get(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = DB::open_default(dir.path().join("rocks")).unwrap();

        for i in 0..DOC_COUNT {
            db.put(key(i).as_bytes(), make_doc(i).as_bytes()).unwrap();
        }

        let mut i = 0usize;
        c.bench_function("rocksdb/get", |b| {
            b.iter(|| {
                i = (i + 1) % DOC_COUNT;
                black_box(db.get(key(i).as_bytes()).unwrap());
            })
        });
    }

    pub fn scan(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = DB::open_default(dir.path().join("rocks")).unwrap();

        for i in 0..DOC_COUNT {
            db.put(key(i).as_bytes(), make_doc(i).as_bytes()).unwrap();
        }

        c.bench_function("rocksdb/scan_all", |b| {
            b.iter(|| {
                let mut count = 0;
                let iter = db.iterator(rocksdb::IteratorMode::Start);
                for item in iter {
                    let (k, v) = item.unwrap();
                    black_box((&k, &v));
                    count += 1;
                }
                black_box(count);
            })
        });
    }
}

// ── redb ────────────────────────────────────────────────────────────────────

mod redb_bench {
    use super::*;
    use redb::{Database, ReadableTable, TableDefinition};

    const TABLE: TableDefinition<&str, &str> = TableDefinition::new("docs");

    pub fn insert(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("redb")).unwrap();

        let mut i = 0usize;
        c.bench_function("redb/insert", |b| {
            b.iter(|| {
                let k = key(i);
                let v = make_doc(i);
                let write_txn = db.begin_write().unwrap();
                {
                    let mut table = write_txn.open_table(TABLE).unwrap();
                    table.insert(k.as_str(), v.as_str()).unwrap();
                }
                write_txn.commit().unwrap();
                i += 1;
            })
        });
    }

    pub fn get(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("redb")).unwrap();

        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(TABLE).unwrap();
                for i in 0..DOC_COUNT {
                    let k = key(i);
                    let v = make_doc(i);
                    table.insert(k.as_str(), v.as_str()).unwrap();
                }
            }
            write_txn.commit().unwrap();
        }

        let mut i = 0usize;
        c.bench_function("redb/get", |b| {
            b.iter(|| {
                i = (i + 1) % DOC_COUNT;
                let k = key(i);
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                let val = table.get(k.as_str()).unwrap().unwrap();
                black_box(val.value());
            })
        });
    }

    pub fn scan(c: &mut Criterion) {
        let dir = tempdir().unwrap();
        let db = Database::create(dir.path().join("redb")).unwrap();

        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(TABLE).unwrap();
                for i in 0..DOC_COUNT {
                    let k = key(i);
                    let v = make_doc(i);
                    table.insert(k.as_str(), v.as_str()).unwrap();
                }
            }
            write_txn.commit().unwrap();
        }

        c.bench_function("redb/scan_all", |b| {
            b.iter(|| {
                let read_txn = db.begin_read().unwrap();
                let table = read_txn.open_table(TABLE).unwrap();
                let mut count = 0;
                for entry in table.iter().unwrap() {
                    let (k, v) = entry.unwrap();
                    black_box((k.value(), v.value()));
                    count += 1;
                }
                black_box(count);
            })
        });
    }
}

// ── MongoDB ─────────────────────────────────────────────────────────────────

mod mongodb_bench {
    use super::*;
    use mongodb::bson::{doc, Document};
    use mongodb::sync::Client;

    fn connect() -> Option<mongodb::sync::Collection<Document>> {
        let client = Client::with_uri_str("mongodb://127.0.0.1:27017").ok()?;
        // Quick connectivity check
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .run()
            .ok()?;
        let db = client.database("fluxdb_bench");
        let _ = db.collection::<Document>("bench").drop().run();
        Some(db.collection("bench"))
    }

    pub fn insert(c: &mut Criterion) {
        let col = match connect() {
            Some(c) => c,
            None => {
                eprintln!("MongoDB not available, skipping mongodb/insert");
                return;
            }
        };

        let mut i = 0usize;
        c.bench_function("mongodb/insert", |b| {
            b.iter(|| {
                let doc = doc! {
                    "_id": key(i),
                    "name": format!("user{i}"),
                    "age": ((i % 80) + 18) as i32,
                    "email": format!("user{i}@example.com"),
                    "active": i % 3 != 0,
                    "tags": ["rust", "database"],
                };
                black_box(col.insert_one(doc).run().unwrap());
                i += 1;
            })
        });
    }

    pub fn get(c: &mut Criterion) {
        let col = match connect() {
            Some(c) => c,
            None => {
                eprintln!("MongoDB not available, skipping mongodb/get");
                return;
            }
        };

        // Populate
        let docs: Vec<Document> = (0..DOC_COUNT)
            .map(|i| {
                doc! {
                    "_id": key(i),
                    "name": format!("user{i}"),
                    "age": ((i % 80) + 18) as i32,
                    "email": format!("user{i}@example.com"),
                    "active": i % 3 != 0,
                    "tags": ["rust", "database"],
                }
            })
            .collect();
        col.insert_many(docs).run().unwrap();

        let mut i = 0usize;
        c.bench_function("mongodb/get", |b| {
            b.iter(|| {
                i = (i + 1) % DOC_COUNT;
                let filter = doc! { "_id": key(i) };
                black_box(col.find_one(filter).run().unwrap());
            })
        });
    }

    pub fn scan(c: &mut Criterion) {
        let col = match connect() {
            Some(c) => c,
            None => {
                eprintln!("MongoDB not available, skipping mongodb/scan_all");
                return;
            }
        };

        // Populate
        let docs: Vec<Document> = (0..DOC_COUNT)
            .map(|i| {
                doc! {
                    "_id": key(i),
                    "name": format!("user{i}"),
                    "age": ((i % 80) + 18) as i32,
                    "email": format!("user{i}@example.com"),
                    "active": i % 3 != 0,
                    "tags": ["rust", "database"],
                }
            })
            .collect();
        col.insert_many(docs).run().unwrap();

        c.bench_function("mongodb/scan_all", |b| {
            b.iter(|| {
                let mut count = 0;
                let cursor = col.find(doc! {}).run().unwrap();
                for result in cursor {
                    let d: Document = result.unwrap();
                    black_box(&d);
                    count += 1;
                }
                black_box(count);
            })
        });
    }
}

// ── Benchmark groups ────────────────────────────────────────────────────────

criterion_group!(
    comparison,
    fluxdb_bench::insert,
    fluxdb_bench::get,
    fluxdb_bench::scan,
    sqlite_bench::insert,
    sqlite_bench::get,
    sqlite_bench::scan,
    rocksdb_bench::insert,
    rocksdb_bench::get,
    rocksdb_bench::scan,
    redb_bench::insert,
    redb_bench::get,
    redb_bench::scan,
    mongodb_bench::insert,
    mongodb_bench::get,
    mongodb_bench::scan,
);
criterion_main!(comparison);
