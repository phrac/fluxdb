use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion, BatchSize, BenchmarkId};
use serde_json::json;
use tempfile::tempdir;

use fluxdb::database::Database;

/// Insert documents sequentially (persisted with disk WAL).
fn bench_insert_persisted(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db = Database::open(dir.path()).unwrap();
    db.create_collection("bench").unwrap();

    let mut i = 0u64;
    c.bench_function("insert_persisted", |b| {
        b.iter(|| {
            i += 1;
            black_box(
                db.insert(
                    "bench",
                    json!({"_id": format!("d{i}"), "name": "test", "value": i, "tags": ["a", "b"]}),
                )
                .unwrap(),
            );
        })
    });
}

/// Insert documents sequentially (in-memory, no WAL overhead).
fn bench_insert_memory(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();

    let mut i = 0u64;
    c.bench_function("insert_memory", |b| {
        b.iter(|| {
            i += 1;
            black_box(
                db.insert(
                    "bench",
                    json!({"_id": format!("d{i}"), "name": "test", "value": i, "tags": ["a", "b"]}),
                )
                .unwrap(),
            );
        })
    });
}

/// Insert documents into a collection with a secondary index.
fn bench_insert_indexed(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();
    db.create_index("bench", "value").unwrap();

    let mut i = 0u64;
    c.bench_function("insert_indexed", |b| {
        b.iter(|| {
            i += 1;
            black_box(
                db.insert(
                    "bench",
                    json!({"_id": format!("d{i}"), "value": i, "data": "payload"}),
                )
                .unwrap(),
            );
        })
    });
}

/// Point read by document ID from a 10K-document collection.
fn bench_get_by_id(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();

    for i in 0..10_000 {
        db.insert("bench", json!({"_id": format!("d{i}"), "n": i, "name": "user"}))
            .unwrap();
    }

    let mut i = 0u64;
    c.bench_function("get_by_id/10k", |b| {
        b.iter(|| {
            i = (i + 1) % 10_000;
            black_box(db.get("bench", &format!("d{i}")).unwrap());
        })
    });
}

/// Find with equality filter — full collection scan (no index).
fn bench_find_scan(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();

    for i in 0..10_000 {
        db.insert(
            "bench",
            json!({"_id": format!("d{i}"), "age": i % 100, "name": format!("user{i}")}),
        )
        .unwrap();
    }

    c.bench_function("find_scan/10k", |b| {
        b.iter(|| {
            black_box(
                db.find("bench", json!({"age": 42}), None, None, None, None)
                    .unwrap(),
            );
        })
    });
}

/// Find with equality filter — accelerated by secondary index.
fn bench_find_indexed(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();
    db.create_index("bench", "age").unwrap();

    for i in 0..10_000 {
        db.insert(
            "bench",
            json!({"_id": format!("d{i}"), "age": i % 100, "name": format!("user{i}")}),
        )
        .unwrap();
    }

    c.bench_function("find_indexed/10k", |b| {
        b.iter(|| {
            black_box(
                db.find("bench", json!({"age": 42}), None, None, None, None)
                    .unwrap(),
            );
        })
    });
}

/// Range query with index on 10K documents.
fn bench_find_range_indexed(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();
    db.create_index("bench", "score").unwrap();

    for i in 0..10_000 {
        db.insert(
            "bench",
            json!({"_id": format!("d{i}"), "score": i, "data": "x"}),
        )
        .unwrap();
    }

    c.bench_function("find_range_indexed/10k", |b| {
        b.iter(|| {
            black_box(
                db.find(
                    "bench",
                    json!({"score": {"$gte": 4000, "$lt": 6000}}),
                    None,
                    None,
                    None,
                    None,
                )
                .unwrap(),
            );
        })
    });
}

/// Count with filter — full scan.
fn bench_count_scan(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();

    for i in 0..10_000 {
        db.insert(
            "bench",
            json!({"_id": format!("d{i}"), "status": if i % 3 == 0 { "active" } else { "inactive" }}),
        )
        .unwrap();
    }

    c.bench_function("count_scan/10k", |b| {
        b.iter(|| {
            black_box(db.count("bench", json!({"status": "active"})).unwrap());
        })
    });
}

/// Update existing documents.
fn bench_update(c: &mut Criterion) {
    let db = Database::open_memory();
    db.create_collection("bench").unwrap();

    for i in 0..10_000 {
        db.insert("bench", json!({"_id": format!("d{i}"), "val": i}))
            .unwrap();
    }

    let mut i = 0u64;
    c.bench_function("update/10k", |b| {
        b.iter(|| {
            i = (i + 1) % 10_000;
            black_box(
                db.update("bench", &format!("d{i}"), json!({"val": i + 10_000}))
                    .unwrap(),
            );
        })
    });
}

/// Delete documents.
fn bench_delete(c: &mut Criterion) {
    c.bench_function("delete/batch", |b| {
        b.iter_batched(
            || {
                let db = Database::open_memory();
                db.create_collection("bench").unwrap();
                for i in 0..1_000 {
                    db.insert("bench", json!({"_id": format!("d{i}"), "n": i}))
                        .unwrap();
                }
                db
            },
            |db| {
                for i in 0..1_000 {
                    black_box(db.delete("bench", &format!("d{i}")).unwrap());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

/// Concurrent reads from N threads (50K ops per thread, 10K-document collection).
///
/// Each thread does the SAME number of reads. If the per-collection RwLock
/// scales, wall time stays roughly constant as threads increase, meaning total
/// throughput scales linearly.
fn bench_concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_reads");
    group.sample_size(10);

    let db = Arc::new(Database::open_memory());
    db.create_collection("bench").unwrap();
    for i in 0..10_000 {
        db.insert("bench", json!({"_id": format!("d{i}"), "n": i}))
            .unwrap();
    }

    for threads in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{threads}t")),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads)
                        .map(|t| {
                            let db = Arc::clone(&db);
                            std::thread::spawn(move || {
                                for i in 0..50_000u64 {
                                    let id = format!("d{}", (i + t as u64 * 1000) % 10_000);
                                    black_box(db.get("bench", &id).unwrap());
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

/// Concurrent inserts from N threads — each thread writes to its OWN collection
/// (in-memory, no WAL contention). Shows per-collection lock scaling.
fn bench_concurrent_insert_memory(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_insert_mem");
    group.sample_size(10);

    for threads in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{threads}t")),
            &threads,
            |b, &threads| {
                b.iter_batched(
                    || {
                        let db = Arc::new(Database::open_memory());
                        for t in 0..threads {
                            db.create_collection(&format!("col{t}")).unwrap();
                        }
                        db
                    },
                    |db| {
                        let handles: Vec<_> = (0..threads)
                            .map(|t| {
                                let db = Arc::clone(&db);
                                let col = format!("col{t}");
                                std::thread::spawn(move || {
                                    for i in 0..10_000 {
                                        black_box(
                                            db.insert(
                                                &col,
                                                json!({"t": t, "i": i, "data": "payload"}),
                                            )
                                            .unwrap(),
                                        );
                                    }
                                })
                            })
                            .collect();
                        for h in handles {
                            h.join().unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_persisted,
    bench_insert_memory,
    bench_insert_indexed,
    bench_get_by_id,
    bench_find_scan,
    bench_find_indexed,
    bench_find_range_indexed,
    bench_count_scan,
    bench_update,
    bench_delete,
    bench_concurrent_reads,
    bench_concurrent_insert_memory,
);
criterion_main!(benches);
