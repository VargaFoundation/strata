//! Criterion benchmarks for the Ecphoria core engine.
//!
//! Run locally: cargo bench -p ecphoria-core
//! CI runs these on every PR and posts a comparison comment.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use ecphoria_core::{CoreConfig, EcphoriaEngine};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn make_engine(rt: &tokio::runtime::Runtime) -> EcphoriaEngine {
    rt.block_on(EcphoriaEngine::new(CoreConfig::default()))
        .unwrap()
}

fn bench_ingest(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Fresh events (new ids) per iteration via iter_batched — otherwise re-ingesting the same ids
    // would measure the INSERT-OR-IGNORE duplicate-PK skip path, not real inserts.
    let make_batch = || -> Vec<ecphoria_core::memory::episodic::Event> {
        (0..100)
            .map(|i| ecphoria_core::memory::episodic::Event {
                id: uuid::Uuid::new_v4(),
                source: "bench".into(),
                event_type: "test".into(),
                payload: serde_json::json!({"i": i, "data": "benchmark payload"}),
                timestamp: chrono::Utc::now(),
                parent_id: None,
                trace_id: None,
                tags: vec!["bench".into()],
                idempotency_key: None,
            })
            .collect()
    };

    c.bench_function("ingest_100_events", |b| {
        b.iter_batched(
            make_batch,
            |events| {
                rt.block_on(engine.ingest(black_box(events))).unwrap();
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_query(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Seed data
    let events: Vec<ecphoria_core::memory::episodic::Event> = (0..500)
        .map(|i| ecphoria_core::memory::episodic::Event {
            id: uuid::Uuid::new_v4(),
            source: "bench".into(),
            event_type: "test".into(),
            payload: serde_json::json!({"i": i}),
            timestamp: chrono::Utc::now(),
            parent_id: None,
            trace_id: None,
            tags: vec![],
            idempotency_key: None,
        })
        .collect();
    rt.block_on(engine.ingest(events)).unwrap();

    c.bench_function("query_select_100", |b| {
        b.iter(|| {
            rt.block_on(engine.query_sql(black_box(
                "SELECT * FROM episodic ORDER BY ts DESC LIMIT 100",
            )))
            .unwrap();
        });
    });
}

fn bench_memory_add(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);
    let scope = ecphoria_core::memory::cognition::MemoryScope::user("bench");
    let mut n = 0u64;

    // The flagship cognition path: dedup / contradiction / importance on each add.
    c.bench_function("memory_add", |b| {
        b.iter(|| {
            n += 1;
            let input = ecphoria_core::memory::cognition::MemoryInput::new(
                scope.clone(),
                format!("fact number {n}"),
            );
            rt.block_on(engine.memory_add(black_box(input))).unwrap();
        });
    });
}

fn bench_memory_search(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);
    let scope = ecphoria_core::memory::cognition::MemoryScope::user("bench");

    // Seed a realistic corpus of memories.
    rt.block_on(async {
        for i in 0..500 {
            let input = ecphoria_core::memory::cognition::MemoryInput::new(
                scope.clone(),
                format!(
                    "user preference {i}: likes topic {} and tool {}",
                    i % 17,
                    i % 7
                ),
            );
            engine.memory_add(input).await.unwrap();
        }
    });

    // Hybrid BM25 + recency/importance re-ranking (no embedding provider → lexical path).
    c.bench_function("memory_search_hybrid_k5", |b| {
        b.iter(|| {
            rt.block_on(engine.memory_search(black_box("likes topic 3"), &scope, 5))
                .unwrap();
        });
    });
}

fn bench_graph_neighbors(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Seed a graph: a hub entity linked to many others.
    rt.block_on(async {
        for i in 0..1000 {
            engine
                .memory_link("default", "hub", "rel", &format!("node-{i}"), None)
                .await
                .unwrap();
        }
    });

    c.bench_function("graph_neighbors_hub", |b| {
        b.iter(|| {
            rt.block_on(engine.memory_neighbors(black_box("default"), "hub", 50))
                .unwrap();
        });
    });
}

fn bench_state(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    c.bench_function("state_set_get", |b| {
        b.iter(|| {
            rt.block_on(async {
                engine
                    .state_set("agent-1", "key-1", serde_json::json!({"v": 1}))
                    .await
                    .unwrap();
                engine.state_get("agent-1", "key-1").await.unwrap();
            });
        });
    });
}

fn bench_semantic_search(c: &mut Criterion) {
    let rt = runtime();
    let engine = make_engine(&rt);

    // Seed vectors
    rt.block_on(async {
        for i in 0..200 {
            let vec = vec![i as f32 / 200.0; 768];
            let entry = ecphoria_core::memory::semantic::SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: format!("entry {i}"),
                embedding: vec,
                metadata: serde_json::json!({}),
            };
            engine.semantic_upsert(&entry).await.unwrap();
        }
    });

    let query_vec = vec![0.5_f32; 768];

    c.bench_function("semantic_search_k10", |b| {
        b.iter(|| {
            rt.block_on(engine.semantic_search(black_box(&query_vec), 10))
                .unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_ingest,
    bench_query,
    bench_state,
    bench_semantic_search,
    bench_memory_add,
    bench_memory_search,
    bench_graph_neighbors
);
criterion_main!(benches);
