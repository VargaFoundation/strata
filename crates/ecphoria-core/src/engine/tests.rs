
use super::*;

#[tokio::test]
async fn engine_lifecycle() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn engine_ingest_and_count() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();

    let events = vec![Event {
        id: uuid::Uuid::new_v4(),
        source: "test".into(),
        event_type: "click".into(),
        payload: serde_json::json!({"page": "/home"}),
        timestamp: chrono::Utc::now(),
        parent_id: None,
        trace_id: None,
        tags: vec![],
        idempotency_key: None,
    }];

    let count = engine.ingest(events).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(engine.event_count().await.unwrap(), 1);
}

#[tokio::test]
async fn engine_state_crud() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();

    let v = engine
        .state_set("bot", "mood", serde_json::json!("happy"))
        .await
        .unwrap();
    assert_eq!(v, 1);

    let entry = engine.state_get("bot", "mood").await.unwrap().unwrap();
    assert_eq!(entry.value, serde_json::json!("happy"));

    engine.state_delete("bot", "mood").await.unwrap();
    assert!(engine.state_get("bot", "mood").await.unwrap().is_none());
}

#[tokio::test]
async fn engine_query_sql() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let rows = engine
        .query_sql("SELECT 42::VARCHAR as answer")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["answer"], "42");
}

#[tokio::test]
async fn engine_semantic_search() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();

    // Use distinct vectors so cosine similarity clearly differentiates them
    let mut rust_vec = vec![0.0f32; 768];
    rust_vec[0] = 1.0; // points strongly in dimension 0

    let mut python_vec = vec![0.0f32; 768];
    python_vec[1] = 1.0; // points strongly in dimension 1

    let entry1 = SemanticEntry {
        id: uuid::Uuid::new_v4(),
        content: "Rust programming language".into(),
        embedding: rust_vec.clone(),
        metadata: serde_json::json!({}),
    };
    engine.semantic_upsert(&entry1).await.unwrap();

    let entry2 = SemanticEntry {
        id: uuid::Uuid::new_v4(),
        content: "Python scripting".into(),
        embedding: python_vec,
        metadata: serde_json::json!({}),
    };
    engine.semantic_upsert(&entry2).await.unwrap();

    assert_eq!(engine.semantic_count(), 2);

    // Search for vector close to "Rust"
    let results = engine.semantic_search(&rust_vec, 1).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry.content, "Rust programming language");
}

#[tokio::test]
async fn semantic_index_persists_across_reopen() {
    // Regression: the event semantic index is loaded on startup but was only saved by a
    // never-called shutdown(). `persist()` must write it so a file-backed reopen recovers it.
    let tmp = tempfile::TempDir::new().unwrap();
    let p = |f: &str| tmp.path().join(f).to_string_lossy().to_string();
    // Fully file-backed: the index reload is (correctly) gated on episodic being file-backed too.
    let cfg = || {
        let mut c = CoreConfig::default();
        c.memory.episodic.db_path = p("episodic.duckdb");
        c.memory.state.db_path = p("state.db");
        c.memory.cognition.db_path = p("mem.duckdb");
        c.runtime.db_path = p("runtime.db");
        c.memory.semantic.index_dir = p("vectors");
        c.embedding.dimension = 4;
        c
    };
    let vec = vec![1.0_f32, 0.0, 0.0, 0.0];

    {
        let engine = EcphoriaEngine::new(cfg()).await.unwrap();
        engine
            .semantic_upsert(&SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: "persisted event".into(),
                embedding: vec.clone(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        engine.persist().await.unwrap(); // <- the fix under test
    }
    // Reopen: engine::new loads the saved index → the vector is still searchable.
    {
        let engine = EcphoriaEngine::new(cfg()).await.unwrap();
        assert_eq!(engine.semantic_count(), 1, "index not recovered from disk");
        let hits = engine.semantic_search(&vec, 1).await.unwrap();
        assert_eq!(hits[0].entry.content, "persisted event");
    }
}

/// Deterministic in-process embedding provider for cognition tests. Every text embeds to the
/// same unit vector, so any two memories in a scope are exact near-duplicates (cosine = 1.0) —
/// which deterministically drives the semantic dedup/merge path without a network backend.
struct ConstEmbedding {
    dim: usize,
}

#[async_trait::async_trait]
impl EmbeddingProvider for ConstEmbedding {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let mut v = vec![0.0_f32; self.dim];
        v[0] = 1.0;
        Ok(texts.iter().map(|_| v.clone()).collect())
    }
    fn dimension(&self) -> usize {
        self.dim
    }
    fn model_name(&self) -> &str {
        "const-test"
    }
}

/// Fully in-memory config so cognition tests don't touch `./data`.
fn inmem_config() -> CoreConfig {
    let mut c = CoreConfig::default();
    c.memory.episodic.db_path = ":memory:".into();
    c.memory.state.db_path = ":memory:".into();
    c.memory.cognition.db_path = ":memory:".into();
    c.runtime.db_path = ":memory:".into();
    c
}

#[tokio::test]
async fn delete_tenant_erases_all_stores() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let ta = crate::config::TenantContext::new("tenant-a");

    // tenant-a data across stores.
    engine
        .ingest_for_tenant(vec![Event::new("s", "e", serde_json::json!({"x": 1}))], &ta)
        .await
        .unwrap();
    engine
        .memory_add(MemoryInput::new(
            MemoryScope::tenant("tenant-a"),
            "likes tea",
        ))
        .await
        .unwrap();
    engine
        .state_set_for_tenant("tenant-a", "bot", "mood", serde_json::json!("happy"))
        .await
        .unwrap();
    // tenant-b control data that must survive.
    engine
        .memory_add(MemoryInput::new(MemoryScope::tenant("tenant-b"), "b-fact"))
        .await
        .unwrap();

    let summary = engine.delete_tenant("tenant-a").await.unwrap();
    assert_eq!(summary["events_deleted"], 1);
    assert_eq!(summary["memories_deleted"], 1);
    assert_eq!(summary["state_deleted"], 1);

    // tenant-a is gone…
    let a_events = engine
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-a")
        .await
        .unwrap();
    assert_eq!(a_events[0]["c"], "0");
    assert_eq!(
        engine
            .memory_all(&MemoryScope::tenant("tenant-a"), 100)
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(engine
        .state_get_for_tenant("tenant-a", "bot", "mood")
        .await
        .unwrap()
        .is_none());
    // …but tenant-b survives.
    assert_eq!(
        engine
            .memory_all(&MemoryScope::tenant("tenant-b"), 100)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn memory_reembed_indexes_vectorless_memories() {
    let mut cfg = inmem_config();
    cfg.embedding.dimension = 8;
    let mut engine = EcphoriaEngine::new(cfg).await.unwrap();
    let scope = MemoryScope::user("alice");

    // Store a memory with NO embedding (as if ingested while the provider was down / before a
    // model was configured).
    let m = Memory::new(scope.clone(), "alice prefers window seats");
    let id = m.id;
    engine
        .memory_apply_rows(vec![MemoryRow {
            memory: m,
            embedding: None,
        }])
        .await
        .unwrap();
    assert!(engine
        .memory_store
        .get_embedding(id)
        .await
        .unwrap()
        .is_none());

    // No provider yet → reembed is a no-op.
    assert_eq!(engine.memory_reembed(100).await.unwrap(), 0);

    // Configure a provider and re-embed: the memory now carries a vector.
    engine.set_embedding_for_test(Arc::new(ConstEmbedding { dim: 8 }));
    assert_eq!(engine.memory_reembed(100).await.unwrap(), 1);
    assert!(engine
        .memory_store
        .get_embedding(id)
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn sql_over_memories_visible_scoped_and_readonly() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .memory_add(MemoryInput::new(
            MemoryScope::tenant("acme"),
            "acme likes rust",
        ))
        .await
        .unwrap();
    engine
        .memory_add(MemoryInput::new(
            MemoryScope::tenant("globex"),
            "globex likes go",
        ))
        .await
        .unwrap();

    // The `memories` table is now reachable from SQL (bi-temporal columns included).
    let rows = engine
            .query_sql(
                "SELECT content, valid_from, valid_to FROM memories WHERE valid_to IS NULL ORDER BY content",
            )
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["content"], "acme likes rust");
    // valid_from must serialize as a real timestamp string (not null) — the bi-temporal story.
    assert!(
        rows[0]["valid_from"]
            .as_str()
            .is_some_and(|s| s.contains('T')),
        "valid_from did not serialize: {:?}",
        rows[0]["valid_from"]
    );
    assert!(rows[0]["valid_to"].is_null());

    // Tenant-scoped SQL only sees its own rows — the other tenant's content must not leak.
    let a = engine
        .query_sql_for_tenant("SELECT content FROM memories", "acme")
        .await
        .unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0]["content"], "acme likes rust");
    let g = engine
        .query_sql_for_tenant("SELECT content FROM memories", "globex")
        .await
        .unwrap();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0]["content"], "globex likes go");

    // Read-only: writes to the memory tables are rejected.
    assert!(engine.query_sql("DELETE FROM memories").await.is_err());
    assert!(engine
        .query_sql("INSERT INTO memories (id) VALUES ('x')")
        .await
        .is_err());
    // A query spanning both stores is rejected (they are separate databases).
    assert!(engine
        .query_sql("SELECT * FROM memories m JOIN episodic e ON e.id = m.id")
        .await
        .is_err());
}

#[tokio::test]
async fn prune_backups_keeps_newest_and_ignores_non_backups() {
    let tmp = tempfile::TempDir::new().unwrap();
    let backups = tmp.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    // Five backups, oldest→newest by timestamp name (which sort lexicographically).
    for name in [
        "20260101T000000Z",
        "20260102T000000Z",
        "20260103T000000Z",
        "20260104T000000Z",
        "20260105T000000Z",
    ] {
        let d = backups.join(name);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("manifest.json"), "{}").unwrap();
    }
    // A stray dir without a manifest must never be pruned.
    std::fs::create_dir_all(backups.join("notabackup")).unwrap();

    let mut cfg = inmem_config();
    cfg.backup.max_backups = 3;
    let engine = EcphoriaEngine::new(cfg).await.unwrap();

    assert_eq!(engine.prune_backups(&backups).await.unwrap(), 2);
    assert!(!backups.join("20260101T000000Z").exists());
    assert!(!backups.join("20260102T000000Z").exists());
    assert!(backups.join("20260103T000000Z").exists());
    assert!(backups.join("20260105T000000Z").exists());
    assert!(backups.join("notabackup").exists(), "stray dir untouched");

    // max_backups = 0 → keep all (no-op).
    let mut cfg0 = inmem_config();
    cfg0.backup.max_backups = 0;
    let engine0 = EcphoriaEngine::new(cfg0).await.unwrap();
    assert_eq!(engine0.prune_backups(&backups).await.unwrap(), 0);
}

#[tokio::test]
async fn memory_published_returns_only_published() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let mut pubd = MemoryInput::new(MemoryScope::tenant("default"), "public fact");
    pubd.metadata = serde_json::json!({ "published": true });
    engine.memory_add(pubd).await.unwrap();
    engine
        .memory_add(MemoryInput::new(
            MemoryScope::tenant("default"),
            "private fact",
        ))
        .await
        .unwrap();

    let published = engine.memory_published("default", 50).await.unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].content, "public fact");
}

#[tokio::test]
async fn memory_published_survives_limit_with_newer_unpublished() {
    // Regression: the published memory is the OLDEST; many newer unpublished ones follow. A small
    // limit must NOT truncate the published memory away (the old fetch-then-limit-then-filter bug).
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let mut pubd = MemoryInput::new(MemoryScope::tenant("default"), "the one published fact");
    pubd.metadata = serde_json::json!({ "published": true });
    engine.memory_add(pubd).await.unwrap();
    for i in 0..10 {
        engine
            .memory_add(MemoryInput::new(
                MemoryScope::tenant("default"),
                format!("newer private fact {i}"),
            ))
            .await
            .unwrap();
    }
    // limit=3 is far smaller than the 11 active memories; the published one is the oldest.
    let published = engine.memory_published("default", 3).await.unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].content, "the one published fact");
}

#[tokio::test]
async fn graph_analytics_centrality_path_communities() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    // Cluster 1: a,b,c all point at hub; hub → z. Cluster 2: x → y (disjoint).
    for (s, d) in [
        ("a", "hub"),
        ("b", "hub"),
        ("c", "hub"),
        ("hub", "z"),
        ("x", "y"),
    ] {
        engine
            .memory_link("default", s, "rel", d, None)
            .await
            .unwrap();
    }

    // Centrality: hub has in-degree 3.
    let c = engine.graph_centrality("default", None).await.unwrap();
    let hub = c.iter().find(|n| n.node == "hub").unwrap();
    assert_eq!(hub.in_degree, 3);
    assert_eq!(hub.out_degree, 1);

    // Shortest directed path a → z.
    assert_eq!(
        engine.graph_path("default", "a", "z", None).await.unwrap(),
        Some(vec!["a".into(), "hub".into(), "z".into()])
    );
    assert!(engine
        .graph_path("default", "z", "a", None)
        .await
        .unwrap()
        .is_none());

    // Two communities: {a,b,c,hub,z} and {x,y}.
    let comms = engine.graph_communities("default", None).await.unwrap();
    assert_eq!(comms.len(), 2);
    assert_eq!(comms[0].len(), 5);
    assert_eq!(comms[1], vec!["x".to_string(), "y".to_string()]);
}

#[tokio::test]
async fn image_attachment_embeds_and_searches() {
    // A deterministic stub image embedder (no ONNX): vector keyed on the first byte + length.
    struct StubImg;
    #[async_trait::async_trait]
    impl crate::embedding::ImageEmbeddingProvider for StubImg {
        async fn embed_image(&self, bytes: &[u8]) -> Result<Vec<f32>> {
            Ok(vec![
                bytes.first().copied().unwrap_or(0) as f32,
                bytes.len() as f32,
                1.0,
                0.0,
            ])
        }
        fn dimension(&self) -> usize {
            4
        }
        fn model_name(&self) -> &str {
            "stub"
        }
    }

    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = inmem_config();
    cfg.storage.data_dir = tmp.path().to_string_lossy().to_string();
    let engine = EcphoriaEngine::new(cfg).await.unwrap();
    engine.set_image_embedding(Arc::new(StubImg));

    let red = bytes::Bytes::from_static(&[10u8, 1, 2, 3, 4]);
    let blue = bytes::Bytes::from_static(&[200u8, 9, 8]);
    let red_meta = engine
        .attachment_put("t", None, "image/png", Some("red.png".into()), red.clone())
        .await
        .unwrap();
    engine
        .attachment_put("t", None, "image/png", Some("blue.png".into()), blue)
        .await
        .unwrap();

    // Searching by the red image recalls the red attachment first.
    let hits = engine.attachment_search_image("t", &red, 2).await.unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].id, red_meta.id);

    // Tenant isolation: another tenant's image search sees nothing here.
    assert!(engine
        .attachment_search_image("other", &red, 2)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn attachment_put_get_list_delete_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut cfg = inmem_config();
    cfg.storage.data_dir = tmp.path().to_string_lossy().to_string();
    let engine = EcphoriaEngine::new(cfg).await.unwrap();

    let data = bytes::Bytes::from_static(b"\x89PNG\r\n fake image bytes");
    let meta = engine
        .attachment_put(
            "t1",
            None,
            "image/png",
            Some("shot.png".into()),
            data.clone(),
        )
        .await
        .unwrap();
    assert_eq!(meta.size, data.len() as u64);
    assert_eq!(meta.content_type, "image/png");

    // Round-trips metadata + bytes.
    let (m2, b2) = engine.attachment_get("t1", meta.id).await.unwrap().unwrap();
    assert_eq!(m2.filename.as_deref(), Some("shot.png"));
    assert_eq!(&b2[..], &data[..]);

    // Tenant-scoped: another tenant can't read it.
    assert!(engine
        .attachment_get("other", meta.id)
        .await
        .unwrap()
        .is_none());

    assert_eq!(
        engine.attachment_list("t1", None, 10).await.unwrap().len(),
        1
    );

    // Delete removes metadata (and blob).
    assert!(engine.attachment_delete("t1", meta.id).await.unwrap());
    assert!(engine
        .attachment_get("t1", meta.id)
        .await
        .unwrap()
        .is_none());
    assert!(!engine.attachment_delete("t1", meta.id).await.unwrap());
}

#[tokio::test]
async fn authz_backend_is_pluggable() {
    // A custom backend that grants read of "bob" to everyone — proves the seam is on the read
    // path (no DB grant is created; the swap alone changes what shared-search returns).
    struct AlwaysBob;
    #[async_trait::async_trait]
    impl crate::authz::AuthzBackend for AlwaysBob {
        async fn granted_read_scopes(&self, _t: &str, _u: &str) -> Result<Vec<String>> {
            Ok(vec!["bob".into()])
        }
    }
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let alice = MemoryScope::user("alice");
    engine
        .memory_add(MemoryInput::new(
            MemoryScope::user("bob"),
            "bob likes sushi",
        ))
        .await
        .unwrap();
    // Default LocalGrants + no grant → alice sees nothing shared.
    assert!(engine
        .memory_search_shared("sushi", &alice, 5)
        .await
        .unwrap()
        .is_empty());
    // Inject the custom backend → alice now reads bob's memory, with no DB grant.
    engine.set_authz_backend(std::sync::Arc::new(AlwaysBob));
    let shared = engine
        .memory_search_shared("sushi", &alice, 5)
        .await
        .unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].memory.content, "bob likes sushi");
}

#[tokio::test]
async fn cross_scope_grants_widen_read_within_tenant_only() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let acme_bob = MemoryScope {
        tenant_id: "acme".into(),
        user_id: Some("bob".into()),
        agent_id: None,
        session_id: None,
    };
    let acme_alice = MemoryScope {
        tenant_id: "acme".into(),
        user_id: Some("alice".into()),
        agent_id: None,
        session_id: None,
    };
    let other_carol = MemoryScope {
        tenant_id: "other".into(),
        user_id: Some("carol".into()),
        agent_id: None,
        session_id: None,
    };
    engine
        .memory_add(MemoryInput::new(acme_bob.clone(), "bob likes sushi"))
        .await
        .unwrap();
    engine
        .memory_add(MemoryInput::new(other_carol.clone(), "carol likes tacos"))
        .await
        .unwrap();

    // No grant → alice's shared search sees nothing of bob's (baseline isolation holds).
    assert!(engine
        .memory_search_shared("sushi", &acme_alice, 5)
        .await
        .unwrap()
        .is_empty());

    // Grant bob→alice within acme → alice's shared search now includes bob's memory.
    engine.grant_share("acme", "alice", "bob").await.unwrap();
    let shared = engine
        .memory_search_shared("sushi", &acme_alice, 5)
        .await
        .unwrap();
    assert_eq!(shared.len(), 1);
    assert_eq!(shared[0].memory.content, "bob likes sushi");
    // Plain (non-shared) search still returns nothing for alice — grants are opt-in.
    assert!(engine
        .memory_search("sushi", &acme_alice, 5)
        .await
        .unwrap()
        .is_empty());

    // A grant cannot cross tenants: even with a grant naming carol, acme-scoped shared search
    // resolves the grantor within acme (acme, carol) and never reaches carol's 'other'-tenant
    // memory. (Without an embedding provider, retrieval falls back to recency and may surface
    // other *acme* memories, so we assert on carol's specific content, not emptiness.)
    engine.grant_share("acme", "alice", "carol").await.unwrap();
    let still = engine
        .memory_search_shared("tacos", &acme_alice, 5)
        .await
        .unwrap();
    assert!(
        still
            .iter()
            .all(|h| h.memory.content != "carol likes tacos"),
        "cross-tenant memory must never surface via a grant"
    );

    // Revoke → back to isolated.
    let grants = engine.list_grants("acme", "alice").await.unwrap();
    let bob_grant = grants.iter().find(|g| g.grantor_user_id == "bob").unwrap();
    assert!(engine
        .revoke_grant("acme", uuid::Uuid::parse_str(&bob_grant.id).unwrap())
        .await
        .unwrap());
    assert!(engine
        .memory_search_shared("sushi", &acme_alice, 5)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn semantic_consolidation_clusters_similar_memories() {
    let mut cfg = inmem_config();
    cfg.embedding.dimension = 8;
    let mut engine = EcphoriaEngine::new(cfg).await.unwrap();
    engine.set_embedding_for_test(Arc::new(ConstEmbedding { dim: 8 }));
    let scope = MemoryScope::user("alice");

    // Insert 3 active memories with identical vectors DIRECTLY (bypassing memory_add's dedup),
    // so the scope holds a cluster of near-duplicates to consolidate.
    let mut cvec = vec![0.0_f32; 8];
    cvec[0] = 1.0;
    for content in [
        "the sky is orange",
        "sky looked orange",
        "orange sky at dusk",
    ] {
        let m = Memory::new(scope.clone(), content);
        engine
            .memory_apply_rows(vec![MemoryRow {
                memory: m,
                embedding: Some(cvec.clone()),
            }])
            .await
            .unwrap();
    }
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 3);

    // Plan: the 3 near-duplicates form one cluster to fold.
    let plans = engine
        .memory_consolidate_similar_plan(&scope, 0.9)
        .await
        .unwrap();
    assert_eq!(plans.len(), 1);
    let (input, expired) = &plans[0];
    assert_eq!(expired.len(), 3);
    assert_eq!(input.metadata["consolidation"], "semantic");
    assert_eq!(
        input.metadata["source_memory_ids"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn memory_update_patches_fields_and_is_tenant_scoped() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice"); // tenant defaults to "default"
    let added = engine
        .memory_add(MemoryInput::new(scope.clone(), "likes tea"))
        .await
        .unwrap();
    let id = added.memory.id;
    let v0 = added.memory.version;

    // Partial patch: content + importance + mem_type + metadata; subject/scope untouched.
    let patch = crate::memory::cognition::MemoryPatch {
        content: Some("likes strong black tea".into()),
        importance: Some(0.9),
        mem_type: Some("episodic".into()),
        metadata: Some(serde_json::json!({ "source": "correction" })),
    };
    let updated = engine
        .memory_update(id, patch, Some("default"))
        .await
        .unwrap()
        .expect("memory should exist");
    assert_eq!(updated.id, id, "id is stable across an update");
    assert_eq!(updated.content, "likes strong black tea");
    assert!((updated.importance - 0.9).abs() < 1e-6);
    assert_eq!(updated.mem_type, "episodic");
    assert_eq!(updated.metadata["source"], "correction");
    assert!(updated.version > v0, "version bumps");
    // Persisted + visible via a normal read.
    assert_eq!(
        engine.memory_get(id).await.unwrap().unwrap().content,
        "likes strong black tea"
    );

    // Tenant isolation: another tenant cannot update this memory (None), and it stays unchanged.
    let none = engine
        .memory_update(
            id,
            crate::memory::cognition::MemoryPatch {
                content: Some("hijacked".into()),
                ..Default::default()
            },
            Some("other-tenant"),
        )
        .await
        .unwrap();
    assert!(
        none.is_none(),
        "cross-tenant update must not find the memory"
    );
    assert_eq!(
        engine.memory_get(id).await.unwrap().unwrap().content,
        "likes strong black tea"
    );

    // A missing id → None.
    assert!(engine
        .memory_update(
            uuid::Uuid::new_v4(),
            crate::memory::cognition::MemoryPatch {
                importance: Some(0.1),
                ..Default::default()
            },
            Some("default"),
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn memory_list_filters_and_paginates() {
    use crate::memory::cognition::MemoryFilter;
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("bob");

    for (i, (content, imp, mt)) in [
        ("a", 0.9, "semantic"),
        ("b", 0.7, "semantic"),
        ("c", 0.5, "episodic"),
        ("d", 0.3, "semantic"),
        ("e", 0.1, "episodic"),
    ]
    .iter()
    .enumerate()
    {
        let mut input = MemoryInput::new(scope.clone(), *content);
        input.importance = Some(*imp);
        input.mem_type = Some((*mt).into());
        input.subject = Some(format!("s{i}")); // distinct subjects → no dedup/supersession
        engine.memory_add(input).await.unwrap();
    }

    // No filter → all 5, ordered by importance desc.
    let all = engine
        .memory_list(&scope, 100, 0, &MemoryFilter::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].content, "a");

    // mem_type exact filter.
    let sem = engine
        .memory_list(
            &scope,
            100,
            0,
            &MemoryFilter {
                mem_type: Some("semantic".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(sem.len(), 3);
    assert!(sem.iter().all(|m| m.mem_type == "semantic"));

    // min_importance filter (>= 0.6 → 0.9, 0.7).
    let important = engine
        .memory_list(
            &scope,
            100,
            0,
            &MemoryFilter {
                min_importance: Some(0.6),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(important.len(), 2);

    // Offset pagination: two non-overlapping pages of 2.
    let page1 = engine
        .memory_list(&scope, 2, 0, &MemoryFilter::default())
        .await
        .unwrap();
    let page2 = engine
        .memory_list(&scope, 2, 2, &MemoryFilter::default())
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page2.len(), 2);
    assert_ne!(page1[0].id, page2[0].id, "pages don't overlap");
    assert_eq!(page1[0].content, "a");
    assert_eq!(page2[0].content, "c");

    // metadata exact-key filter: tag one memory, then filter on it.
    let target = all.iter().find(|m| m.content == "c").unwrap().id;
    engine
        .memory_update(
            target,
            crate::memory::cognition::MemoryPatch {
                metadata: Some(serde_json::json!({ "tag": "vip" })),
                ..Default::default()
            },
            Some("default"),
        )
        .await
        .unwrap();
    let vip = engine
        .memory_list(
            &scope,
            100,
            0,
            &MemoryFilter {
                metadata: Some(("tag".into(), "vip".into())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(vip.len(), 1);
    assert_eq!(vip[0].content, "c");

    // An unsafe metadata key is rejected (no rows) rather than risking injection.
    let inj = engine
        .memory_list(
            &scope,
            100,
            0,
            &MemoryFilter {
                metadata: Some(("a' OR '1'='1".into(), "x".into())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(inj.is_empty(), "unsafe metadata key must match nothing");
}

#[tokio::test]
async fn memory_scopes_enumerates_distinct_scopes_with_counts() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    // alice: 2 memories, bob: 1 — distinct subjects so nothing supersedes.
    for (u, s) in [("alice", "s1"), ("alice", "s2"), ("bob", "s3")] {
        let mut input = MemoryInput::new(MemoryScope::user(u), format!("{u}-{s}"));
        input.subject = Some(s.into());
        engine.memory_add(input).await.unwrap();
    }
    let scopes = engine.memory_scopes(Some("default")).await.unwrap();
    // Two distinct user scopes (alice, bob), most-populated first.
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].user_id.as_deref(), Some("alice"));
    assert_eq!(scopes[0].count, 2);
    let bob = scopes.iter().find(|s| s.user_id.as_deref() == Some("bob"));
    assert_eq!(bob.map(|s| s.count), Some(1));

    // Another tenant sees nothing.
    assert!(engine
        .memory_scopes(Some("other"))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn memory_remember_plan_materializes_without_writing() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    // No LLM extraction configured → the text becomes one fact/plan.
    let plans = engine
        .memory_remember_plan("alice prefers tea over coffee", &scope)
        .await
        .unwrap();
    assert_eq!(plans.len(), 1);
    let (result, rows) = &plans[0];
    assert_eq!(result.memory.content, "alice prefers tea over coffee");
    assert!(!rows.is_empty());
    // Planning is side-effect-free (the cluster path applies via Raft; nothing written locally).
    assert_eq!(engine.memory_count().await.unwrap(), 0);
}

#[tokio::test]
async fn session_distill_turns_events_into_memory() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    // Two session events (linked via the `_session_id` payload tag).
    engine
        .ingest(vec![
            Event::new(
                "chat",
                "user.msg",
                serde_json::json!({"_session_id": "sess-1", "text": "I moved to Berlin"}),
            ),
            Event::new(
                "chat",
                "assistant.msg",
                serde_json::json!({"_session_id": "sess-1", "text": "Noted your move."}),
            ),
        ])
        .await
        .unwrap();

    let scope = MemoryScope::tenant("default");
    let distilled = engine.session_distill("sess-1", &scope).await.unwrap();
    // No LLM extraction configured → one distilled memory holding the digest.
    assert_eq!(distilled.len(), 1);
    let mem = &distilled[0].memory;
    assert_eq!(mem.scope.session_id.as_deref(), Some("sess-1"));
    assert!(mem.content.contains("user.msg"));
    assert_eq!(mem.mem_type, "episodic");

    // It is persisted and retrievable in the session scope.
    let session_scope = MemoryScope {
        tenant_id: "default".into(),
        user_id: None,
        agent_id: None,
        session_id: Some("sess-1".into()),
    };
    assert_eq!(
        engine.memory_all(&session_scope, 10).await.unwrap().len(),
        1
    );

    // Distilling an empty session is a no-op.
    assert!(engine
        .session_distill("no-such-session", &scope)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn contradiction_review_queues_then_resolves() {
    let mut cfg = inmem_config();
    cfg.memory.cognition.contradiction_review = true;
    let engine = EcphoriaEngine::new(cfg).await.unwrap();
    let scope = MemoryScope::user("alice");

    let first = engine
        .memory_add(MemoryInput::new(scope.clone(), "on the pro plan").with_subject("plan"))
        .await
        .unwrap();
    // In review mode a contradiction does NOT auto-supersede — it flags a Conflict.
    let second = engine
        .memory_add(MemoryInput::new(scope.clone(), "upgraded to enterprise").with_subject("plan"))
        .await
        .unwrap();
    assert_eq!(second.outcome, MemoryOutcome::Conflict);
    // Both are active.
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 2);

    // The review queue surfaces the conflicting subject.
    let queue = engine.memory_contradictions(&scope).await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].subject, "plan");
    assert_eq!(queue[0].memories.len(), 2);

    // Resolve: keep the newer memory, supersede the other.
    let rows = engine
        .memory_resolve_plan(&scope, "plan", second.memory.id)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    engine.memory_apply_rows(rows).await.unwrap();

    // Now a single active memory, and the queue is empty.
    let active = engine.memory_all(&scope, 10).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].content, "upgraded to enterprise");
    assert!(engine
        .memory_contradictions(&scope)
        .await
        .unwrap()
        .is_empty());

    // Resolving with an id that isn't active for the subject is rejected (fail-closed).
    assert!(engine
        .memory_resolve_plan(&scope, "plan", first.memory.id)
        .await
        .is_err());
}

#[tokio::test]
async fn subject_casing_variants_contradict_not_coexist() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");

    // Same subject, different casing/whitespace → must be treated as ONE subject.
    let first = engine
        .memory_add(MemoryInput::new(scope.clone(), "blue").with_subject("Favorite Color"))
        .await
        .unwrap();
    assert_eq!(first.outcome, MemoryOutcome::Inserted);
    let second = engine
        .memory_add(MemoryInput::new(scope.clone(), "green").with_subject("  favorite   color "))
        .await
        .unwrap();
    // Contradiction resolved rather than a parallel active memory created.
    assert_eq!(second.outcome, MemoryOutcome::Superseded);

    let active = engine.memory_all(&scope, 10).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].content, "green");
    assert_eq!(active[0].subject.as_deref(), Some("favorite color"));

    // History is retrievable via any casing (normalized at query time too).
    let hist = engine
        .memory_history(&scope, "FAVORITE color")
        .await
        .unwrap();
    assert_eq!(hist.len(), 2);
}

#[tokio::test]
async fn memory_feedback_reinforces_and_retires() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    let added = engine
        .memory_add(MemoryInput::new(scope.clone(), "likes espresso"))
        .await
        .unwrap();
    let id = added.memory.id;
    let base = added.memory.importance;

    // Helpful → importance rises, memory stays active.
    let (mem, action) = engine
        .memory_feedback_plan(id, None, MemoryFeedback::Helpful)
        .await
        .unwrap()
        .unwrap();
    assert!(mem.importance > base);
    engine.memory_feedback_apply(action).await.unwrap();
    let after = engine.memory_get(id).await.unwrap().unwrap();
    assert!(after.importance > base);
    assert_eq!(after.state, MemoryState::Active);
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 1);

    // Wrong → retired (no longer active).
    let (_m, action) = engine
        .memory_feedback_plan(id, None, MemoryFeedback::Wrong)
        .await
        .unwrap()
        .unwrap();
    engine.memory_feedback_apply(action).await.unwrap();
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 0);

    // Cross-tenant id is not found.
    assert!(engine
        .memory_feedback_plan(id, Some("other"), MemoryFeedback::Helpful)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn memory_cdc_emits_lifecycle_events() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let mut rx = engine.memory_subscribe();
    let scope = MemoryScope::user("alice");

    // Insert → "upserted".
    let added = engine
        .memory_add(MemoryInput::new(scope.clone(), "on the pro plan").with_subject("plan"))
        .await
        .unwrap();
    let c = rx.recv().await.unwrap();
    assert_eq!(c.event, "upserted");
    assert_eq!(c.subject.as_deref(), Some("plan"));

    // Contradiction → old "superseded" + new "upserted".
    engine
        .memory_add(MemoryInput::new(scope.clone(), "upgraded to enterprise").with_subject("plan"))
        .await
        .unwrap();
    let mut events = vec![
        rx.recv().await.unwrap().event,
        rx.recv().await.unwrap().event,
    ];
    events.sort_unstable();
    assert_eq!(events, vec!["superseded", "upserted"]);

    // Expire → "expired".
    engine.memory_expire(&[added.memory.id]).await.unwrap();
    // (the first memory is already superseded; expiring emits an "expired" for it)
    let c = rx.recv().await.unwrap();
    assert_eq!(c.event, "expired");
}

#[tokio::test]
async fn delete_user_erases_only_that_users_memories() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let alice = MemoryScope {
        tenant_id: "acme".into(),
        user_id: Some("alice".into()),
        agent_id: None,
        session_id: None,
    };
    let bob = MemoryScope {
        tenant_id: "acme".into(),
        user_id: Some("bob".into()),
        agent_id: None,
        session_id: None,
    };
    engine
        .memory_add(MemoryInput::new(alice.clone(), "alice likes tea"))
        .await
        .unwrap();
    engine
        .memory_add(MemoryInput::new(bob.clone(), "bob likes coffee"))
        .await
        .unwrap();

    let summary = engine.delete_user("acme", "alice").await.unwrap();
    assert_eq!(summary["memories_deleted"], 1);

    // Alice erased, Bob (same tenant) untouched.
    assert_eq!(engine.memory_all(&alice, 100).await.unwrap().len(), 0);
    assert_eq!(engine.memory_all(&bob, 100).await.unwrap().len(), 1);
}

#[test]
fn parse_triple_lines_parses_pipe_format() {
    let t = parse_triple_lines("Alice | likes | coffee\nBob | works at | Acme\ngarbage line");
    assert_eq!(t.len(), 2);
    assert_eq!(t[0], ("Alice".into(), "likes".into(), "coffee".into()));
    assert_eq!(t[1], ("Bob".into(), "works_at".into(), "Acme".into()));
}

#[tokio::test]
async fn memory_subgraph_traverses_multiple_hops() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    // A → B → C → D chain.
    engine
        .memory_link("default", "A", "to", "B", None)
        .await
        .unwrap();
    engine
        .memory_link("default", "B", "to", "C", None)
        .await
        .unwrap();
    engine
        .memory_link("default", "C", "to", "D", None)
        .await
        .unwrap();
    // 1 hop from A reaches only the A→B edge.
    assert_eq!(
        engine
            .memory_subgraph("default", "A", 1, 100)
            .await
            .unwrap()
            .len(),
        1
    );
    // 2 hops reaches A→B and B→C.
    assert_eq!(
        engine
            .memory_subgraph("default", "A", 2, 100)
            .await
            .unwrap()
            .len(),
        2
    );
    // 3 hops reaches the whole chain.
    assert_eq!(
        engine
            .memory_subgraph("default", "A", 3, 100)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn multimodal_indexes_support_mixed_dimensions() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    // Different modalities can have DIFFERENT vector dimensions (separate per-modality indexes).
    let text_vec = vec![0.1f32; 768]; // e.g. a text embedder
    let img_vec = vec![0.2f32; 512]; // e.g. CLIP image embeddings
    engine
        .semantic_upsert_modal(
            uuid::Uuid::new_v4(),
            "text",
            "a caption",
            text_vec.clone(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    engine
        .semantic_upsert_modal(
            uuid::Uuid::new_v4(),
            "image",
            "cat.png",
            img_vec.clone(),
            serde_json::json!({}),
        )
        .await
        .unwrap();
    assert_eq!(
        engine.modalities().len(),
        2,
        "two independent modality indexes"
    );

    // Each modality searches with its own dimension.
    let img_hits = engine
        .semantic_search_modal(&img_vec, 5, Some("image"))
        .await
        .unwrap();
    assert_eq!(img_hits.len(), 1);
    assert_eq!(img_hits[0].entry.content, "cat.png");
    let text_hits = engine
        .semantic_search_modal(&text_vec, 5, Some("text"))
        .await
        .unwrap();
    assert_eq!(text_hits.len(), 1);
    assert_eq!(text_hits[0].entry.content, "a caption");

    // search_all with a 512-d vector only hits the matching-dimension (image) index.
    let all = engine
        .semantic_search_modal(&img_vec, 5, None)
        .await
        .unwrap();
    assert!(all.iter().all(|h| h.entry.content == "cat.png"));
}

#[tokio::test]
async fn unembedded_events_are_visible_for_reindex() {
    // No embedding provider configured → events are ingested but left unembedded, and the gap
    // is now visible/recoverable instead of silently lost.
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .ingest(vec![
            Event::new("s", "e", serde_json::json!({"x": 1})),
            Event::new("s", "e", serde_json::json!({"x": 2})),
            Event::new("s", "e", serde_json::json!({"x": 3})),
        ])
        .await
        .unwrap();
    assert_eq!(engine.unembedded_count().await.unwrap(), 3);
    // Reindex without a provider is a no-op (nothing to embed), leaving them recoverable.
    assert_eq!(engine.reindex_unembedded(100).await.unwrap(), 0);
    assert_eq!(engine.unembedded_count().await.unwrap(), 3);
}

#[tokio::test]
async fn migrate_tenant_memories_moves_between_engines() {
    // Simulates a rebalance move: tenant-a's memories move from shard A to shard B.
    let shard_a = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let shard_b = EcphoriaEngine::new(inmem_config()).await.unwrap();
    for i in 0..3 {
        shard_a
            .memory_add(MemoryInput::new(
                MemoryScope::tenant("tenant-a"),
                format!("fact {i}"),
            ))
            .await
            .unwrap();
    }
    // A different tenant on B must be untouched.
    shard_b
        .memory_add(MemoryInput::new(MemoryScope::tenant("tenant-b"), "b-fact"))
        .await
        .unwrap();

    let moved = shard_a
        .migrate_tenant_memories_to(&shard_b, "tenant-a")
        .await
        .unwrap();
    assert_eq!(moved, 3);
    // tenant-a is gone from A, present on B; tenant-b on B survives.
    assert_eq!(
        shard_a
            .export_tenant_memories("tenant-a")
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        shard_b
            .export_tenant_memories("tenant-a")
            .await
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        shard_b
            .export_tenant_memories("tenant-b")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn migrate_tenant_full_moves_events_memories_state() {
    // A FULL tenant move relocates episodic events + memories + state, then erases the source.
    let a = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let b = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let ta = crate::config::TenantContext::new("t");
    a.ingest_for_tenant(vec![Event::new("s", "e", serde_json::json!({"x": 1}))], &ta)
        .await
        .unwrap();
    a.memory_add(MemoryInput::new(MemoryScope::tenant("t"), "fact"))
        .await
        .unwrap();
    a.state_set_for_tenant("t", "bot", "k", serde_json::json!("v"))
        .await
        .unwrap();

    a.migrate_tenant_to(&b, "t").await.unwrap();

    // Everything is on the destination.
    let ev_b = b
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "t")
        .await
        .unwrap();
    assert_eq!(ev_b[0]["c"], "1");
    assert_eq!(
        b.memory_all(&MemoryScope::tenant("t"), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.state_get_for_tenant("t", "bot", "k")
            .await
            .unwrap()
            .map(|e| e.value),
        Some(serde_json::json!("v"))
    );
    // And erased from the source.
    let ev_a = a
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "t")
        .await
        .unwrap();
    assert_eq!(ev_a[0]["c"], "0");
    assert_eq!(
        a.memory_all(&MemoryScope::tenant("t"), 10)
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(a
        .state_get_for_tenant("t", "bot", "k")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn migrate_tenant_memories_preserves_source_events() {
    // A rebalance memory-move must NOT cascade-delete the tenant's episodic events on the source.
    let a = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let b = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let ta = crate::config::TenantContext::new("t");
    a.ingest_for_tenant(vec![Event::new("s", "e", serde_json::json!({"x": 1}))], &ta)
        .await
        .unwrap();
    a.memory_add(MemoryInput::new(MemoryScope::tenant("t"), "fact"))
        .await
        .unwrap();

    a.migrate_tenant_memories_to(&b, "t").await.unwrap();

    // Memories moved off the source, onto the destination.
    assert_eq!(
        a.memory_all(&MemoryScope::tenant("t"), 10)
            .await
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        b.memory_all(&MemoryScope::tenant("t"), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    // Episodic events stay on the source (not cascade-deleted).
    let ev = a
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "t")
        .await
        .unwrap();
    assert_eq!(ev[0]["c"], "1");
}

#[tokio::test]
async fn memory_consolidate_folds_lowest_importance() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    for i in 0..5 {
        engine
            .memory_add(MemoryInput::new(scope.clone(), format!("fact number {i}")))
            .await
            .unwrap();
    }
    // Keep the top 2; fold the other 3 into one summary memory.
    let consolidated = engine.memory_consolidate(&scope, 2).await.unwrap();
    assert!(consolidated.is_some());
    let mems = engine.memory_all(&scope, 100).await.unwrap();
    assert_eq!(
        mems.len(),
        3,
        "2 kept + 1 consolidated; the 3 originals are expired"
    );
    let summary = mems
        .iter()
        .find(|m| m.content.starts_with("Consolidated 3 memories"))
        .expect("a consolidated memory should exist");
    assert_eq!(summary.metadata["consolidated"], serde_json::json!(true));
    // Nothing to fold when within budget.
    assert!(engine
        .memory_consolidate(&scope, 10)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn memory_type_roundtrips_and_defaults() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    // Default type is "semantic".
    let added = engine
        .memory_add(MemoryInput::new(scope.clone(), "a plain fact"))
        .await
        .unwrap();
    assert_eq!(added.memory.mem_type, "semantic");
    // Explicit "procedural" type round-trips through DuckDB.
    let mut input = MemoryInput::new(scope.clone(), "how to deploy: run make");
    input.mem_type = Some("procedural".into());
    engine.memory_add(input).await.unwrap();
    let mems = engine.memory_all(&scope, 100).await.unwrap();
    assert!(mems
        .iter()
        .any(|m| m.mem_type == "procedural" && m.content.contains("deploy")));
    assert!(mems.iter().any(|m| m.mem_type == "semantic"));
}

#[tokio::test]
async fn memory_scope_cap_evicts_lowest() {
    let mut cfg = inmem_config();
    cfg.memory.cognition.max_memories_per_scope = 3;
    let engine = EcphoriaEngine::new(cfg).await.unwrap();
    let scope = MemoryScope::user("alice");
    for i in 0..5 {
        engine
            .memory_add(MemoryInput::new(
                scope.clone(),
                format!("distinct fact {i}"),
            ))
            .await
            .unwrap();
    }
    let mems = engine.memory_all(&scope, 100).await.unwrap();
    assert_eq!(mems.len(), 3, "scope should be capped at 3 memories");
}

#[tokio::test]
async fn memory_all_clamps_to_max_rows() {
    let mut cfg = inmem_config();
    cfg.query.max_rows = 2; // hard cap
    let engine = EcphoriaEngine::new(cfg).await.unwrap();
    let scope = MemoryScope::user("alice");
    for i in 0..5 {
        engine
            .memory_add(MemoryInput::new(scope.clone(), format!("fact {i}")))
            .await
            .unwrap();
    }
    // A huge requested limit is clamped to max_rows.
    let mems = engine.memory_all(&scope, usize::MAX).await.unwrap();
    assert_eq!(mems.len(), 2, "memory_all must clamp to query.max_rows");
}

#[tokio::test]
async fn memory_add_insert_and_get() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let added = engine
        .memory_add(MemoryInput::new(
            MemoryScope::user("alice"),
            "likes espresso",
        ))
        .await
        .unwrap();
    assert_eq!(added.outcome, MemoryOutcome::Inserted);
    let got = engine.memory_get(added.memory.id).await.unwrap().unwrap();
    assert_eq!(got.content, "likes espresso");
    assert_eq!(engine.memory_count().await.unwrap(), 1);
}

#[tokio::test]
async fn memory_subject_contradiction_supersedes_with_history() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");

    let first = engine
        .memory_add(
            MemoryInput::new(scope.clone(), "favorite color is blue")
                .with_subject("favorite_color"),
        )
        .await
        .unwrap();
    assert_eq!(first.outcome, MemoryOutcome::Inserted);

    let second = engine
        .memory_add(
            MemoryInput::new(scope.clone(), "favorite color is green")
                .with_subject("favorite_color"),
        )
        .await
        .unwrap();
    assert_eq!(second.outcome, MemoryOutcome::Superseded);
    assert_eq!(second.memory.supersedes, Some(first.memory.id));

    // Only the latest is active.
    let active = engine.memory_all(&scope, 10).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].content, "favorite color is green");

    // History keeps both, oldest first.
    let hist = engine
        .memory_history(&scope, "favorite_color")
        .await
        .unwrap();
    assert_eq!(hist.len(), 2);
    assert_eq!(hist[0].content, "favorite color is blue");

    // Bi-temporal: the superseded value is still answerable "as of" its validity window.
    let before = engine
        .memory_as_of(&scope, "favorite_color", first.memory.valid_from)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.content, "favorite color is blue");
}

#[tokio::test]
async fn semantic_merge_preserves_history_bitemporally() {
    // Regression: the semantic-dedup (subjectless) path must NOT overwrite the old memory's
    // content in place. It should close the old row as superseded and insert a new one, so the
    // prior text stays answerable — the same "nothing is silently hard-deleted" guarantee the
    // subject-contradiction path already upholds.
    let mut cfg = inmem_config();
    cfg.embedding.dimension = 8;
    let mut engine = EcphoriaEngine::new(cfg).await.unwrap();
    engine.set_embedding_for_test(Arc::new(ConstEmbedding { dim: 8 }));
    let scope = MemoryScope::user("alice");

    // First subjectless fact → a fresh insert.
    let first = engine
        .memory_add(MemoryInput::new(
            scope.clone(),
            "the sky looked orange at dusk",
        ))
        .await
        .unwrap();
    assert_eq!(first.outcome, MemoryOutcome::Inserted);

    // A near-duplicate (const embedding ⇒ cosine 1.0 ≥ dedup_threshold) → merged.
    let second = engine
        .memory_add(MemoryInput::new(
            scope.clone(),
            "the sky was a deep orange at sunset",
        ))
        .await
        .unwrap();
    assert_eq!(second.outcome, MemoryOutcome::Merged);
    // The merge is modeled as a supersession: the new row points back at the old.
    assert_eq!(second.memory.supersedes, Some(first.memory.id));
    assert_ne!(second.memory.id, first.memory.id);

    // Only the new memory is active, carrying the new content.
    let active = engine.memory_all(&scope, 10).await.unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].content, "the sky was a deep orange at sunset");

    // The OLD row is preserved (not overwritten): still retrievable, now superseded, with its
    // original content and a closed validity window.
    let old = engine.memory_get(first.memory.id).await.unwrap().unwrap();
    assert_eq!(old.content, "the sky looked orange at dusk");
    assert_eq!(old.state, MemoryState::Superseded);
    assert!(
        old.valid_to.is_some(),
        "superseded row must have valid_to set"
    );

    // Both rows persist (old superseded + new active) — history is intact.
    assert_eq!(engine.memory_count().await.unwrap(), 2);
}

#[tokio::test]
async fn memory_provenance_resolves_sources_and_history() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");

    // An ingested event that will back a memory.
    let ev = Event::new(
        "crm",
        "note",
        serde_json::json!({"text": "moved to Enterprise"}),
    );
    let ev_id = ev.id;
    engine.ingest(vec![ev]).await.unwrap();

    // A subject-keyed memory citing that event, then a contradiction (supersession).
    let first = engine
        .memory_add(
            MemoryInput::new(scope.clone(), "On the Pro plan")
                .with_subject("plan")
                .with_source_event_ids(vec![ev_id]),
        )
        .await
        .unwrap();
    let second = engine
        .memory_add(MemoryInput::new(scope.clone(), "Upgraded to Enterprise").with_subject("plan"))
        .await
        .unwrap();
    assert_eq!(second.outcome, MemoryOutcome::Superseded);

    // Provenance of the FIRST (now superseded) memory: its source event resolves, and the
    // history chain shows both versions.
    let prov = engine
        .memory_provenance(first.memory.id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prov.source_events.len(), 1);
    assert_eq!(prov.source_events[0].id, ev_id);
    assert_eq!(prov.history.len(), 2);
    assert_eq!(prov.history[0].content, "On the Pro plan");

    // A cross-tenant id reads as not found.
    assert!(engine
        .memory_provenance(first.memory.id, Some("other-tenant"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn memory_identical_is_confirmed_not_duplicated() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("bob");
    engine
        .memory_add(MemoryInput::new(scope.clone(), "works at ACME").with_subject("employer"))
        .await
        .unwrap();
    let again = engine
        .memory_add(MemoryInput::new(scope.clone(), "works at ACME").with_subject("employer"))
        .await
        .unwrap();
    assert_eq!(again.outcome, MemoryOutcome::Confirmed);
    assert_eq!(engine.memory_count().await.unwrap(), 1);
}

#[tokio::test]
async fn memory_search_lexical_ranks_relevant_first() {
    // No embedding provider in the default config → pure deterministic BM25 ranking.
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    for content in [
        "alice loves hiking in the mountains",
        "alice works as a software engineer",
        "the weather is sunny today",
    ] {
        engine
            .memory_add(MemoryInput::new(scope.clone(), content))
            .await
            .unwrap();
    }

    let hits = engine
        .memory_search("software engineering job", &scope, 3)
        .await
        .unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].memory.content, "alice works as a software engineer");
}

#[tokio::test]
async fn memory_search_applies_reranker() {
    use crate::rerank::Reranker;

    // A reranker that forces any "mountains" passage to the top, overriding BM25/recency.
    struct KeywordReranker;
    #[async_trait::async_trait]
    impl Reranker for KeywordReranker {
        async fn rerank(&self, _q: &str, docs: &[String]) -> Result<Vec<f32>> {
            Ok(docs
                .iter()
                .map(|d| if d.contains("mountains") { 10.0 } else { 1.0 })
                .collect())
        }
        fn model_name(&self) -> &str {
            "keyword"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    // Both share the term "alice", so both are in the lexical candidate pool the reranker sees.
    for content in [
        "alice works as a software engineer",
        "alice loves hiking in the mountains",
    ] {
        engine
            .memory_add(MemoryInput::new(scope.clone(), content))
            .await
            .unwrap();
    }

    // Without a reranker the keyword doc is not first for this query.
    let baseline = engine.memory_search("alice", &scope, 2).await.unwrap();
    assert_eq!(baseline.len(), 2);

    // With the reranker, the "mountains" passage is promoted to rank 0.
    engine.reranker = Some(Arc::new(KeywordReranker));
    let reranked = engine.memory_search("alice", &scope, 2).await.unwrap();
    assert_eq!(
        reranked[0].memory.content,
        "alice loves hiking in the mountains"
    );
}

#[tokio::test]
async fn memory_search_graph_expansion_surfaces_linked_memory() {
    // Set up two memories + an edge; return the id of the graph-linked (lexically-unmatched) one.
    async fn setup(graph_expansion: bool) -> (EcphoriaEngine, uuid::Uuid) {
        let mut cfg = inmem_config();
        cfg.memory.cognition.graph_expansion = graph_expansion;
        let engine = EcphoriaEngine::new(cfg).await.unwrap();
        let scope = MemoryScope::user("alice");
        // Linked fact: shares NO term with the query "Acme".
        let linked = engine
            .memory_add(MemoryInput::new(
                scope.clone(),
                "The Q3 offsite is in Lisbon",
            ))
            .await
            .unwrap();
        // Decoy that DOES match "Acme" lexically (so rankings are non-empty → no recency fallback).
        engine
            .memory_add(MemoryInput::new(
                scope.clone(),
                "Acme reported strong revenue",
            ))
            .await
            .unwrap();
        // Edge: Acme --hosts--> offsite, sourced from the Lisbon memory.
        engine
            .memory_link(
                "default",
                "Acme",
                "hosts",
                "offsite",
                Some(linked.memory.id),
            )
            .await
            .unwrap();
        (engine, linked.memory.id)
    }

    let scope = MemoryScope::user("alice");

    // Off: the query "Acme" matches only the decoy; the Lisbon memory is not retrieved.
    let (off, off_id) = setup(false).await;
    let off_hits = off.memory_search("Acme", &scope, 5).await.unwrap();
    assert!(
        !off_hits.iter().any(|h| h.memory.id == off_id),
        "without graph expansion the edge-linked memory is not surfaced"
    );

    // On: the Acme→offsite edge surfaces the Lisbon memory despite no lexical/vector match.
    let (on, on_id) = setup(true).await;
    let on_hits = on.memory_search("Acme", &scope, 5).await.unwrap();
    assert!(
        on_hits.iter().any(|h| h.memory.id == on_id),
        "graph expansion should surface the edge-linked memory"
    );
}

#[tokio::test]
async fn auto_graph_extracts_edges_deterministically() {
    use crate::memory::cognition::MemoryRow;

    let mut cfg_a = inmem_config();
    cfg_a.memory.cognition.auto_graph = true;
    let a = EcphoriaEngine::new(cfg_a).await.unwrap();
    let scope = MemoryScope::user("alice");
    let added = a
        .memory_add(MemoryInput::new(scope.clone(), "Alice works at Acme"))
        .await
        .unwrap();

    // auto_graph created at least one edge, all sourced from this memory.
    let edges_a = a.memory_store.list_edges("default", 50).await.unwrap();
    assert!(!edges_a.is_empty(), "auto_graph should extract edges");
    assert!(edges_a
        .iter()
        .all(|e| e.source_memory_id == Some(added.memory.id)));

    // Determinism (replication-safety): applying the same materialized memory row on a second
    // engine yields byte-identical edge ids (uuidv5 derived from the memory id) — so followers
    // build the identical graph during Raft apply without any payload change.
    let mut cfg_b = inmem_config();
    cfg_b.memory.cognition.auto_graph = true;
    let b = EcphoriaEngine::new(cfg_b).await.unwrap();
    b.memory_apply_rows(vec![MemoryRow {
        memory: added.memory.clone(),
        embedding: None,
    }])
    .await
    .unwrap();
    let edges_b = b.memory_store.list_edges("default", 50).await.unwrap();

    let mut ids_a: Vec<_> = edges_a.iter().map(|e| e.id).collect();
    let mut ids_b: Vec<_> = edges_b.iter().map(|e| e.id).collect();
    ids_a.sort();
    ids_b.sort();
    assert_eq!(ids_a, ids_b, "edge ids must be identical on every replica");

    // Idempotent: re-applying the same row doesn't duplicate edges (ON CONFLICT DO NOTHING).
    b.memory_apply_rows(vec![MemoryRow {
        memory: added.memory.clone(),
        embedding: None,
    }])
    .await
    .unwrap();
    let edges_b2 = b.memory_store.list_edges("default", 50).await.unwrap();
    assert_eq!(
        edges_b2.len(),
        edges_b.len(),
        "re-apply must not duplicate edges"
    );
}

#[tokio::test]
async fn run_ledger_lifecycle() {
    use crate::runtime::{RunPatch, RunStatus};
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create(
            "default",
            Some("agent-1".into()),
            None,
            serde_json::json!({"q": "hi"}),
        )
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Pending);

    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Running),
                started_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Succeeded),
                result: Some(serde_json::json!({"a": 42})),
                ended_at: Some(chrono::Utc::now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let got = engine.run_get(run.id).await.unwrap().unwrap();
    assert_eq!(got.status, RunStatus::Succeeded);
    assert_eq!(got.result, serde_json::json!({"a": 42}));
    assert!(got.started_at.is_some() && got.ended_at.is_some());
    assert_eq!(got.input, serde_json::json!({"q": "hi"}));

    assert_eq!(engine.run_list("default", None, 10).await.unwrap().len(), 1);
    assert!(engine
        .run_list("default", Some(RunStatus::Running), 10)
        .await
        .unwrap()
        .is_empty());
    // A fresh run has an empty step trace (no episodic events tagged with its id yet).
    assert!(engine.run_trace(run.id).await.unwrap().is_empty());
}

#[tokio::test]
async fn run_apply_is_deterministic_across_engines() {
    // The same materialized run + patch applied on two engines yields identical rows — the
    // replication-safety contract (no now()/uuid at apply time), ready for a GraphSupersede-style
    // RunCreate/RunUpdate AppRequest.
    use crate::runtime::{Run, RunPatch, RunStatus};
    let now = chrono::Utc::now();
    let id = uuid::Uuid::new_v4();
    let seed = Run {
        id,
        tenant_id: "default".into(),
        agent_id: None,
        parent_run_id: None,
        status: RunStatus::Pending,
        input: serde_json::json!({"x": 1}),
        result: serde_json::Value::Null,
        error: None,
        cursor: serde_json::Value::Null,
        created_at: now,
        updated_at: now,
        started_at: None,
        ended_at: None,
    };
    let patch = RunPatch {
        status: Some(RunStatus::Succeeded),
        result: Some(serde_json::json!({"ok": true})),
        ended_at: Some(now),
        ..Default::default()
    };

    let mut rows = Vec::new();
    for _ in 0..2 {
        let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
        engine.run_apply_create(&seed).await.unwrap();
        engine.run_apply_update(id, &patch, now).await.unwrap();
        rows.push(engine.run_get(id).await.unwrap().unwrap());
    }
    assert_eq!(rows[0].status, RunStatus::Succeeded);
    assert_eq!(rows[0].result, rows[1].result);
    assert_eq!(rows[0].updated_at, rows[1].updated_at);
    assert_eq!(rows[0].ended_at, rows[1].ended_at);
}

#[tokio::test]
async fn run_agent_loops_tool_then_answers_and_journals_steps() {
    use crate::llm::CompletionProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Scripted model: first turn calls the search tool, second turn answers.
    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _system: &str, _user: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL search: cats".to_string(),
                _ => "Cats are fluffy companions.".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    let scope = MemoryScope {
        tenant_id: "default".into(),
        agent_id: Some("a1".into()),
        ..Default::default()
    };
    engine
        .memory_add(MemoryInput::new(scope, "cats are fluffy"))
        .await
        .unwrap();

    let run = engine
        .run_agent("default", "a1", "tell me about cats", 5)
        .await
        .unwrap();

    assert_eq!(run.status, crate::runtime::RunStatus::Succeeded);
    assert_eq!(run.result["answer"], "Cats are fluffy companions.");

    // The trace journaled run_start + tool_call + llm_answer (3 steps), recallable by run id.
    let steps = engine.run_trace(run.id).await.unwrap();
    assert_eq!(steps.len(), 3);
    assert!(steps.iter().any(|s| s["event_type"] == "tool_call"));
    assert!(steps.iter().any(|s| s["event_type"] == "llm_answer"));
}

#[tokio::test]
async fn triggers_register_match_and_fire_runs() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .trigger_register("on_pr", "github", "pull_request.opened", "pr-agent")
        .await
        .unwrap();
    engine
        .trigger_register("on_any", "*", "*", "catch-all")
        .await
        .unwrap();
    assert_eq!(engine.trigger_list().await.unwrap().len(), 2);

    // A GitHub PR event matches both the exact and the wildcard trigger → 2 runs.
    let fired = engine
        .fire_triggers(
            "default",
            "github",
            "pull_request.opened",
            serde_json::json!({"pr": 42}),
        )
        .await
        .unwrap();
    assert_eq!(fired.len(), 2);

    // A different source matches only the wildcard → 1 run.
    let fired2 = engine
        .fire_triggers("default", "sentry", "issue.created", serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(fired2.len(), 1);

    assert_eq!(engine.run_list("default", None, 10).await.unwrap().len(), 3);
}

#[tokio::test]
async fn hitl_request_then_approve() {
    use crate::runtime::RunStatus;
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();

    engine
        .run_request_approval(run.id, "default", "ship it?")
        .await
        .unwrap();
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::WaitingApproval
    );
    assert_eq!(
        engine.run_approval_status(run.id).await.unwrap().unwrap()["state"],
        "pending"
    );

    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(
        engine.run_approval_status(run.id).await.unwrap().unwrap()["state"],
        "approved"
    );
}

#[tokio::test]
async fn run_workflow_executes_subagents_in_dep_order() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, WorkflowNode};

    struct Echo;
    #[async_trait::async_trait]
    impl CompletionProvider for Echo {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok("done".to_string())
        }
        fn model_name(&self) -> &str {
            "echo"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Echo));

    // Node "b" depends on "a"; topo order must run a before b.
    let nodes = vec![
        WorkflowNode {
            id: "b".into(),
            agent_id: "agent".into(),
            question: "second".into(),
            deps: vec!["a".into()],
        },
        WorkflowNode {
            id: "a".into(),
            agent_id: "agent".into(),
            question: "first".into(),
            deps: vec![],
        },
    ];
    let parent = engine.run_workflow("default", nodes).await.unwrap();
    assert_eq!(parent.status, RunStatus::Succeeded);

    // Parent + 2 sub-agent children; children link back to the parent.
    let runs = engine.run_list("default", None, 10).await.unwrap();
    assert_eq!(runs.len(), 3);
    let children: Vec<_> = runs
        .iter()
        .filter(|r| r.parent_run_id == Some(parent.id))
        .collect();
    assert_eq!(children.len(), 2);
}

#[tokio::test]
async fn run_agent_pauses_for_approval_then_resumes() {
    use crate::llm::CompletionProvider;
    use crate::runtime::RunStatus;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL approve: deploy to prod?".to_string(),
                _ => "deployed".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));

    // The agent asks for approval → run pauses.
    let run = engine
        .run_agent("default", "a", "ship it", 5)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::WaitingApproval);

    // Approve, then resume → the agent continues and finishes.
    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    let resumed = engine.run_resume(run.id, "default").await.unwrap();
    assert_eq!(resumed.status, RunStatus::Succeeded);
    assert_eq!(resumed.result["answer"], "deployed");
}

#[tokio::test]
async fn rebuild_transcript_is_faithful_and_keeps_tool_seq_stable() {
    // Regression for the resume path: every journaled step type must re-render as the exact line
    // the live loop emitted. Otherwise the idempotency counter (which counts "TOOL call ") resets
    // to 0 and external-tool results are erased — defeating effectively-once across a resume.
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    let rid = run.id;
    for (et, payload) in [
        ("run_start", serde_json::json!({ "question": "Q?" })),
        (
            "tool_call",
            serde_json::json!({ "tool": "search", "query": "cats", "results": ["fluffy", "cute"] }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "remember", "content": "cats are fluffy" }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "billing/charge", "result": { "ok": true } }),
        ),
        (
            "tool_call",
            serde_json::json!({ "tool": "email/send", "result": { "sent": 1 } }),
        ),
    ] {
        engine
            .run_log_step(rid, "default", et, payload)
            .await
            .unwrap();
    }

    let t = engine.rebuild_agent_transcript(rid).await.unwrap();

    assert!(t.contains("Question: Q?"), "{t}");
    assert!(t.contains("TOOL search: cats"), "{t}");
    assert!(
        t.contains("fluffy | cute"),
        "search results must be replayed: {t}"
    );
    assert!(t.contains("TOOL remember: cats are fluffy"), "{t}");
    // External calls must re-render as `TOOL call …` with their real result replayed…
    assert!(t.contains("TOOL call billing charge"), "{t}");
    assert!(t.contains("TOOL call email send"), "{t}");
    assert!(
        t.contains("\"ok\":true"),
        "external result must be replayed: {t}"
    );
    // …so on resume the idempotency counter is 2 (the two prior external calls), not 0.
    assert_eq!(
        t.matches("TOOL call ").count(),
        2,
        "tool_seq must resume at the count of prior external calls: {t}"
    );
}

#[tokio::test]
async fn approval_cannot_be_resolved_twice() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_request_approval(run.id, "default", "ok to proceed?")
        .await
        .unwrap();
    // First resolution succeeds...
    engine
        .run_resolve_approval(run.id, "default", true)
        .await
        .unwrap();
    // ...a second (double-approve / late reject) is rejected — approval is no longer pending.
    assert!(engine
        .run_resolve_approval(run.id, "default", false)
        .await
        .is_err());
}

#[tokio::test]
async fn event_vector_index_reloads_on_startup() {
    use crate::memory::semantic::{SemanticEntry, SemanticStore};
    let dir = tempfile::tempdir().unwrap();
    let idx_dir = dir.path().join("vectors");
    // Pre-populate + persist an event vector index to disk.
    {
        let store = SemanticStore::with_dimension(4).unwrap();
        store
            .upsert(&SemanticEntry {
                id: uuid::Uuid::new_v4(),
                content: "hello".into(),
                embedding: vec![0.1, 0.2, 0.3, 0.4],
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        store.save(&idx_dir).unwrap();
    }
    // A file-backed engine pointed at that index_dir must RELOAD it (not start empty).
    let mut c = CoreConfig::default();
    c.embedding.dimension = 4;
    c.memory.episodic.db_path = dir.path().join("ep.duckdb").to_string_lossy().into_owned();
    c.memory.state.db_path = dir.path().join("st.db").to_string_lossy().into_owned();
    c.memory.cognition.db_path = dir.path().join("cog.duckdb").to_string_lossy().into_owned();
    c.runtime.db_path = ":memory:".into();
    c.memory.semantic.index_dir = idx_dir.to_string_lossy().into_owned();
    let engine = EcphoriaEngine::new(c).await.unwrap();
    assert_eq!(
        engine.semantic_count(),
        1,
        "event vector index must be reloaded from disk on startup"
    );
}

#[tokio::test]
async fn erroring_agent_run_is_marked_failed_not_left_running() {
    use crate::llm::CompletionProvider;
    // A provider that always errors → the loop returns Err on the first turn.
    struct Boom;
    #[async_trait::async_trait]
    impl CompletionProvider for Boom {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Err(crate::Error::Llm("boom".into()))
        }
        fn model_name(&self) -> &str {
            "boom"
        }
    }
    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Boom));
    let run = engine
        .run_create("default", Some("a1".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    let res = engine
        .drive_agent_loop(run.id, "default", "a1", String::new(), 8)
        .await;
    assert!(res.is_err(), "the erroring loop must surface the error");
    // The run must be terminal (Failed), so the dispatcher won't resume it forever (poison run).
    let after = engine.run_get(run.id).await.unwrap().unwrap();
    assert_eq!(after.status, RunStatus::Failed);
}

#[tokio::test]
async fn resume_reuses_recorded_tool_result_without_re_executing() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Scripted model: issues the external call, then answers.
    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL call billing charge: {}".to_string(),
                _ => "done".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }
    // Counts how many times the external tool ACTUALLY runs.
    struct CountingTool {
        runs: std::sync::Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl ToolExecutor for CountingTool {
        async fn call_tool(
            &self,
            _server: &str,
            _tool: &str,
            _args: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({ "charged": true }))
        }
    }

    let runs = std::sync::Arc::new(AtomicUsize::new(0));
    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    engine.set_tool_executor(std::sync::Arc::new(CountingTool { runs: runs.clone() }));

    // Pre-journal the tool_call (idempotency key :tool:0) as if it already ran in a prior
    // attempt — the server-side ledger must reuse its result and NOT re-execute the side effect.
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_log_step(
            run.id,
            "default",
            "tool_call",
            serde_json::json!({
                "tool": "billing/charge",
                "idempotency_key": format!("{}:tool:0", run.id),
                "result": { "charged": true },
            }),
        )
        .await
        .unwrap();

    let out = engine
        .drive_agent_loop(run.id, "default", "a", String::new(), 5)
        .await
        .unwrap();
    assert_eq!(out.status, RunStatus::Succeeded);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the external tool must not re-execute — its recorded result is reused"
    );
}

#[tokio::test]
async fn run_agent_calls_external_tool_via_executor() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunStatus, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scripted {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl CompletionProvider for Scripted {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok(match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => "TOOL call gh create_issue: {\"title\":\"bug\"}".to_string(),
                _ => "issue created".to_string(),
            })
        }
        fn model_name(&self) -> &str {
            "scripted"
        }
    }

    struct MockTool;
    #[async_trait::async_trait]
    impl ToolExecutor for MockTool {
        async fn call_tool(
            &self,
            server: &str,
            tool: &str,
            args: serde_json::Value,
        ) -> crate::Result<serde_json::Value> {
            Ok(serde_json::json!({ "server": server, "tool": tool, "args": args, "ok": true }))
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Scripted {
        calls: AtomicUsize::new(0),
    }));
    engine.set_tool_executor(std::sync::Arc::new(MockTool));

    let run = engine
        .run_agent("default", "a", "make an issue", 5)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Succeeded);
    assert_eq!(run.result["answer"], "issue created");

    // The downstream tool call was executed and journaled.
    let steps = engine.run_trace(run.id).await.unwrap();
    // The tool call was journaled with a deterministic idempotency key (first call → :tool:0).
    assert!(steps.iter().any(|s| s["event_type"] == "tool_call"
        && s["payload"]["tool"] == "gh/create_issue"
        && s["payload"]["idempotency_key"] == format!("{}:tool:0", run.id)));
}

#[tokio::test]
async fn run_writes_route_through_replicator_when_set() {
    use crate::runtime::{Run, RunPatch, RunReplicator};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Recorder {
        creates: AtomicUsize,
        updates: AtomicUsize,
        steps: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl RunReplicator for Recorder {
        async fn replicate_run_create(&self, _run: &Run) -> crate::Result<()> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_run_update(
            &self,
            _id: uuid::Uuid,
            _patch: &RunPatch,
            _updated_at: chrono::DateTime<chrono::Utc>,
        ) -> crate::Result<()> {
            self.updates.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_step(
            &self,
            _event: crate::memory::episodic::Event,
        ) -> crate::Result<()> {
            self.steps.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn replicate_state_set(
            &self,
            _agent_id: &str,
            _key: &str,
            _value: serde_json::Value,
        ) -> crate::Result<()> {
            Ok(())
        }
    }

    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let rec = std::sync::Arc::new(Recorder::default());
    engine.set_run_replicator(rec.clone());

    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_update(
            run.id,
            RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    engine
        .run_log_step(run.id, "default", "test", serde_json::json!({}))
        .await
        .unwrap();

    assert_eq!(rec.creates.load(Ordering::SeqCst), 1);
    assert_eq!(rec.updates.load(Ordering::SeqCst), 1);
    assert_eq!(rec.steps.load(Ordering::SeqCst), 1);
    // The recorder doesn't apply, so the local store was bypassed (apply happens via Raft).
    assert!(engine.run_get(run.id).await.unwrap().is_none());
}

#[tokio::test]
async fn dispatcher_resumes_orphaned_running_run() {
    use crate::llm::CompletionProvider;
    use crate::runtime::{RunPatch, RunStatus};

    struct Echo;
    #[async_trait::async_trait]
    impl CompletionProvider for Echo {
        async fn complete(&self, _s: &str, _u: &str) -> crate::Result<String> {
            Ok("done".to_string())
        }
        fn model_name(&self) -> &str {
            "echo"
        }
    }

    let mut engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine.completion = Some(std::sync::Arc::new(Echo));

    // A run that was left "running" with a journaled start step but a STALE updated_at — as if
    // the leader driving it crashed mid-loop.
    let run = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_log_step(
            run.id,
            "default",
            "run_start",
            serde_json::json!({ "question": "hi" }),
        )
        .await
        .unwrap();
    let stale = chrono::Utc::now() - chrono::Duration::seconds(120);
    engine
        .run_apply_update(
            run.id,
            &RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
            stale,
        )
        .await
        .unwrap();

    // A fresh (non-stale) running run must NOT be picked up.
    let fresh = engine
        .run_create("default", Some("a".into()), None, serde_json::json!({}))
        .await
        .unwrap();
    engine
        .run_update(
            fresh.id,
            RunPatch {
                status: Some(RunStatus::Running),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let resumed = engine.run_dispatch_once(60, 10).await.unwrap();
    assert_eq!(resumed, 1);
    assert_eq!(
        engine.run_get(run.id).await.unwrap().unwrap().status,
        RunStatus::Succeeded
    );
    // The fresh run was skipped (still running, not driven).
    assert_eq!(
        engine.run_get(fresh.id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
}

#[test]
fn parse_extracted_facts_handles_fenced_json() {
    let text = "Sure!\n```json\n[{\"subject\":\"city\",\"content\":\"Lives in Paris\"},\
                    {\"content\":\"Likes jazz\"}]\n```";
    let facts = super::parse_extracted_facts(text).unwrap();
    assert_eq!(facts.len(), 2);
    assert_eq!(
        facts[0],
        (Some("city".to_string()), "Lives in Paris".to_string())
    );
    assert_eq!(facts[1], (None, "Likes jazz".to_string()));
}

#[tokio::test]
async fn memory_remember_fallback_stores_raw_text() {
    // Default config has extraction = "none" → deterministic single-memory fallback.
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    let added = engine
        .memory_remember("alice prefers tea over coffee", &scope)
        .await
        .unwrap();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].memory.content, "alice prefers tea over coffee");
    assert_eq!(engine.memory_count().await.unwrap(), 1);
}

#[tokio::test]
async fn memory_enforce_decay_keeps_fresh_memories() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    engine
        .memory_add(MemoryInput::new(scope.clone(), "fresh fact"))
        .await
        .unwrap();
    // Nothing is old enough to forget yet.
    assert_eq!(engine.memory_enforce_decay().await.unwrap(), 0);
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn memory_decay_plan_is_read_only() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let scope = MemoryScope::user("alice");
    engine
        .memory_add(MemoryInput::new(scope.clone(), "fresh fact"))
        .await
        .unwrap();
    // Fresh memory → nothing to forget; and the plan must NOT mutate anything.
    let plan = engine.memory_decay_plan().await.unwrap();
    assert!(plan.is_empty());
    assert_eq!(engine.memory_all(&scope, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn semantic_search_for_tenant_isolates() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let mut v = vec![0.0f32; 768];
    v[0] = 1.0; // both entries point the same way → both would match without scoping
    engine
        .semantic_upsert(&SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "tenant A secret".into(),
            embedding: v.clone(),
            metadata: serde_json::json!({"tenant_id": "tenant-a"}),
        })
        .await
        .unwrap();
    engine
        .semantic_upsert(&SemanticEntry {
            id: uuid::Uuid::new_v4(),
            content: "tenant B secret".into(),
            embedding: v.clone(),
            metadata: serde_json::json!({"tenant_id": "tenant-b"}),
        })
        .await
        .unwrap();

    let hits = engine
        .semantic_search_for_tenant(&v, 5, "tenant-a", None, None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].entry.content, "tenant A secret");
}

#[tokio::test]
async fn memory_scope_isolation() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .memory_add(MemoryInput::new(MemoryScope::user("alice"), "secret A"))
        .await
        .unwrap();
    engine
        .memory_add(MemoryInput::new(MemoryScope::user("bob"), "secret B"))
        .await
        .unwrap();

    let alice = engine
        .memory_all(&MemoryScope::user("alice"), 10)
        .await
        .unwrap();
    assert_eq!(alice.len(), 1);
    assert_eq!(alice[0].content, "secret A");
}

#[tokio::test]
async fn backup_and_restore_roundtrips_all_stores() {
    let dir = tempfile::tempdir().unwrap();
    let backup_dir = dir.path().join("backup");

    // Source: an episodic event, a memory, and agent state.
    let src = EcphoriaEngine::new(inmem_config()).await.unwrap();
    src.ingest(vec![Event::new("src", "e", serde_json::json!({"x": 1}))])
        .await
        .unwrap();
    src.memory_add(MemoryInput::new(MemoryScope::user("alice"), "likes tea"))
        .await
        .unwrap();
    src.state_set("bot", "mood", serde_json::json!("happy"))
        .await
        .unwrap();
    src.backup(&backup_dir).await.unwrap();

    // Fresh engine → restore → all three stores are present.
    let dst = EcphoriaEngine::new(inmem_config()).await.unwrap();
    assert_eq!(dst.event_count().await.unwrap(), 0);
    dst.restore_from_backup(&backup_dir).await.unwrap();

    assert_eq!(dst.event_count().await.unwrap(), 1);
    assert_eq!(dst.memory_count().await.unwrap(), 1);
    assert_eq!(
        dst.memory_all(&MemoryScope::user("alice"), 10)
            .await
            .unwrap()[0]
            .content,
        "likes tea"
    );
    assert_eq!(
        dst.state_get("bot", "mood").await.unwrap().unwrap().value,
        serde_json::json!("happy")
    );

    // The backup carries a manifest with matching counts.
    let manifest: BackupManifest =
        serde_json::from_slice(&std::fs::read(backup_dir.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest.format_version, BACKUP_FORMAT_VERSION);
    assert_eq!(manifest.counts.episodic_events, 1);
    assert_eq!(manifest.counts.memories, 1);
    assert!(!manifest.artifacts.is_empty());
}

#[tokio::test]
async fn restore_rejects_corrupted_backup() {
    let dir = tempfile::tempdir().unwrap();
    let backup_dir = dir.path().join("backup");

    let src = EcphoriaEngine::new(inmem_config()).await.unwrap();
    src.ingest(vec![Event::new("src", "e", serde_json::json!({"x": 1}))])
        .await
        .unwrap();
    src.backup(&backup_dir).await.unwrap();

    // Tamper with the state backup after the manifest was written.
    std::fs::write(backup_dir.join("state.db"), b"corrupted").unwrap();

    let dst = EcphoriaEngine::new(inmem_config()).await.unwrap();
    let err = dst.restore_from_backup(&backup_dir).await.unwrap_err();
    assert!(
        err.to_string().contains("integrity check failed"),
        "corrupted backup must be rejected, got: {err}"
    );
}

#[tokio::test]
async fn concurrent_tenant_ingest_does_not_cross_tag() {
    let engine = Arc::new(EcphoriaEngine::new(inmem_config()).await.unwrap());
    let (e1, e2) = (engine.clone(), engine.clone());
    let h1 = tokio::spawn(async move {
        for i in 0..50 {
            e1.ingest_for_tenant(
                vec![Event::new("a", "e", serde_json::json!({ "n": i }))],
                &crate::config::TenantContext::new("tenant-a"),
            )
            .await
            .unwrap();
        }
    });
    let h2 = tokio::spawn(async move {
        for i in 0..50 {
            e2.ingest_for_tenant(
                vec![Event::new("b", "e", serde_json::json!({ "n": i }))],
                &crate::config::TenantContext::new("tenant-b"),
            )
            .await
            .unwrap();
        }
    });
    h1.await.unwrap();
    h2.await.unwrap();

    // Each tenant sees EXACTLY its own 50 events — no cross-tagging under concurrency.
    let a = engine
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-a")
        .await
        .unwrap();
    assert_eq!(a[0]["c"], "50");
    let b = engine
        .query_sql_for_tenant("SELECT count(*)::VARCHAR AS c FROM episodic", "tenant-b")
        .await
        .unwrap();
    assert_eq!(b[0]["c"], "50");
}

#[tokio::test]
async fn ecphoria_state_sql_function_is_tenant_scoped() {
    let engine = EcphoriaEngine::new(inmem_config()).await.unwrap();
    engine
        .state_set_for_tenant("tenant-a", "bot", "secret", serde_json::json!("a-value"))
        .await
        .unwrap();

    // tenant-b querying the same agent/key via ecphoria_state() sees nothing.
    let rows = engine
        .query_sql_for_tenant("SELECT * FROM ecphoria_state('bot', 'secret')", "tenant-b")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "ecphoria_state() leaked tenant-a state to tenant-b!"
    );

    // tenant-a sees its own.
    let rows = engine
        .query_sql_for_tenant("SELECT * FROM ecphoria_state('bot', 'secret')", "tenant-a")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}
