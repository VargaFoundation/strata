//! LoCoMo-style memory retrieval evaluation harness.
//!
//! Measures the cognition layer the way the agent-memory market is benchmarked: ingest a
//! multi-session "conversation", then answer questions by retrieving memories and checking
//! whether the answer-bearing memory was recalled (recall@k), plus retrieval latency.
//!
//! Run it (synthetic dataset, offline):
//!   cargo run -p strata-core --example locomo_eval
//!
//! Run it on a REAL dataset with an embedding provider (true hybrid retrieval, closer to the
//! published LoCoMo setups):
//!   LOCOMO_PATH=examples/locomo-sample.json \
//!   STRATA_EMBEDDING__PROVIDER=ollama \
//!   cargo run -p strata-core --example locomo_eval
//!
//! Dataset schema (JSON): an array of conversations, each:
//!   { "user": "alice",
//!     "turns": ["...session text...", "..."],
//!     "qa": [ { "question": "...", "expected": "substring of the answer-bearing memory" } ] }
//! Convert a real LoCoMo export into this shape to reproduce leaderboard-style numbers.

use serde::Deserialize;
use strata_core::memory::cognition::MemoryScope;
use strata_core::{CoreConfig, StrataEngine};

#[derive(Deserialize)]
struct Qa {
    question: String,
    expected: String,
}

#[derive(Deserialize)]
struct Conversation {
    user: String,
    turns: Vec<String>,
    qa: Vec<Qa>,
}

fn embedded_dataset() -> Vec<Conversation> {
    let c = |user: &str, turns: &[&str], qa: &[(&str, &str)]| Conversation {
        user: user.into(),
        turns: turns.iter().map(|s| s.to_string()).collect(),
        qa: qa
            .iter()
            .map(|(q, e)| Qa {
                question: q.to_string(),
                expected: e.to_string(),
            })
            .collect(),
    };
    vec![
        c(
            "alice",
            &[
                "Alice mentioned she works as a data scientist at Acme Corp.",
                "Alice said her favorite programming language is Rust.",
                "Alice is planning a trip to Japan next spring.",
                "Alice has a golden retriever named Max.",
                "Alice recently moved from Berlin to Amsterdam.",
            ],
            &[
                ("What does Alice do for work?", "data scientist"),
                ("Where is Alice traveling next spring?", "Japan"),
                ("What is the name of Alice's dog?", "Max"),
                ("Which city does Alice live in now?", "Amsterdam"),
            ],
        ),
        c(
            "bob",
            &[
                "Bob is a high school chemistry teacher.",
                "Bob plays the saxophone in a jazz band on weekends.",
                "Bob is allergic to peanuts.",
                "Bob's daughter Mia just started college in Boston.",
            ],
            &[
                ("What instrument does Bob play?", "saxophone"),
                ("What is Bob allergic to?", "peanuts"),
                ("Where did Bob's daughter start college?", "Boston"),
            ],
        ),
    ]
}

fn load_dataset() -> Vec<Conversation> {
    if let Ok(path) = std::env::var("LOCOMO_PATH") {
        match std::fs::read_to_string(&path) {
            Ok(s) => match serde_json::from_str::<Vec<Conversation>>(&s) {
                Ok(d) => {
                    println!("loaded {} conversations from {path}", d.len());
                    return d;
                }
                Err(e) => eprintln!("failed to parse {path}: {e} — using synthetic dataset"),
            },
            Err(e) => eprintln!("failed to read {path}: {e} — using synthetic dataset"),
        }
    }
    embedded_dataset()
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(run());
}

async fn run() {
    // In-memory stores so the harness is self-contained.
    let mut config = CoreConfig::default();
    config.memory.episodic.db_path = ":memory:".into();
    config.memory.state.db_path = ":memory:".into();
    config.memory.cognition.db_path = ":memory:".into();
    let engine = StrataEngine::new(config).await.expect("engine");

    let dataset = load_dataset();
    // Retrieve a deeper top-K so we can report recall@{1,3,5} + MRR from a single search.
    const K: usize = 10;
    let mut total = 0usize;
    let mut stored = 0usize;
    // 1-indexed rank of the first answer-bearing memory in the results (None = not in top-K).
    let mut ranks: Vec<Option<usize>> = Vec::new();
    let mut ingest_ms: Vec<f64> = Vec::new();
    let mut query_ms: Vec<f64> = Vec::new();

    for convo in &dataset {
        let scope = MemoryScope::user(&convo.user);
        for turn in &convo.turns {
            let start = std::time::Instant::now();
            let added = engine
                .memory_remember(turn, &scope)
                .await
                .expect("remember");
            ingest_ms.push(start.elapsed().as_secs_f64() * 1000.0);
            stored += added.len();
        }
        for qa in &convo.qa {
            let start = std::time::Instant::now();
            let results = engine
                .memory_search(&qa.question, &scope, K)
                .await
                .expect("search");
            query_ms.push(start.elapsed().as_secs_f64() * 1000.0);

            let needle = qa.expected.to_lowercase();
            let rank = results
                .iter()
                .position(|h| h.memory.content.to_lowercase().contains(&needle))
                .map(|i| i + 1);
            if rank.is_none() {
                println!("  MISS: q={:?} expected={:?}", qa.question, qa.expected);
            }
            ranks.push(rank);
            total += 1;
        }
    }

    let denom = total.max(1) as f64;
    let recall_at = |n: usize| {
        ranks
            .iter()
            .filter(|r| matches!(r, Some(rk) if *rk <= n))
            .count()
    };
    let pct_of = |v: &mut Vec<f64>, p: f64| {
        if v.is_empty() {
            return 0.0;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[((p * v.len() as f64) as usize).min(v.len() - 1)]
    };
    // Mean Reciprocal Rank — the standard rank-aware retrieval metric.
    let mrr = ranks
        .iter()
        .map(|r| r.map(|rk| 1.0 / rk as f64).unwrap_or(0.0))
        .sum::<f64>()
        / denom;

    println!("\n── LoCoMo-style eval ──────────────────────────────");
    println!("conversations:    {}", dataset.len());
    println!("memories stored:  {stored}");
    println!("questions:        {total}");
    println!(
        "recall@1:         {}/{total} = {:.1}%",
        recall_at(1),
        100.0 * recall_at(1) as f64 / denom
    );
    println!(
        "recall@3:         {}/{total} = {:.1}%",
        recall_at(3),
        100.0 * recall_at(3) as f64 / denom
    );
    println!(
        "recall@5:         {}/{total} = {:.1}%",
        recall_at(5),
        100.0 * recall_at(5) as f64 / denom
    );
    println!("MRR:              {mrr:.3}");
    println!(
        "ingest  p50/p95:  {:.2} / {:.2} ms",
        pct_of(&mut ingest_ms, 0.50),
        pct_of(&mut ingest_ms, 0.95)
    );
    println!(
        "query   p50/p95:  {:.2} / {:.2} ms",
        pct_of(&mut query_ms, 0.50),
        pct_of(&mut query_ms, 0.95)
    );
    println!(
        "mode:             {}",
        if engine.semantic_count() > 0 {
            "hybrid (BM25 + vector)"
        } else {
            "lexical (BM25 only — set STRATA_EMBEDDING__PROVIDER for hybrid)"
        }
    );
}
