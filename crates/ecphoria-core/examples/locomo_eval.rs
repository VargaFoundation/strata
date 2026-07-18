//! LoCoMo-style memory evaluation harness.
//!
//! Measures the cognition layer the way the agent-memory market is benchmarked: ingest a
//! multi-session "conversation", then for each question retrieve memories and report:
//!   - **retrieval quality** — recall@{1,3,5} + MRR of the answer-bearing memory (deterministic,
//!     no LLM needed), broken down **per category** (single_hop, multi_hop, temporal, …);
//!   - **end-to-end QA accuracy** — optional: generate an answer from the retrieved facts with a
//!     configured LLM and score it with **token-F1** against the gold answer (the number
//!     comparable to the published Mem0 ~66% / Zep 63.8% leaderboards);
//!   - **latency** — ingest/query p50/p95.
//!
//! Run it (synthetic dataset, offline, retrieval metrics only):
//!   cargo run -p ecphoria-core --example locomo_eval
//!
//! Real dataset + hybrid retrieval + LLM reranking:
//!   LOCOMO_PATH=examples/locomo-sample.json \
//!   ECPHORIA_EMBEDDING__PROVIDER=ollama \
//!   ECPHORIA_RERANK__PROVIDER=llm ECPHORIA_RERANK__BACKEND=ollama ECPHORIA_RERANK__MODEL=llama3.2 \
//!   cargo run -p ecphoria-core --example locomo_eval
//!
//! Add end-to-end QA accuracy (token-F1) by configuring an answerer model:
//!   ECPHORIA_EVAL__PROVIDER=ollama ECPHORIA_EVAL__MODEL=llama3.2  (reuses the EMBEDDING url/key envs)
//!
//! Env overrides recognized by this harness: ECPHORIA_EMBEDDING__{PROVIDER,MODEL,OLLAMA_URL,
//! OPENAI_API_KEY}, ECPHORIA_RERANK__{PROVIDER,BACKEND,MODEL}, ECPHORIA_EVAL__{PROVIDER,MODEL}.
//!
//! Dataset schema (JSON): an array of conversations, each:
//!   { "user": "alice",
//!     "turns": ["...session text...", "..."],
//!     "qa": [ { "question": "...", "expected": "gold answer substring", "category": "temporal" } ] }
//! `category` is optional. Convert a real LoCoMo/LongMemEval export into this shape to reproduce
//! leaderboard-style numbers.

use std::collections::HashMap;
use std::sync::Arc;

use ecphoria_core::llm::CompletionProvider;
use ecphoria_core::memory::cognition::MemoryScope;
use ecphoria_core::memory::semantic::{SemanticEntry, SemanticStore};
use ecphoria_core::{CoreConfig, EcphoriaEngine};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
struct Qa {
    question: String,
    expected: String,
    /// Optional LoCoMo/LongMemEval category (single_hop, multi_hop, temporal, …).
    #[serde(default)]
    category: Option<String>,
}

#[derive(Deserialize)]
struct Conversation {
    user: String,
    turns: Vec<String>,
    qa: Vec<Qa>,
}

/// Per-question outcome, aggregated overall and per category.
struct Record {
    category: String,
    /// 1-indexed rank of the first answer-bearing memory (None = not in top-K).
    rank: Option<usize>,
    /// token-F1 of the generated answer vs gold (None when QA mode is off).
    f1: Option<f64>,
    /// LLM-judge verdict (Some(true) = judged correct) when ECPHORIA_EVAL__JUDGE is set.
    judge: Option<bool>,
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
                category: None,
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

/// Apply the `ECPHORIA_*` env overrides this harness understands onto an in-memory config, so the
/// documented `ECPHORIA_EMBEDDING__…` / `ECPHORIA_RERANK__…` knobs take effect (the example builds a
/// `CoreConfig::default()` directly rather than going through the server's env-layered loader).
fn apply_env(config: &mut CoreConfig) {
    let set = |dst: &mut String, key: &str| {
        if let Ok(v) = std::env::var(key) {
            *dst = v;
        }
    };
    set(
        &mut config.embedding.provider,
        "ECPHORIA_EMBEDDING__PROVIDER",
    );
    set(&mut config.embedding.model, "ECPHORIA_EMBEDDING__MODEL");
    set(
        &mut config.embedding.ollama_url,
        "ECPHORIA_EMBEDDING__OLLAMA_URL",
    );
    set(
        &mut config.embedding.openai_api_key,
        "ECPHORIA_EMBEDDING__OPENAI_API_KEY",
    );
    set(
        &mut config.embedding.anthropic_api_key,
        "ECPHORIA_EMBEDDING__ANTHROPIC_API_KEY",
    );
    // Asymmetric retrieval task prefixes. Unset → auto-derived from the model (nomic/e5). Set to a
    // value (incl. empty "") to force it — used to A/B the prefix fix on an identical binary.
    if let Ok(v) = std::env::var("ECPHORIA_EMBEDDING__QUERY_PREFIX") {
        config.embedding.query_prefix = Some(v);
    }
    if let Ok(v) = std::env::var("ECPHORIA_EMBEDDING__DOCUMENT_PREFIX") {
        config.embedding.document_prefix = Some(v);
    }
    set(&mut config.rerank.provider, "ECPHORIA_RERANK__PROVIDER");
    set(&mut config.rerank.backend, "ECPHORIA_RERANK__BACKEND");
    set(&mut config.rerank.model, "ECPHORIA_RERANK__MODEL");
    // LLM fact extraction at ingest (the biggest recall lever): "none" or "llm".
    set(
        &mut config.memory.cognition.extraction,
        "ECPHORIA_COGNITION__EXTRACTION",
    );
    set(
        &mut config.memory.cognition.extraction_provider,
        "ECPHORIA_COGNITION__EXTRACTION_PROVIDER",
    );
    set(
        &mut config.memory.cognition.extraction_model,
        "ECPHORIA_COGNITION__EXTRACTION_MODEL",
    );
    let flag = |key: &str| matches!(std::env::var(key).as_deref(), Ok("1") | Ok("true"));
    if std::env::var("ECPHORIA_COGNITION__GRAPH_EXPANSION").is_ok() {
        config.memory.cognition.graph_expansion = flag("ECPHORIA_COGNITION__GRAPH_EXPANSION");
    }
    if std::env::var("ECPHORIA_COGNITION__AUTO_GRAPH").is_ok() {
        config.memory.cognition.auto_graph = flag("ECPHORIA_COGNITION__AUTO_GRAPH");
    }
    // Retrieval widths (for A/B: narrow legacy = SCAN_CAP=512 POOL=15, wide default = 2048/50).
    let set_usize = |dst: &mut usize, key: &str| {
        if let Ok(v) = std::env::var(key) {
            if let Ok(n) = v.parse() {
                *dst = n;
            }
        }
    };
    set_usize(
        &mut config.memory.cognition.retrieval_scan_cap,
        "ECPHORIA_COGNITION__RETRIEVAL_SCAN_CAP",
    );
    set_usize(
        &mut config.memory.cognition.retrieval_pool,
        "ECPHORIA_COGNITION__RETRIEVAL_POOL",
    );
    // Embedding vector dimension — must match the model (nomic=768, bge-m3=1024, e5-large=1024,
    // text-embedding-3-large=3072) so the index/query dimensions line up.
    set_usize(
        &mut config.embedding.dimension,
        "ECPHORIA_EMBEDDING__DIMENSION",
    );
    // Retrieval blend weights (for A/B: 0/0 = pure relevance vs the 0.3/0.2 default).
    let set_f32 = |dst: &mut f32, key: &str| {
        if let Ok(v) = std::env::var(key) {
            if let Ok(n) = v.parse() {
                *dst = n;
            }
        }
    };
    set_f32(
        &mut config.memory.cognition.retrieval_importance_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_IMPORTANCE_WEIGHT",
    );
    set_f32(
        &mut config.memory.cognition.retrieval_recency_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_RECENCY_WEIGHT",
    );
    // Weighted-RRF arm weights (for A/B: default 1/1 = equal, e.g. vector=2 lexical=1 favours vector).
    set_f32(
        &mut config.memory.cognition.retrieval_vector_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_VECTOR_WEIGHT",
    );
    set_f32(
        &mut config.memory.cognition.retrieval_lexical_weight,
        "ECPHORIA_COGNITION__RETRIEVAL_LEXICAL_WEIGHT",
    );
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

/// Build the optional QA "answerer" model used for end-to-end QA-accuracy. Enabled by
/// `ECPHORIA_EVAL__PROVIDER` (ollama|openai|anthropic) + `ECPHORIA_EVAL__MODEL`; reuses the embedding
/// URL/key envs.
fn build_answerer() -> Option<Arc<dyn CompletionProvider>> {
    let provider = std::env::var("ECPHORIA_EVAL__PROVIDER").ok()?;
    let model = std::env::var("ECPHORIA_EVAL__MODEL").unwrap_or_else(|_| "llama3.2".into());
    match provider.as_str() {
        "ollama" => {
            let url = std::env::var("ECPHORIA_EMBEDDING__OLLAMA_URL")
                .unwrap_or_else(|_| "http://localhost:11434".into());
            Some(Arc::new(ecphoria_core::llm::ollama::OllamaCompletion::new(
                url, model,
            )))
        }
        "openai" => {
            let key = std::env::var("ECPHORIA_EMBEDDING__OPENAI_API_KEY").unwrap_or_default();
            if key.is_empty() {
                eprintln!("ECPHORIA_EVAL__PROVIDER=openai but no ECPHORIA_EMBEDDING__OPENAI_API_KEY — QA mode off");
                return None;
            }
            Some(Arc::new(ecphoria_core::llm::openai::OpenAiCompletion::new(
                key, model,
            )))
        }
        "anthropic" => {
            let key = std::env::var("ECPHORIA_EMBEDDING__ANTHROPIC_API_KEY").unwrap_or_default();
            if key.is_empty() {
                eprintln!("ECPHORIA_EVAL__PROVIDER=anthropic but no ECPHORIA_EMBEDDING__ANTHROPIC_API_KEY — QA mode off");
                return None;
            }
            Some(Arc::new(
                ecphoria_core::llm::anthropic::AnthropicCompletion::new(key, model),
            ))
        }
        "claude-cli" => Some(Arc::new(
            ecphoria_core::llm::claude_cli::ClaudeCliCompletion::new(model),
        )),
        other => {
            eprintln!("unknown ECPHORIA_EVAL__PROVIDER={other:?} — QA mode off");
            None
        }
    }
}

fn tokenize(s: &str) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// Bag-of-words token F1 (SQuAD-style) between a predicted answer and the gold string.
fn token_f1(pred: &str, gold: &str) -> f64 {
    let p = tokenize(pred);
    let g = tokenize(gold);
    if p.is_empty() || g.is_empty() {
        return if p.is_empty() && g.is_empty() {
            1.0
        } else {
            0.0
        };
    }
    let mut gold_counts: HashMap<&str, i32> = HashMap::new();
    for t in &g {
        *gold_counts.entry(t.as_str()).or_insert(0) += 1;
    }
    let mut pred_counts: HashMap<&str, i32> = HashMap::new();
    for t in &p {
        *pred_counts.entry(t.as_str()).or_insert(0) += 1;
    }
    let common: i32 = pred_counts
        .iter()
        .map(|(t, pc)| (*pc).min(*gold_counts.get(t).unwrap_or(&0)))
        .sum();
    if common == 0 {
        return 0.0;
    }
    let precision = common as f64 / p.len() as f64;
    let recall = common as f64 / g.len() as f64;
    2.0 * precision * recall / (precision + recall)
}

/// Ask the answerer model to answer `question` using only the retrieved `facts`.
async fn answer_question(
    model: &dyn CompletionProvider,
    question: &str,
    facts: &[String],
) -> Option<String> {
    let mut user = String::from("Facts:\n");
    for f in facts {
        user.push_str("- ");
        user.push_str(f);
        user.push('\n');
    }
    user.push_str("\nQuestion: ");
    user.push_str(question);
    user.push_str(
        "\nAnswer in as few words as possible using ONLY the facts above. If the facts do not \
         contain the answer, reply \"unknown\".",
    );
    model
        .complete("You are a precise question-answering assistant.", &user)
        .await
        .ok()
}

/// LLM judge: does `pred` convey the same information as the reference `gold` answer? Returns the
/// leaderboard-style binary verdict (more lenient than token-F1, which penalizes rewording).
async fn judge_correct(
    model: &dyn CompletionProvider,
    question: &str,
    gold: &str,
    pred: &str,
) -> Option<bool> {
    let user = format!(
        "Question: {question}\nReference answer: {gold}\nCandidate answer: {pred}\n\nDoes the \
         candidate convey the same information as the reference (ignore wording and formatting)? \
         Reply with only \"yes\" or \"no\"."
    );
    let reply = model
        .complete("You are a strict grader.", &user)
        .await
        .ok()?;
    Some(reply.trim().to_lowercase().starts_with("yes"))
}

/// Print one metrics line for a set of question records.
fn report(label: &str, recs: &[&Record]) {
    let n = recs.len().max(1) as f64;
    let recall_at = |k: usize| {
        recs.iter()
            .filter(|r| matches!(r.rank, Some(rk) if rk <= k))
            .count()
    };
    let mrr: f64 = recs
        .iter()
        .map(|r| r.rank.map(|rk| 1.0 / rk as f64).unwrap_or(0.0))
        .sum::<f64>()
        / n;
    let f1s: Vec<f64> = recs.iter().filter_map(|r| r.f1).collect();
    print!(
        "{label:<14} n={:<4} R@1={:>5.1}% R@3={:>5.1}% R@5={:>5.1}% MRR={:.3}",
        recs.len(),
        100.0 * recall_at(1) as f64 / n,
        100.0 * recall_at(3) as f64 / n,
        100.0 * recall_at(5) as f64 / n,
        mrr,
    );
    if !f1s.is_empty() {
        print!(
            "  QA-F1={:>5.1}%",
            100.0 * f1s.iter().sum::<f64>() / f1s.len() as f64
        );
    }
    let judged: Vec<bool> = recs.iter().filter_map(|r| r.judge).collect();
    if !judged.is_empty() {
        let correct = judged.iter().filter(|&&v| v).count();
        print!(
            "  QA-judge={:>5.1}%",
            100.0 * correct as f64 / judged.len() as f64
        );
    }
    println!();
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
    apply_env(&mut config);
    let engine = EcphoriaEngine::new(config).await.expect("engine");
    let answerer = build_answerer();
    let judge_enabled = matches!(
        std::env::var("ECPHORIA_EVAL__JUDGE").as_deref(),
        Ok("1") | Ok("true")
    );

    let dataset = load_dataset();
    // Retrieve a deeper top-K so we can report recall@{1,3,5} + MRR from a single search.
    const K: usize = 10;
    // Facts fed to the answerer for the QA-accuracy generation step.
    const QA_FACTS: usize = 8;
    let mut stored = 0usize;
    let mut records: Vec<Record> = Vec::new();
    let mut ingest_ms: Vec<f64> = Vec::new();
    let mut query_ms: Vec<f64> = Vec::new();

    // Retrieval engine under test: "ecphoria" (full cognition — hybrid BM25+vector RRF, graph
    // expansion, rerank, LLM extraction) or "naive" (pure top-k vector over the raw turns: the
    // honest RAG floor, so the delta measures exactly what Ecphoria's retrieval stack adds). Both
    // engines flow through the *same* metrics + answerer + judge below.
    let engine_mode = std::env::var("ECPHORIA_BENCH__ENGINE").unwrap_or_else(|_| "ecphoria".into());
    let naive = engine_mode == "naive";
    let provider = engine.config().embedding.provider.clone();
    if naive && (provider.is_empty() || provider == "none") {
        eprintln!(
            "ECPHORIA_BENCH__ENGINE=naive needs vectors — set ECPHORIA_EMBEDDING__PROVIDER (ollama|openai)"
        );
        std::process::exit(2);
    }
    let dim = engine.config().embedding.dimension;

    for convo in &dataset {
        let scope = MemoryScope::user(&convo.user);
        // Naive-RAG: a flat per-user vector index of the raw turns — no cognition, no extraction,
        // no lexical/graph/rerank. Same embeddings (incl. the query/document task prefixes).
        let naive_store = if naive {
            Some(SemanticStore::with_dimension(dim).expect("naive vector store"))
        } else {
            None
        };
        for turn in &convo.turns {
            let start = std::time::Instant::now();
            if let Some(store) = &naive_store {
                match engine.embed_document_text(turn).await {
                    Ok(v) => {
                        let _ = store
                            .upsert(&SemanticEntry {
                                id: Uuid::new_v4(),
                                content: turn.clone(),
                                embedding: v,
                                metadata: serde_json::json!({}),
                            })
                            .await;
                        stored += 1;
                    }
                    Err(e) => eprintln!("  naive ingest embed error: {e}"),
                }
            } else {
                let added = engine
                    .memory_remember(turn, &scope)
                    .await
                    .expect("remember");
                stored += added.len();
            }
            ingest_ms.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        for qa in &convo.qa {
            let start = std::time::Instant::now();
            // Ordered retrieved memory texts — both engines reduce to this for scoring.
            let contents: Vec<String> = if let Some(store) = &naive_store {
                match engine.embed_text(&qa.question).await {
                    Ok(qv) => store
                        .search(&qv, K)
                        .await
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.entry.content)
                        .collect(),
                    Err(_) => Vec::new(),
                }
            } else {
                engine
                    .memory_search(&qa.question, &scope, K)
                    .await
                    .expect("search")
                    .into_iter()
                    .map(|h| h.memory.content)
                    .collect()
            };
            query_ms.push(start.elapsed().as_secs_f64() * 1000.0);

            let needle = qa.expected.to_lowercase();
            let rank = contents
                .iter()
                .position(|c| c.to_lowercase().contains(&needle))
                .map(|i| i + 1);
            if rank.is_none() {
                println!("  MISS: q={:?} expected={:?}", qa.question, qa.expected);
            }

            // Optional end-to-end QA: answer from the retrieved facts, score by token-F1 (and, when
            // enabled, an LLM judge — the metric comparable to published leaderboards).
            let (f1, judge) = match &answerer {
                Some(model) => {
                    let facts: Vec<String> = contents.iter().take(QA_FACTS).cloned().collect();
                    match answer_question(model.as_ref(), &qa.question, &facts).await {
                        Some(ans) => {
                            let f1 = token_f1(&ans, &qa.expected);
                            let judge = if judge_enabled {
                                judge_correct(model.as_ref(), &qa.question, &qa.expected, &ans)
                                    .await
                            } else {
                                None
                            };
                            (Some(f1), judge)
                        }
                        None => (Some(0.0), judge_enabled.then_some(false)),
                    }
                }
                None => (None, None),
            };

            records.push(Record {
                category: qa.category.clone().unwrap_or_else(|| "all".into()),
                rank,
                f1,
                judge,
            });
        }
    }

    let pct_of = |v: &mut Vec<f64>, p: f64| {
        if v.is_empty() {
            return 0.0;
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[((p * v.len() as f64) as usize).min(v.len() - 1)]
    };

    println!("\n── LoCoMo-style eval ──────────────────────────────");
    println!("conversations:    {}", dataset.len());
    println!("memories stored:  {stored}");
    println!("questions:        {}\n", records.len());

    // Overall, then a per-category breakdown (skipped when the dataset has no categories).
    report("OVERALL", &records.iter().collect::<Vec<_>>());
    let mut cats: Vec<&str> = Vec::new();
    for r in &records {
        if !cats.contains(&r.category.as_str()) {
            cats.push(r.category.as_str());
        }
    }
    if !(cats.len() == 1 && cats[0] == "all") {
        for cat in cats {
            let subset: Vec<&Record> = records.iter().filter(|r| r.category == cat).collect();
            report(cat, &subset);
        }
    }

    println!(
        "\ningest  p50/p95:  {:.2} / {:.2} ms",
        pct_of(&mut ingest_ms, 0.50),
        pct_of(&mut ingest_ms, 0.95)
    );
    println!(
        "query   p50/p95:  {:.2} / {:.2} ms",
        pct_of(&mut query_ms, 0.50),
        pct_of(&mut query_ms, 0.95)
    );
    let provider = engine.config().embedding.provider.as_str();
    println!("engine:           {engine_mode}");
    println!(
        "mode:             {}",
        if naive {
            "naive-RAG (pure top-k vector, raw turns)"
        } else if !provider.is_empty() && provider != "none" {
            "hybrid (BM25 + vector)"
        } else {
            "lexical (BM25 only — set ECPHORIA_EMBEDDING__PROVIDER for hybrid)"
        }
    );
    println!(
        "rerank:           {}",
        match engine.config().rerank.provider.as_str() {
            "none" | "" => "off",
            p => p,
        }
    );
    println!(
        "QA accuracy:      {}",
        if answerer.is_some() {
            "on (token-F1)"
        } else {
            "off (set ECPHORIA_EVAL__PROVIDER + ECPHORIA_EVAL__MODEL)"
        }
    );
}
