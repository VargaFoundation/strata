# LoCoMo evaluation — reproducible baseline

Strata ships a LoCoMo-style harness (`crates/strata-core/examples/locomo_eval.rs`) and a converter
for the real public datasets (`crates/strata-core/examples/locomo_convert.rs`). This page records
**reproducible** runs on the real LoCoMo dataset (snap-research/locomo — 10 conversations, 1986 QA).

We publish the **repro recipe** and the numbers *our exact config* produces — **not** a leaderboard
claim. The numbers below are a deliberate *conservative floor* (see Caveats); treat them as a
regression baseline and a starting point, not as Strata's ceiling.

## Reproduce

**Turnkey (no API key)** — the fastest path uses the logged-in `claude` CLI as answerer + judge,
Ollama for embeddings, and downloads the dataset for you:

```bash
make bench-smoke   # validate the whole pipeline in minutes (1 conversation, 5 QA)
make bench         # full 10-conversation overnight run → /tmp/strata-bench/results-<ts>.txt
```

See [ops/bench/README.md](../ops/bench/README.md) for knobs (`CONVS`, `EXTRACTION`, `EVAL_MODEL`, …)
and the self-judge-bias caveat. The manual, provider-agnostic recipe below produces the same metrics:

```bash
# 1. Convert the real dataset into the harness schema
curl -sL https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json -o locomo10.json
cargo run -p strata-core --example locomo_convert -- locomo10.json > locomo.json

# 2. Retrieval metrics — full, hybrid (BM25 + vector) + graph expansion + auto-graph
#    (~11 min on a laptop CPU with Ollama nomic-embed-text)
LOCOMO_PATH=locomo.json \
  STRATA_EMBEDDING__PROVIDER=ollama STRATA_EMBEDDING__MODEL=nomic-embed-text \
  STRATA_COGNITION__GRAPH_EXPANSION=1 STRATA_COGNITION__AUTO_GRAPH=1 \
  cargo run -p strata-core --example locomo_eval

# 3. End-to-end QA accuracy (token-F1): add an answerer (+ optional LLM reranker)
#    NB: the LLM reranker is ~140 s/query on a local 7B model — use a small slice
LOCOMO_PATH=locomo_slice.json \
  STRATA_EMBEDDING__PROVIDER=ollama STRATA_EMBEDDING__MODEL=nomic-embed-text \
  STRATA_RERANK__PROVIDER=llm STRATA_RERANK__BACKEND=ollama STRATA_RERANK__MODEL=glm-4.7-flash:latest \
  STRATA_EVAL__PROVIDER=ollama STRATA_EVAL__MODEL=glm-4.7-flash:latest \
  STRATA_COGNITION__GRAPH_EXPANSION=1 STRATA_COGNITION__AUTO_GRAPH=1 \
  cargo run -p strata-core --example locomo_eval
```

### With Claude (Anthropic) — extraction + answerer/judge

The harness also supports Claude for the LLM roles (fact extraction at ingest, the QA answerer, and
the judge). Set your Anthropic API key and pick a model. Use a cheaper model for the high-volume
extraction and a stronger one for the judge if you like.

```bash
export STRATA_EMBEDDING__ANTHROPIC_API_KEY=sk-ant-...
LOCOMO_PATH=locomo.json \
  STRATA_EMBEDDING__PROVIDER=ollama STRATA_EMBEDDING__MODEL=nomic-embed-text \
  STRATA_COGNITION__EXTRACTION=llm \
  STRATA_COGNITION__EXTRACTION_PROVIDER=anthropic STRATA_COGNITION__EXTRACTION_MODEL=claude-haiku-4-5 \
  STRATA_EVAL__PROVIDER=anthropic STRATA_EVAL__MODEL=claude-sonnet-5 \
  STRATA_COGNITION__GRAPH_EXPANSION=1 STRATA_COGNITION__AUTO_GRAPH=1 \
  cargo run -p strata-core --example locomo_eval
```

For a fast, high-quality reranker instead of the ~140 s/query LLM reranker, build with the local
cross-encoder: `--features rerank-local` + `STRATA_RERANK__PROVIDER=cross_encoder`.

## Results

Run 2026-06-30 · Ollama `nomic-embed-text` (768-d) + `glm-4.7-flash` · laptop CPU · `extraction=none`.

### Retrieval — full 1986 QA, hybrid + graph expansion + auto-graph

| category | n | recall@1 | recall@3 | recall@5 | MRR |
|---|---|---|---|---|---|
| **overall** | 1986 | 9.0% | 17.8% | **21.7%** | 0.140 |
| cat 1 | 282 | 1.8% | 3.5% | 4.6% | 0.032 |
| cat 2 | 321 | 1.6% | 2.5% | 4.0% | 0.023 |
| cat 3 | 96 | 4.2% | 4.2% | 6.2% | 0.047 |
| cat 4 | 841 | 12.2% | 25.3% | 30.1% | 0.194 |
| cat 5 | 446 | 13.7% | 26.7% | 32.5% | 0.209 |

Latency: ingest p50/p95 = 34 / 44 ms · query p50/p95 = 205 / 366 ms (query includes the Ollama
embedding round-trip).

### End-to-end QA accuracy — 12-QA slice, full pipeline (+ LLM rerank + QA answerer)

| metric | value |
|---|---|
| QA-F1 (token-F1 vs gold) | **27.4%** |
| recall@5 | 16.7% |
| query p50 | ~140 s (dominated by the LLM reranker over ~50 candidates) |

## Caveats — why these are a floor, not a leaderboard number

1. **Raw turns, no LLM fact extraction.** This run stores each conversation turn verbatim
   (`extraction=none`). Mem0/Zep extract *atomic facts* at ingest, which both compresses and
   normalizes the text — the single biggest lever. Strata's `extraction=llm` path exists, but a
   full-dataset LLM-extraction run is ~5882 LLM calls (hours) and was not run here.
2. **Strict metrics.** `recall` = the gold answer appearing as a substring of a retrieved memory;
   `QA-F1` = bag-of-words token-F1. Published leaderboards (~66% Mem0 / ~68% Zep) use an **LLM
   judge** over generated answers, which is more lenient. Substring/token-F1 under-count
   correct-but-reworded answers — LoCoMo answers are reformatted (e.g. gold `7 May 2023` vs a
   memory saying `May 7th`).
3. **Small local model + tiny QA slice.** `glm-4.7-flash` on CPU is a weak answerer next to the
   GPT-4o used in published runs, and the QA slice is 12 questions (high variance).
4. **LLM reranker latency.** Reranking ~50 candidates with a local 7B+ model is ~140 s/query —
   impractical for production. The cross-encoder reranker (feature-gated, planned) is the
   production answer; the LLM reranker is the zero-dependency baseline.

## What moves these numbers (in priority order)

- **LLM fact extraction at ingest** (`extraction=llm`) — atomic, normalized facts instead of raw turns.
- **A cross-encoder reranker** (vs the LLM baseline) — relevance gain without the latency.
- **An LLM judge** for QA scoring (vs token-F1) — the metric that is actually comparable to the leaderboards.
- A stronger answerer/embedding model than a CPU-hosted 7B.

## Harness knobs (all env-toggled, so the levers above are runnable)

| env | effect |
|---|---|
| `STRATA_EMBEDDING__PROVIDER` / `__MODEL` | hybrid retrieval (BM25 + vector) |
| `STRATA_RERANK__PROVIDER=llm` / `__MODEL` | second-stage LLM reranking (slow — see latency note) |
| `STRATA_COGNITION__GRAPH_EXPANSION=1` | query-time knowledge-graph expansion |
| `STRATA_COGNITION__AUTO_GRAPH=1` | deterministic auto-population of graph edges from each memory |
| `STRATA_COGNITION__EXTRACTION=llm` (+ `__EXTRACTION_PROVIDER` / `__EXTRACTION_MODEL`) | **LLM fact extraction** at ingest — distils each turn into atomic facts |
| `STRATA_EVAL__PROVIDER` / `__MODEL` | end-to-end QA answerer (token-F1) |
| `STRATA_EVAL__JUDGE=1` | add an **LLM judge** (`QA-judge` column) — the leniency-matched, leaderboard-comparable metric |

**Extraction sanity check** (Ollama `glm-4.7-flash`): on a 3-turn / 4-QA micro set,
`extraction=llm` split the 3 multi-fact turns into **7 atomic memories**, and the run reported
`QA-F1 = QA-judge = 75%`. This confirms the extraction + judge paths end-to-end; the *scaled* lift on
full LoCoMo (~5882 turns ⇒ ~5882 extraction LLM calls, hours) is the owner's run to make — the
harness is now ready for it.
