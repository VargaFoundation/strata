# LoCoMo evaluation — reproducible baseline

Strata ships a LoCoMo-style harness (`crates/strata-core/examples/locomo_eval.rs`) and a converter
for the real public datasets (`crates/strata-core/examples/locomo_convert.rs`). This page records
**reproducible** runs on the real LoCoMo dataset (snap-research/locomo — 10 conversations, 1986 QA).

We publish the **repro recipe** and the numbers *our exact config* produces — **not** a leaderboard
claim. The numbers below are a deliberate *conservative floor* (see Caveats); treat them as a
regression baseline and a starting point, not as Strata's ceiling.

## Reproduce

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
