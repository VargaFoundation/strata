# LoCoMo benchmark with Claude (via the CLI)

Turnkey runner for the LoCoMo memory benchmark, using **Claude through the logged-in `claude` CLI**
(no API key needed) as the answerer + judge. Retrieval embeddings run locally on Ollama; the reranker
is the local ONNX cross-encoder.

## Prerequisites
- `claude` CLI logged in (subscription/OAuth) with the chosen model available.
- **Ollama** running at `:11434` with the embedding model (`ollama pull nomic-embed-text`).
- First run downloads the LoCoMo dataset and (for the reranker) the bge-reranker ONNX model.

## Run

```bash
# 1) Smoke-test the whole setup in a few minutes (1 conversation, 5 QA, no LLM extraction):
CONVS=1 QA_LIMIT=5 EXTRACTION=none ops/bench/run-locomo-claude.sh

# 2) Full overnight run (all 10 conversations, extraction=llm). Detach + watch the log:
nohup ops/bench/run-locomo-claude.sh > /tmp/ecphoria-bench/run.log 2>&1 &
tail -f /tmp/ecphoria-bench/run.log
```

Results (the per-category table) are teed to `/tmp/ecphoria-bench/results-<timestamp>.txt`.

## Knobs (env vars)
| Var | Default | Meaning |
|-----|---------|---------|
| `CONVS` | `10` | Conversations to run (10 = full LoCoMo). |
| `QA_LIMIT` | `0` | Cap questions per conversation (0 = all). Use for smoke runs. |
| `EXTRACTION` | `llm` | `none` (raw turns) or `llm` (extract atomic facts at ingest — **the biggest recall lever**). |
| `EXTRACTION_PROVIDER` | `ollama` | `ollama` (fast, local `glm`) · `claude-cli` (best, but ~1 spawn/turn = hours) · `anthropic` (needs a key). |
| `EXTRACTION_MODEL` | `glm-4.7-flash:latest` | Extraction model (or a Claude alias when provider is `claude-cli`). |
| `EVAL_MODEL` | `sonnet` | Claude model for the answerer + judge (via the CLI). |
| `EMBED_MODEL` | `nomic-embed-text` | Ollama embedding model. |
| `RERANK` | `cross_encoder` | `cross_encoder` (local ONNX, fast) · `none` · `llm`. |

## Cost / time
- Embeddings + extraction are local (CPU) → minutes to ~an hour for ingest depending on `EXTRACTION`.
- The QA loop spawns `claude` **once per question to answer + once to judge** → ~4k spawns for the
  full 1986-QA set = **several hours**. This is why it's an overnight job.

## Honest caveats (read before quoting a number)
- **Self-judge bias:** the judge is Claude grading Claude's own answers — it tends to be lenient.
  For a publishable number, spot-check a sample by hand or use an independent judge.
- **`extraction=llm` via local `glm`** is weaker than Claude extraction; set
  `EXTRACTION_PROVIDER=claude-cli` (slower) for max quality.
- Substring `recall@k` and token-`F1` under-count reworded-but-correct answers; the LLM-judge column
  is the leaderboard-comparable metric. See `docs/benchmarks-locomo.md`.
