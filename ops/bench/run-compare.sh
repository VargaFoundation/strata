#!/usr/bin/env bash
# Head-to-head LoCoMo comparison: Ecphoria vs a naive-RAG floor vs Mem0 — on the SAME dataset and the
# SAME metrics (recall@{1,3,5}, MRR, optional LLM-judge QA), so the numbers are directly comparable.
#
# All three engines flow through one harness:
#   - ecphoria : full cognition (hybrid BM25+vector RRF, graph expansion, rerank, opt. LLM extraction)
#   - naive  : pure top-k vector over raw turns — the honest RAG floor (same embeddings + prefixes)
#   - mem0   : the Mem0 competitor (ops/bench/mem0_locomo.py), Ollama-local, same metrics
#
# Local + free by default: embeddings + optional fact-extraction on Ollama; the QA answerer + judge
# use the logged-in `claude` CLI (no API key), like run-locomo-claude.sh.
#
# Usage:
#   ops/bench/run-compare.sh                       # retrieval metrics, all engines, full dataset
#   CONVS=1 ENGINES="naive ecphoria" ops/bench/run-compare.sh    # quick smoke (1 convo, 2 engines)
#   JUDGE=1 ops/bench/run-compare.sh               # + end-to-end QA accuracy via the Claude CLI judge
#
# Knobs (env):
#   ENGINES="naive ecphoria mem0"  which engines to run (space-separated)
#   CONVS=10                 conversations (10 = full LoCoMo; start with 1 to validate)
#   QA_LIMIT=0               cap questions per conversation (0 = all)
#   EMBED_MODEL=nomic-embed-text     Ollama embedding model (prefixes auto-applied)
#   OLLAMA_URL=http://localhost:11434
#   EXTRACTION=none          none | llm   (Ecphoria only; llm = atomic facts at ingest — biggest lever)
#   EXTRACTION_MODEL=llama3.1 EXTRACTION_PROVIDER=ollama
#   GRAPH=1                  Ecphoria query-time graph expansion + auto-graph (1/0)
#   JUDGE=0                  1 = generate answers + grade with the Claude CLI (adds QA-F1 / QA-judge)
#   EVAL_MODEL=sonnet        Claude model (via CLI) for the answerer + judge
#   DATA_DIR=/tmp/ecphoria-bench
set -euo pipefail
cd "$(dirname "$0")/../.."

ENGINES="${ENGINES:-naive ecphoria mem0}"
CONVS="${CONVS:-10}"
QA_LIMIT="${QA_LIMIT:-0}"
EMBED_MODEL="${EMBED_MODEL:-nomic-embed-text}"
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
EXTRACTION="${EXTRACTION:-none}"
EXTRACTION_MODEL="${EXTRACTION_MODEL:-llama3.1}"
EXTRACTION_PROVIDER="${EXTRACTION_PROVIDER:-ollama}"
GRAPH="${GRAPH:-1}"
JUDGE="${JUDGE:-0}"
EVAL_MODEL="${EVAL_MODEL:-sonnet}"
DATA_DIR="${DATA_DIR:-/tmp/ecphoria-bench}"
mkdir -p "$DATA_DIR"
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$DATA_DIR/compare-$STAMP.txt"

# ── 1. dataset (download + convert once) ─────────────────────────────────────
if [ ! -f "$DATA_DIR/locomo.json" ]; then
  echo "▶ downloading + converting LoCoMo (snap-research/locomo, 10 convos / 1986 QA)…"
  curl -sL https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json \
    -o "$DATA_DIR/locomo10.json"
  cargo build --release -p ecphoria-core --example locomo_convert
  ./target/release/examples/locomo_convert "$DATA_DIR/locomo10.json" > "$DATA_DIR/locomo.json"
fi
python3 - "$DATA_DIR/locomo.json" "$DATA_DIR/run.json" "$CONVS" "$QA_LIMIT" <<'PY'
import json, sys
src, dst, convs, qa_limit = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
d = json.load(open(src))[:convs]
if qa_limit > 0:
    for c in d: c["qa"] = c.get("qa", [])[:qa_limit]
json.dump(d, open(dst, "w"))
print(f"  run set: {len(d)} conversation(s), {sum(len(c.get('qa', [])) for c in d)} QA")
PY

# ── 2. build the Rust harness ────────────────────────────────────────────────
echo "▶ building locomo_eval…"
cargo build --release -p ecphoria-core --example locomo_eval

# Shared env for the Rust harness.
export ECPHORIA_EMBEDDING__PROVIDER=ollama
export ECPHORIA_EMBEDDING__OLLAMA_URL="$OLLAMA_URL"
export ECPHORIA_EMBEDDING__MODEL="$EMBED_MODEL"
export LOCOMO_PATH="$DATA_DIR/run.json"
if [ "$JUDGE" = "1" ]; then
  export ECPHORIA_EVAL__PROVIDER=claude-cli ECPHORIA_EVAL__MODEL="$EVAL_MODEL" ECPHORIA_EVAL__JUDGE=1
fi

echo "▶ warming the embedding model (keep_alive=-1)…"
curl -sS "$OLLAMA_URL/api/embed" -d "{\"model\":\"$EMBED_MODEL\",\"input\":[\"warm\"],\"keep_alive\":-1}" >/dev/null || true

: > "$OUT"
for eng in $ENGINES; do
  echo "======================================================================" | tee -a "$OUT"
  echo "### engine=$eng  convs=$CONVS qa_limit=$QA_LIMIT judge=$JUDGE" | tee -a "$OUT"
  case "$eng" in
    naive)
      ECPHORIA_BENCH__ENGINE=naive \
        ./target/release/examples/locomo_eval | tee -a "$OUT" ;;
    ecphoria)
      env ECPHORIA_BENCH__ENGINE=ecphoria \
        ECPHORIA_COGNITION__GRAPH_EXPANSION="$GRAPH" ECPHORIA_COGNITION__AUTO_GRAPH="$GRAPH" \
        ECPHORIA_COGNITION__EXTRACTION="$EXTRACTION" \
        ECPHORIA_COGNITION__EXTRACTION_PROVIDER="$EXTRACTION_PROVIDER" \
        ECPHORIA_COGNITION__EXTRACTION_MODEL="$EXTRACTION_MODEL" \
        ./target/release/examples/locomo_eval | tee -a "$OUT" ;;
    mem0)
      if python3 -c "import mem0" 2>/dev/null; then
        JUDGE="$JUDGE" EVAL_MODEL="$EVAL_MODEL" OLLAMA_URL="$OLLAMA_URL" \
          EMBED_MODEL="$EMBED_MODEL" LLM_MODEL="$EXTRACTION_MODEL" \
          QA_LIMIT="$QA_LIMIT" CONVS="$CONVS" \
          python3 ops/bench/mem0_locomo.py "$DATA_DIR/run.json" | tee -a "$OUT"
      else
        echo "  (skipped — 'pip install mem0ai chromadb' to enable the Mem0 comparison)" | tee -a "$OUT"
      fi ;;
    *) echo "  unknown engine: $eng" | tee -a "$OUT" ;;
  esac
done
echo "======================================================================" | tee -a "$OUT"
echo "results written to $OUT"
