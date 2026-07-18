#!/usr/bin/env bash
# Full LoCoMo evaluation using Claude via the logged-in `claude` CLI (no API key).
#
# Self-contained: downloads + converts the real LoCoMo dataset, builds the eval example (with the
# local cross-encoder reranker), preflights the deps, then runs the full pipeline and writes a
# timestamped results file. Designed to run overnight (the QA loop spawns `claude` per question).
#
# Usage:
#   ops/bench/run-locomo-claude.sh                 # full 10 conversations, extraction=llm (local)
#   CONVS=1 ops/bench/run-locomo-claude.sh         # smoke run (1 conversation) — validate setup first
#   EXTRACTION_PROVIDER=claude-cli ops/bench/run-locomo-claude.sh   # max quality, much slower
#
# Knobs (env):
#   CONVS=10                 conversations to run (10 = full LoCoMo; start with 1 to validate)
#   EXTRACTION=llm           none | llm  (llm = extract atomic facts at ingest — the biggest lever)
#   EXTRACTION_PROVIDER=ollama   ollama (fast, local) | claude-cli (best, ~1 spawn/turn = hours) | anthropic
#   EXTRACTION_MODEL=glm-4.7-flash:latest   (or a Claude alias when provider=claude-cli, e.g. haiku)
#   EVAL_MODEL=sonnet        Claude model for the answerer + judge (via the CLI)
#   EMBED_MODEL=nomic-embed-text
#   RERANK=cross_encoder     cross_encoder (fast, local ONNX) | none | llm
#   DATA_DIR=/tmp/ecphoria-bench
set -euo pipefail
cd "$(dirname "$0")/../.."

CONVS="${CONVS:-10}"
EXTRACTION="${EXTRACTION:-llm}"
EXTRACTION_PROVIDER="${EXTRACTION_PROVIDER:-ollama}"
EXTRACTION_MODEL="${EXTRACTION_MODEL:-glm-4.7-flash:latest}"
EVAL_MODEL="${EVAL_MODEL:-sonnet}"
EMBED_MODEL="${EMBED_MODEL:-nomic-embed-text}"
RERANK="${RERANK:-cross_encoder}"
DATA_DIR="${DATA_DIR:-/tmp/ecphoria-bench}"
mkdir -p "$DATA_DIR"

# ── 1. dataset ───────────────────────────────────────────────────────────────
if [ ! -f "$DATA_DIR/locomo.json" ]; then
  echo "▶ downloading + converting LoCoMo (snap-research/locomo, 10 convos / 1986 QA)…"
  curl -sL https://raw.githubusercontent.com/snap-research/locomo/main/data/locomo10.json \
    -o "$DATA_DIR/locomo10.json"
  cargo build --release -p ecphoria-core --example locomo_convert
  ./target/release/examples/locomo_convert "$DATA_DIR/locomo10.json" > "$DATA_DIR/locomo.json"
fi
# QA_LIMIT>0 caps questions per conversation (for a quick smoke run; 0 = all).
QA_LIMIT="${QA_LIMIT:-0}"
python3 - "$DATA_DIR/locomo.json" "$DATA_DIR/run.json" "$CONVS" "$QA_LIMIT" <<'PY'
import json, sys
src, dst, convs, qa_limit = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
d = json.load(open(src))[:convs]
if qa_limit > 0:
    for c in d:
        c["qa"] = c.get("qa", [])[:qa_limit]
json.dump(d, open(dst, "w"))
print(f"  run set: {len(d)} conversation(s), {sum(len(c.get('qa', [])) for c in d)} QA")
PY

# ── 2. build eval (with the cross-encoder feature when selected) ──────────────
FEATURES=""
[ "$RERANK" = "cross_encoder" ] && FEATURES="--features rerank-local"
echo "▶ building the eval example ($FEATURES)…"
cargo build --release -p ecphoria-core --example locomo_eval $FEATURES

# ── 3. preflight ─────────────────────────────────────────────────────────────
echo "▶ preflight…"
curl -fsS --max-time 3 http://localhost:11434/api/tags >/dev/null 2>&1 \
  || { echo "  ✗ Ollama unreachable at :11434 — embeddings need it. Start Ollama and retry."; exit 1; }
echo "  ✓ Ollama up"
echo "  test" | claude -p --model "$EVAL_MODEL" >/dev/null 2>&1 \
  || { echo "  ✗ 'claude' CLI not working (log in with the model $EVAL_MODEL available)."; exit 1; }
echo "  ✓ claude CLI works (model $EVAL_MODEL)"
if [ "$RERANK" = "cross_encoder" ]; then
  echo "  ℹ cross-encoder will download the bge-reranker model on first use (~one-time)."
fi

# ── 4. run ───────────────────────────────────────────────────────────────────
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$DATA_DIR/results-$STAMP.txt"
echo "▶ running — writing to $OUT"
echo "  NB: the QA loop spawns 'claude' per question (answer + judge). $CONVS convo(s) ≈ this many"
echo "      minutes-to-hours; run detached (nohup … &) and 'tail -f $OUT' if you like."

RERANK_PROVIDER="none"
[ "$RERANK" != "none" ] && RERANK_PROVIDER="$RERANK"

env \
  LOCOMO_PATH="$DATA_DIR/run.json" \
  ECPHORIA_EMBEDDING__PROVIDER=ollama ECPHORIA_EMBEDDING__MODEL="$EMBED_MODEL" \
  ECPHORIA_COGNITION__EXTRACTION="$EXTRACTION" \
  ECPHORIA_COGNITION__EXTRACTION_PROVIDER="$EXTRACTION_PROVIDER" \
  ECPHORIA_COGNITION__EXTRACTION_MODEL="$EXTRACTION_MODEL" \
  ECPHORIA_RERANK__PROVIDER="$RERANK_PROVIDER" ECPHORIA_RERANK__BACKEND=ollama ECPHORIA_RERANK__MODEL="$EXTRACTION_MODEL" \
  ECPHORIA_EVAL__PROVIDER=claude-cli ECPHORIA_EVAL__MODEL="$EVAL_MODEL" ECPHORIA_EVAL__JUDGE=1 \
  ECPHORIA_COGNITION__GRAPH_EXPANSION=1 ECPHORIA_COGNITION__AUTO_GRAPH=1 \
  ./target/release/examples/locomo_eval 2>&1 | tee "$OUT"

echo "▶ done → $OUT"
