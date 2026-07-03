#!/usr/bin/env python3
"""
Mem0 vs Strata on LoCoMo — an apples-to-apples competitor baseline.

Runs **Mem0** over the *same* `locomo.json` that the Rust harness
(`crates/strata-core/examples/locomo_eval.rs`) consumes, and reports the *same* metrics
(recall@{1,3,5}, MRR, and an optional end-to-end QA LLM-judge), so the two numbers are directly
comparable rather than cited from different methodologies.

Local + free by default, matching the Strata bench's philosophy:
  - Mem0's fact extraction (LLM) and embeddings run on **Ollama** (no API key).
  - The QA answerer + judge use the logged-in **`claude` CLI** (`claude -p`, no API key), exactly
    like `ops/bench/run-locomo-claude.sh`.

Prerequisites
  pip install "mem0ai" chromadb
  ollama pull nomic-embed-text          # embedder (768-d) — MUST match EMBED_DIMS below
  ollama pull llama3.1                  # Mem0's extraction LLM
  # `claude` CLI logged in (only needed when JUDGE=1)

Run
  # 1) Retrieval metrics only (fast, no Claude spawns):
  python ops/bench/mem0_locomo.py locomo.json

  # 2) + end-to-end QA accuracy (answer from Mem0 hits, grade with the Claude CLI judge):
  JUDGE=1 python ops/bench/mem0_locomo.py locomo.json

Knobs (env): OLLAMA_URL, EMBED_MODEL, EMBED_DIMS, LLM_MODEL, TOPK, QA_FACTS, QA_LIMIT, CONVS,
             JUDGE, EVAL_MODEL (Claude alias for answerer/judge).

NB: Mem0's Python API and return shapes have drifted across releases; this script reads results
defensively (handles both list and {"results": [...]} shapes). If your installed Mem0 differs,
eyeball the `_memories_from` helper and adjust — same spirit as the dataset-shape caveat in
`locomo_convert.rs`.
"""
import json
import os
import re
import subprocess
import sys
import time
from collections import Counter

# ------------------------------- config (env-overridable) -------------------------------
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
EMBED_DIMS = int(os.environ.get("EMBED_DIMS", "768"))
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.1")
TOPK = int(os.environ.get("TOPK", "10"))        # retrieve depth (matches locomo_eval K=10)
QA_FACTS = int(os.environ.get("QA_FACTS", "8"))  # facts fed to the answerer (matches harness)
QA_LIMIT = int(os.environ.get("QA_LIMIT", "0"))  # cap questions per conversation (0 = all)
CONVS = int(os.environ.get("CONVS", "0"))        # cap conversations (0 = all)
JUDGE = os.environ.get("JUDGE", "0") in ("1", "true")
EVAL_MODEL = os.environ.get("EVAL_MODEL", "sonnet")


def tokenize(s: str):
    return [t for t in re.split(r"[^0-9a-zA-Z]+", s.lower()) if t]


def token_f1(pred: str, gold: str) -> float:
    """SQuAD-style bag-of-words token F1 — identical to the Rust harness's token_f1."""
    p, g = tokenize(pred), tokenize(gold)
    if not p or not g:
        return 1.0 if not p and not g else 0.0
    common = sum((Counter(p) & Counter(g)).values())
    if common == 0:
        return 0.0
    prec, rec = common / len(p), common / len(g)
    return 2 * prec * rec / (prec + rec)


def claude_cli(system: str, user: str) -> str:
    """Answer/judge via the logged-in Claude CLI (no API key), like run-locomo-claude.sh."""
    prompt = f"{system}\n\n{user}"
    try:
        out = subprocess.run(
            ["claude", "-p", prompt, "--model", EVAL_MODEL],
            capture_output=True, text=True, timeout=180,
        )
        return out.stdout.strip()
    except Exception as e:  # noqa: BLE001
        print(f"  claude CLI error: {e}", file=sys.stderr)
        return ""


def answer_question(question: str, facts) -> str:
    body = "Facts:\n" + "".join(f"- {f}\n" for f in facts)
    body += (
        f"\nQuestion: {question}\nAnswer in as few words as possible using ONLY the facts above. "
        'If the facts do not contain the answer, reply "unknown".'
    )
    return claude_cli("You are a precise question-answering assistant.", body)


def judge_correct(question: str, gold: str, pred: str) -> bool:
    body = (
        f"Question: {question}\nReference answer: {gold}\nCandidate answer: {pred}\n\n"
        "Does the candidate convey the same information as the reference (ignore wording and "
        'formatting)? Reply with only "yes" or "no".'
    )
    return claude_cli("You are a strict grader.", body).lower().lstrip().startswith("yes")


def _memories_from(res):
    """Normalize Mem0 search() return into a list of memory strings (version-tolerant)."""
    items = res.get("results", res) if isinstance(res, dict) else res
    out = []
    for it in items or []:
        if isinstance(it, dict):
            out.append(it.get("memory") or it.get("text") or json.dumps(it))
        else:
            out.append(str(it))
    return out


def build_memory():
    from mem0 import Memory  # imported here so --help works without mem0 installed
    config = {
        "llm": {
            "provider": "ollama",
            "config": {"model": LLM_MODEL, "ollama_base_url": OLLAMA_URL, "temperature": 0.0},
        },
        "embedder": {
            "provider": "ollama",
            "config": {
                "model": EMBED_MODEL,
                "ollama_base_url": OLLAMA_URL,
                "embedding_dims": EMBED_DIMS,
            },
        },
        "vector_store": {
            "provider": "chroma",
            "config": {"collection_name": "locomo_mem0", "path": "/tmp/strata-bench/mem0_chroma"},
        },
    }
    return Memory.from_config(config)


def main():
    if len(sys.argv) < 2:
        print("usage: python ops/bench/mem0_locomo.py <locomo.json>", file=sys.stderr)
        sys.exit(2)
    dataset = json.load(open(sys.argv[1]))
    if CONVS:
        dataset = dataset[:CONVS]

    mem = build_memory()

    ranks, f1s, judged = [], [], []
    ingest_ms, query_ms = [], []
    stored = 0

    for convo in dataset:
        user = convo["user"]
        # Fresh namespace per conversation (Mem0 scopes by user_id).
        for turn in convo["turns"]:
            t0 = time.time()
            res = mem.add(turn, user_id=user)
            ingest_ms.append((time.time() - t0) * 1000)
            # Mem0 returns the facts it extracted+stored; count them when available.
            stored += len(_memories_from(res)) or 1
        qa = convo["qa"][:QA_LIMIT] if QA_LIMIT else convo["qa"]
        for item in qa:
            q, gold = item["question"], str(item["expected"])
            t0 = time.time()
            hits = _memories_from(mem.search(q, user_id=user, limit=TOPK))
            query_ms.append((time.time() - t0) * 1000)

            needle = gold.lower()
            rank = next((i + 1 for i, h in enumerate(hits) if needle in h.lower()), None)
            ranks.append(rank)
            if rank is None:
                print(f"  MISS: q={q!r} expected={gold!r}")

            if JUDGE:
                pred = answer_question(q, hits[:QA_FACTS])
                f1s.append(token_f1(pred, gold))
                judged.append(judge_correct(q, gold, pred))

    n = max(len(ranks), 1)
    def recall_at(k): return sum(1 for r in ranks if r is not None and r <= k)
    mrr = sum(1.0 / r for r in ranks if r) / n

    def pct(v, p):
        if not v:
            return 0.0
        v = sorted(v)
        return v[min(int(p * len(v)), len(v) - 1)]

    print("\n── Mem0 LoCoMo eval ───────────────────────────────")
    print(f"conversations:    {len(dataset)}")
    print(f"memories stored:  {stored}")
    print(f"questions:        {len(ranks)}\n")
    line = (f"OVERALL        n={len(ranks):<4} R@1={100*recall_at(1)/n:5.1f}% "
            f"R@3={100*recall_at(3)/n:5.1f}% R@5={100*recall_at(5)/n:5.1f}% MRR={mrr:.3f}")
    if f1s:
        line += f"  QA-F1={100*sum(f1s)/len(f1s):5.1f}%"
    if judged:
        line += f"  QA-judge={100*sum(judged)/len(judged):5.1f}%"
    print(line)
    print(f"\ningest  p50/p95:  {pct(ingest_ms,0.5):.2f} / {pct(ingest_ms,0.95):.2f} ms")
    print(f"query   p50/p95:  {pct(query_ms,0.5):.2f} / {pct(query_ms,0.95):.2f} ms")
    print(f"engine:           mem0 (ollama {LLM_MODEL} + {EMBED_MODEL}) · judge={'claude:'+EVAL_MODEL if JUDGE else 'off'}")


if __name__ == "__main__":
    main()
