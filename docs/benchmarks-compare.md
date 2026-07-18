# LoCoMo head-to-head — Ecphoria vs naive-RAG (vs Mem0)

Companion to [`benchmarks-locomo.md`](benchmarks-locomo.md). Where that page measures Ecphoria alone,
this one puts Ecphoria **on the same dataset and the same metrics** as a competitor floor, so the
numbers are directly comparable instead of cited from different methodologies.

All engines run through one harness (`crates/ecphoria-core/examples/locomo_eval.rs`, toggled by
`ECPHORIA_BENCH__ENGINE`):

- **naive-RAG** — pure top-k vector over the raw turns (one per-user index, no BM25/RRF/graph/rerank,
  no fact extraction). The honest RAG floor. Same embeddings (incl. the query/document task prefixes).
- **Ecphoria** — the full cognition path: hybrid BM25+vector fused by RRF, optional graph expansion,
  optional rerank, optional LLM fact extraction.
- **Mem0** — the competitor, via `ops/bench/mem0_locomo.py` (Ollama-local, same metrics). Not part
  of the run below (needs `pip install mem0ai`); published reference numbers are in the last section.

Reproduce: `ops/bench/run-compare.sh` (see [Reproduce](#reproduce)).

## Results

Real run · full LoCoMo (snap-research/locomo, 10 conversations / 1986 QA) · Ollama `nomic-embed-text`
(768-d) · `extraction=none` · retrieval metrics (substring recall / MRR). Categories:
1=multi-hop, 2=temporal, 3=open-domain, 4=single-hop, 5=adversarial.

| engine / config | R@1 | R@3 | R@5 (overall) | MRR | single-hop R@5 | query p50 |
|---|---|---|---|---|---|---|
| naive-RAG, **no** prefix | 9.7% | 16.8% | 20.5% | 0.145 | 32.8% | 35 ms |
| naive-RAG, **+prefix** | 12.5% | 19.8% | **23.0%** | 0.168 | 35.3% | 42 ms |
| Ecphoria *shared* index + graph, no prefix | 6.2% | 13.4% | 18.5% | 0.114 | 27.3% | 192 ms |
| Ecphoria *shared* index + graph, +prefix *(old defaults)* | 6.4% | 13.7% | 18.6% | 0.116 | 26.6% | 186 ms |
| Ecphoria *shared* index, no-graph + **wide-pool** hack | 13.5% | 21.1% | 24.2% | 0.179 | 35.4% | 586 ms |
| **Ecphoria partitioned index, no-graph, +prefix (the fix)** | 13.2% | 20.8% | **23.6%** | **0.176** | 35.0% | **113 ms** |
| Ecphoria partitioned index, graph on, +prefix | 6.4% | 13.6% | 18.3% | 0.115 | 26.5% | 178 ms |

*Partitioned* rows use the shipped `ScopedVectorIndex` (one HNSW partition per exact scope). It
recovers the recall that the `wide-pool` hack recovered (23.6% vs 24.2%) but at **5× lower latency**
(113 ms vs 586 ms), because it removes the starvation at its root instead of masking it with a huge
per-query scan. Graph expansion is a clean **−5 pt** regression on this workload, independent of the
index (23.6% → 18.3%).

### End-to-end QA accuracy — the leaderboard-comparable metric (LLM-judge)

The retrieval table above uses substring recall. The number Mem0/Zep publish (~66/68%) is an
**LLM-judge over a generated answer**. Ran that here (`JUDGE=1`, claude-cli `haiku` as answerer +
judge, 1 conversation / 199 QA, partitioned index + graph-off) to test whether LLM fact **extraction**
— the documented "biggest lever" — pays off on the lenient metric even though it loses on substring
recall:

| ingest | retrieval R@5 | QA-F1 | **QA-judge** |
|---|---|---|---|
| extraction=none (raw turns) | 12.6% | 23.0% | **22.1%** |
| extraction=llm (haiku, 419→591 facts) | 9.5% | 16.8% | **15.1%** |

**Extraction was net-negative on all three metrics — including the LLM-judge.** It degrades
*retrieval* first (the answerer then gets worse context), and a weak extractor (haiku) drops detail;
reworded atomic facts also retrieve worse against the query terms. This **contradicts** the repo's
"extraction is the biggest lever" framing at this operating point: that assumes a strong extractor +
retrieval tuned for atomic facts + a GPT-4o-class answerer. Caveats: 1 conversation is high-variance
(cat 1/2/3 have n=32/37/13), and haiku-grading-haiku is a lenient self-judge — treat **22%** as an
honest floor for a nomic-768d + haiku pipeline, far below the GPT-4o-class published numbers.

## What the numbers say

**1. The embedding task-prefix fix is real (+~12% relative recall).** On pure vector, adding the
model's asymmetric prefixes (`search_query:` / `search_document:` for nomic) moves recall@5 from
**20.5% → 23.0%** and MRR 0.145 → 0.168. Before the fix, Ecphoria embedded queries and documents
identically with no prefix — a known regression for prefix-trained models. Fixed in the
`EmbeddingProvider::embed_query` / `embed_documents` split.

**2. Ecphoria's *default-style* hybrid+graph stack was net-negative — worse than naive vector, and
prefix-blind.** 18.6% vs naive's 23.0% recall@5, at **4× the latency**. Tellingly, turning the
prefixes on or off barely moved it (18.6% vs 18.5%): the fusion + graph + importance/recency blend
were **drowning the vector signal**, so better embeddings couldn't show through.

**3. The deficit is configuration/design, not the algorithm — and it is now fixed.** Partitioning the
vector index by scope (shipped as `ScopedVectorIndex`) and dropping graph expansion takes Ecphoria to
**23.6%** recall@5 / 0.176 MRR at **113 ms** — the hybrid now **beats** the naive floor (as a good
hybrid should) and is prefix-sensitive again, at 1/5th the latency of the earlier wide-pool
workaround (586 ms). The search machinery is fine; its defaults sabotaged it.

## Root cause of the deficit (search layer)

- **Shared-index oversample starvation → FIXED.** `MemoryStore` used to keep *one* vector index for
  all scopes; the old `memory_index_search` fetched only `pool·4` global nearest neighbours
  (`retrieval_pool=50` → 200) and *then* post-filtered by scope. With 5882 vectors across 10 users a
  user's answer memory often wasn't in the global top-200 → dropped before fusion ran. Replaced by
  `ScopedVectorIndex` (one HNSW partition per exact scope): a search only traverses its own scope, so
  there is no oversample and no starvation. **+5 pts recall@5 at default settings, 113 ms.**
- **Graph expansion adds noise on this workload.** Deterministic triple-extraction edges surface
  loosely-related memories that compete in the fused ranking. Off by default in the product; the
  published `benchmarks-locomo.md` run had it *on* (`AUTO_GRAPH=1`), which is part of why that number
  (21.7%) is mediocre.
- **Importance/recency blend — tested, neutral here.** The post-fusion `rrf·(1 + w_imp·importance +
  w_rec·recency)` re-rank was suspected of diluting relevance. Making the weights configurable and
  A/B-ing them (0/0 vs 0.1/0.05 vs the 0.3/0.2 default) gave **identical** recall@5 (23.6%) / MRR
  (0.176): on LoCoMo importance is uniform (0.5) and every turn shares an ingest time, so the
  multiplier is ~constant and doesn't reorder. Kept the default; the knob matters only for
  varied-age/importance production workloads. The RRF *arm* weighting (vector vs BM25) is still
  equal-weight and untested — a candidate lever.
- **Latency.** The wide-pool config is 586 ms/query (BM25 scans up to `retrieval_scan_cap`=6000
  in-memory per query — not an inverted index). Quality-positive but 14× slower than naive; the
  durable fix is a real BM25 index + partitioned vectors, not a bigger linear scan.

- **Silent embedding failures.** Ingest embedding is best-effort (`engine.rs:memory_add`) — a
  cold/slow Ollama makes memories store with **no vector**, silently degrading search to BM25 with no
  error surfaced. Observed here as intermittent `stored=0` until the model was pinned (`keep_alive=-1`).

## Recommendations (ranked by ROI)

1. ~~Partition the vector index by scope~~ — **done** (`ScopedVectorIndex`): +5 pts recall@5 at
   113 ms, no starvation. Follow-up: a filtered-ANN option for genuinely cross-scope searches.
2. **Don't fuse graph expansion by default on conversational recall** (confirmed −5 pts here,
   index-independent); make it query-type-gated.
3. **Weight the RRF arms** — measured small positive. Favouring the vector arm over BM25
   (`retrieval_vector_weight`) rises recall@5 monotonically: 1/1 → 19.7%, 2/1 → 20.3%, **3/1 →
   20.9%** (3 conv), confirming BM25 is the noisier arm on semantic recall. Shipped as configurable
   weights; **default kept 1/1** (equal) since the best ratio is workload-dependent (keyword-heavy
   corpora want BM25 back). The importance/recency blend, by contrast, was **neutral** (see root cause).
4. **Keep the embedding task-prefix fix** (shipped). A "stronger" model isn't automatically better:
   **bge-m3 (1024d) tested *worse* than nomic-embed-text** on LoCoMo (ecphoria, 3 conv: recall@5 19.7%
   → 17.7%, and 3× the query latency) — via Ollama bge-m3 is dense-only (loses its sparse+ColBERT
   edge) and is tuned for multilingual/long-doc, whereas LoCoMo is short English turns where nomic +
   its task prefixes fit better. nomic-with-prefixes is the best *local* option here; a hosted
   `text-embedding-3-large` is the untested candidate.
5. **Cross-encoder reranker — measured positive; the biggest quality lever after the index fix.**
   bge-reranker (`--features rerank-local`) over the top-50 fused candidates: recall@5 **19.7% →
   23.9%** (+4.2 pts / +21%), R@1 13.1% → 15.9%, MRR 0.160 → 0.190 (nomic, ecphoria, 3 conv). Cost:
   query p50 119 ms → **~1.1 s** (CPU cross-encoder over 50 pairs) — the latency/quality trade the
   docs predicted; gate it behind an SLA. Also fixed the feature's build (it pulled system OpenSSL
   via hf-hub's native-tls → switched to rustls) so it works in lean/rootless environments.
6. **LLM fact extraction is not a free lever — measured net-negative here.** Tested end-to-end with
   the LLM-judge (`JUDGE=1`, claude-cli haiku): extraction dropped QA-judge 22.1% → 15.1% (and QA-F1
   23.0% → 16.8%, recall@5 12.6% → 9.5%) — see the QA table above. The published "biggest lever"
   result assumes a strong extractor + retrieval tuned for atomic facts + a GPT-4o-class answerer; on
   a nomic-768d + haiku pipeline it adds noise. Revisit only with a stronger extractor and
   fact-tuned retrieval, and measure on more than one conversation. (This work also exposed and fixed
   a `claude-cli` provider bug where the system prompt was ignored — extraction/rerank/judge via the
   CLI were silently broken.)
7. **Surface embedding failures** (warn/metric) instead of silently degrading to BM25 *(shipped)*.

## Reproduce

```bash
# Ollama with the embedding model + the logged-in `claude` CLI (for the optional judge).
ollama pull nomic-embed-text
# retrieval metrics, all engines, full dataset:
ops/bench/run-compare.sh
# quick smoke (1 conversation, two engines):
CONVS=1 ENGINES="naive ecphoria" ops/bench/run-compare.sh
# end-to-end QA accuracy via the Claude CLI judge:
JUDGE=1 ops/bench/run-compare.sh
# A/B the prefix fix on an identical binary:
ECPHORIA_EMBEDDING__QUERY_PREFIX= ECPHORIA_EMBEDDING__DOCUMENT_PREFIX= ...   # forces prefixes off
# the recovered config from the table:
GRAPH=0 ECPHORIA_COGNITION__RETRIEVAL_POOL=800 ECPHORIA_COGNITION__RETRIEVAL_SCAN_CAP=6000 ...
```

## Mem0 comparison

`ops/bench/mem0_locomo.py` runs Mem0 over the same `locomo.json` and prints the same metrics
(Ollama-local extraction + embeddings, Claude-CLI judge — no API key). Enable it in the harness with
`ENGINES="naive ecphoria mem0"` after `pip install mem0ai chromadb`.

Published LoCoMo references (methodology differs — an **LLM-judge over generated answers**, which is
far more lenient than the substring recall above, so *not* directly comparable to this page's
retrieval numbers): Mem0 reports ~66% and Zep ~68% end-to-end QA accuracy. To compare on *that*
axis, run this harness with `JUDGE=1` (adds the `QA-judge` column) — the leaderboard-comparable metric.
