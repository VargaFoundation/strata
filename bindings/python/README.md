# ecphoria (Python)

The **embedded** [Ecphoria](https://github.com/VargaFoundation/ecphoria) agent-memory engine, in
Python — durable, bi-temporal memory with dedup, contradiction resolution, and hybrid retrieval,
running **in-process** (no server).

```python
from ecphoria import Ecphoria

mem = Ecphoria.open("./agent-memory")     # file-backed; or Ecphoria.open_in_memory()
mem.remember("alice", "Alice prefers window seats")
mem.add("alice", "Alice's timezone is CET")

for text in mem.recall("alice", "seating preference", k=5):
    print(text)
```

## Build / install

Built with [maturin](https://github.com/PyO3/maturin) (abi3 wheels, Python ≥ 3.9):

```bash
cd bindings/python
maturin develop            # build + install into the current venv
python -m pytest           # run the smoke test
# or build a wheel:
maturin build --release
```

## API

| Method | Description |
|--------|-------------|
| `Ecphoria.open(path)` | Open/resume a file-backed memory. |
| `Ecphoria.open_in_memory()` | Ephemeral in-memory instance. |
| `remember(user, text)` | Distill text into memories (dedup + contradiction). Returns count. |
| `add(user, content)` | Store one memory verbatim. |
| `recall(user, query, k=5)` | Hybrid BM25+vector recall → list of contents. |
| `all(user, limit=100)` | All active memory contents for a user. |

Calls are synchronous (each blocks on an internal Tokio runtime).
