"""Smoke test for the embedded Ecphoria Python binding.

Run after building the extension module (maturin develop, or build + copy the .so onto sys.path):

    cd bindings/python && maturin develop && python -m pytest
"""
import ecphoria


def test_embedded_add_recall_and_scope():
    mem = ecphoria.Ecphoria.open_in_memory()

    mem.add("alice", "Alice lives in Paris")
    n = mem.remember("alice", "Alice loves espresso")
    assert n >= 1

    # Recall works with BM25 (no embedding provider needed).
    hits = mem.recall("alice", "where does alice live", 5)
    assert any("Paris" in h for h in hits)

    # Per-user scoping.
    assert mem.all("bob", 10) == []
    assert len(mem.all("alice", 10)) >= 1


def test_module_has_version():
    assert isinstance(ecphoria.__version__, str)
