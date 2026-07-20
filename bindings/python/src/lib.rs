// The `#[pymethods]` macro combined with `Python::allow_threads` expands to code clippy reads as a
// `PyErr -> PyErr` conversion at each fn signature — a false positive on macro-generated code.
#![allow(clippy::useless_conversion)]

//! Python binding for the **embedded** Ecphoria memory engine — run the agent-memory intelligence
//! in-process from Python, no server:
//!
//! ```python
//! from ecphoria import Ecphoria
//! mem = Ecphoria.open("./agent-memory")   # or Ecphoria.open_in_memory()
//! mem.remember("alice", "Alice prefers window seats")
//! for text in mem.recall("alice", "seating preference", 5):
//!     print(text)
//! ```
//!
//! The API is synchronous: each method blocks on an internal Tokio runtime, so Python callers never
//! see async. Mirrors `ecphoria_core::embedded::Ecphoria`.

use std::sync::Arc;

use ecphoria_core::embedded::Ecphoria as CoreEcphoria;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// A multi-thread Tokio runtime with all drivers enabled. Multi-thread (not current-thread) matters:
/// the engine init drives blocking store work while other tasks make progress on worker threads;
/// paired with `Python::allow_threads` at every entry point (so the GIL is released across
/// `block_on`), this avoids the deadlock a current-thread runtime + held GIL produces in-process.
fn runtime() -> PyResult<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(err)
}

/// An embedded Ecphoria memory (file-backed or in-memory).
#[pyclass]
struct Ecphoria {
    rt: Arc<Runtime>,
    inner: CoreEcphoria,
}

#[pymethods]
impl Ecphoria {
    /// Open a file-backed memory rooted at `path` (created if missing); resumes on reopen.
    #[staticmethod]
    fn open(py: Python<'_>, path: String) -> PyResult<Self> {
        // Release the GIL: the engine init does blocking store work on Tokio's blocking pool and
        // `block_on` parks this thread — holding the GIL across that would deadlock the interpreter.
        py.allow_threads(|| {
            let rt = Arc::new(runtime()?);
            let inner = rt.block_on(CoreEcphoria::open(&path)).map_err(err)?;
            Ok(Self { rt, inner })
        })
    }

    /// Open a purely in-memory instance (nothing persisted).
    #[staticmethod]
    fn open_in_memory(py: Python<'_>) -> PyResult<Self> {
        py.allow_threads(|| {
            let rt = Arc::new(runtime()?);
            let inner = rt.block_on(CoreEcphoria::open_in_memory()).map_err(err)?;
            Ok(Self { rt, inner })
        })
    }

    /// Distill `text` into memories for `user` (dedup + contradiction resolution). Returns the count.
    fn remember(&self, py: Python<'_>, user: &str, text: &str) -> PyResult<usize> {
        py.allow_threads(|| {
            let adds = self
                .rt
                .block_on(self.inner.remember(user, text))
                .map_err(err)?;
            Ok(adds.len())
        })
    }

    /// Store one memory verbatim for `user`.
    fn add(&self, py: Python<'_>, user: &str, content: &str) -> PyResult<()> {
        py.allow_threads(|| {
            self.rt
                .block_on(self.inner.add(user, content))
                .map_err(err)?;
            Ok(())
        })
    }

    /// Hybrid recall (BM25 + vector) of `user`'s memories for `query` — returns the contents.
    #[pyo3(signature = (user, query, k=5))]
    fn recall(&self, py: Python<'_>, user: &str, query: &str, k: usize) -> PyResult<Vec<String>> {
        py.allow_threads(|| {
            let hits = self
                .rt
                .block_on(self.inner.recall(user, query, k))
                .map_err(err)?;
            Ok(hits.into_iter().map(|h| h.memory.content).collect())
        })
    }

    /// All active memory contents for `user`.
    #[pyo3(signature = (user, limit=100))]
    fn all(&self, py: Python<'_>, user: &str, limit: usize) -> PyResult<Vec<String>> {
        py.allow_threads(|| {
            let mems = self.rt.block_on(self.inner.all(user, limit)).map_err(err)?;
            Ok(mems.into_iter().map(|m| m.content).collect())
        })
    }
}

#[pymodule]
fn ecphoria(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Ecphoria>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
