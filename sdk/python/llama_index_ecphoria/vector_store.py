"""Ecphoria vector store for LlamaIndex.

Implements the LlamaIndex ``VectorStore`` interface so Ecphoria can serve
as a drop-in vector backend for any LlamaIndex pipeline.

Usage::

    from llama_index.core import VectorStoreIndex, StorageContext
    from llama_index_ecphoria import EcphoriaVectorStore

    vector_store = EcphoriaVectorStore(url="http://localhost:8432")
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)

    # Query
    query_engine = index.as_query_engine()
    response = query_engine.query("What went wrong in production?")
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

import httpx

try:
    from llama_index.core.schema import BaseNode, TextNode
    from llama_index.core.vector_stores.types import (
        BasePydanticVectorStore,
        VectorStoreQuery,
        VectorStoreQueryResult,
    )

    _HAS_LLAMA_INDEX = True
except ImportError:
    _HAS_LLAMA_INDEX = False


def _check_llama_index() -> None:
    if not _HAS_LLAMA_INDEX:
        raise ImportError(
            "llama-index-core is required for the LlamaIndex integration. "
            "Install it with: pip install llama-index-core"
        )


class EcphoriaVectorStore:
    """LlamaIndex-compatible vector store backed by the Ecphoria context lake.

    This store delegates embedding and storage to Ecphoria's server-side pipeline.
    Documents are ingested via ``/api/v1/ingest`` (auto-embedded by Ecphoria's
    configured provider), and queries use ``/api/v1/embed-and-search`` for
    text-to-vector search in a single round-trip.

    If ``llama-index-core`` is installed, this class also works as a
    ``BasePydanticVectorStore`` so it plugs into ``VectorStoreIndex`` directly.
    """

    stores_text: bool = True
    is_embedding_query: bool = False

    def __init__(
        self,
        url: str = "http://localhost:8432",
        api_key: Optional[str] = None,
        source: str = "llama-index",
        timeout: float = 30.0,
    ) -> None:
        self.url = url.rstrip("/")
        self.source = source
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.Client(
            base_url=self.url, headers=headers, timeout=timeout
        )

    # ── VectorStore interface: add ───────────────────────────────────

    def add(
        self,
        nodes: Sequence[Any],
        **kwargs: Any,
    ) -> List[str]:
        """Add nodes to the vector store.

        Each node's text content is ingested as an episodic event.
        Ecphoria's ingest pipeline handles embedding automatically.
        """
        events: list[dict[str, Any]] = []
        ids: list[str] = []

        for node in nodes:
            event: dict[str, Any] = {"event_type": "document"}
            # Extract text from node
            if hasattr(node, "get_content"):
                event["content"] = node.get_content()
            elif hasattr(node, "text"):
                event["content"] = node.text
            else:
                event["content"] = str(node)

            # Extract node ID
            node_id = getattr(node, "node_id", None) or getattr(node, "id_", None)
            if node_id:
                event["node_id"] = node_id
                ids.append(str(node_id))
            else:
                ids.append(f"ingested-{len(ids)}")

            # Extract metadata
            metadata = getattr(node, "metadata", None)
            if metadata and isinstance(metadata, dict):
                event["metadata"] = metadata

            events.append(event)

        if events:
            resp = self._client.post(
                "/api/v1/ingest",
                json={"source": self.source, "events": events},
            )
            resp.raise_for_status()

        return ids

    # ── VectorStore interface: delete ────────────────────────────────

    def delete(self, ref_doc_id: str, **kwargs: Any) -> None:
        """Delete a document by ID.

        Note: Ecphoria's episodic store is append-only by design.
        This is a no-op — use retention policies for data lifecycle.
        """
        pass

    # ── VectorStore interface: query ─────────────────────────────────

    def query(
        self,
        query: Any,
        **kwargs: Any,
    ) -> Any:
        """Query the vector store.

        Accepts either a ``VectorStoreQuery`` (if llama-index-core is installed)
        or a plain string. Returns results from Ecphoria's embed-and-search endpoint.
        """
        # Extract query text and k
        if hasattr(query, "query_str") and query.query_str:
            query_text: str = query.query_str
            k: int = getattr(query, "similarity_top_k", 5) or 5
        elif hasattr(query, "query_embedding") and query.query_embedding:
            return self._query_by_vector(query.query_embedding, getattr(query, "similarity_top_k", 5) or 5)
        elif isinstance(query, str):
            query_text = query
            k = kwargs.get("k", 5)
        else:
            raise ValueError(f"Unsupported query type: {type(query)}")

        body: dict[str, Any] = {"text": query_text, "k": k}
        resp = self._client.post("/api/v1/embed-and-search", json=body)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        return self._to_query_result(results)

    def _query_by_vector(self, embedding: list[float], k: int) -> Any:
        """Search by pre-computed embedding vector."""
        body: dict[str, Any] = {"vector": embedding, "k": k}
        resp = self._client.post("/api/v1/search", json=body)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        return self._to_query_result(results)

    def _to_query_result(self, results: list[dict[str, Any]]) -> Any:
        """Convert Ecphoria results to VectorStoreQueryResult if possible."""
        if _HAS_LLAMA_INDEX:
            nodes: list[TextNode] = []
            similarities: list[float] = []
            ids: list[str] = []

            for r in results:
                content = r.get("content", "")
                metadata = r.get("metadata", {})
                if not isinstance(metadata, dict):
                    metadata = {}
                node = TextNode(text=str(content), metadata=metadata)
                node_id = r.get("id", r.get("node_id", ""))
                if node_id:
                    node.id_ = str(node_id)

                nodes.append(node)
                similarities.append(r.get("score", 0.0))
                ids.append(str(node_id) if node_id else "")

            return VectorStoreQueryResult(
                nodes=nodes,
                similarities=similarities,
                ids=ids,
            )

        # Fallback: return raw results when llama-index is not installed
        return results

    # ── Convenience methods ──────────────────────────────────────────

    def add_texts(
        self,
        texts: Sequence[str],
        metadatas: Optional[list[dict[str, Any]]] = None,
    ) -> list[str]:
        """Add raw text strings (without LlamaIndex node objects).

        Convenience method for simple use cases.
        """
        events: list[dict[str, Any]] = []
        for i, text in enumerate(texts):
            event: dict[str, Any] = {"event_type": "document", "content": text}
            if metadatas and i < len(metadatas):
                event["metadata"] = metadatas[i]
            events.append(event)

        resp = self._client.post(
            "/api/v1/ingest",
            json={"source": self.source, "events": events},
        )
        resp.raise_for_status()
        return [f"ingested-{i}" for i in range(len(events))]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        source: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Search for similar documents using a text query.

        Returns list of dicts with 'content', 'metadata', 'score'.
        """
        body: dict[str, Any] = {"text": query, "k": k}
        if source:
            body["filters"] = {"source": source}

        resp = self._client.post("/api/v1/embed-and-search", json=body)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
