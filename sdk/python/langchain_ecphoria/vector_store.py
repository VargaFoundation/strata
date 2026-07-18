"""Ecphoria vector store for LangChain."""

from __future__ import annotations

from typing import Any, Iterable, Optional

import httpx


class EcphoriaVectorStore:
    """LangChain-compatible vector store backed by Ecphoria.

    Usage::

        from langchain_ecphoria import EcphoriaVectorStore

        store = EcphoriaVectorStore(url="http://localhost:8432")

        # Add texts (auto-embedded via Ecphoria's configured provider)
        store.add_texts(["Customer complaint about billing", "Server outage report"])

        # Search by text (uses /api/v1/embed-and-search)
        results = store.similarity_search("billing issue", k=5)
    """

    def __init__(
        self,
        url: str = "http://localhost:8432",
        api_key: Optional[str] = None,
    ) -> None:
        self.url = url.rstrip("/")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.Client(base_url=self.url, headers=headers, timeout=30.0)

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[list[dict[str, Any]]] = None,
        source: str = "langchain",
    ) -> list[str]:
        """Ingest texts as episodic events (auto-embedded by Ecphoria pipeline)."""
        events = []
        for i, text in enumerate(texts):
            event: dict[str, Any] = {"event_type": "document", "content": text}
            if metadatas and i < len(metadatas):
                event.update(metadatas[i])
            events.append(event)

        resp = self._client.post(
            "/api/v1/ingest",
            json={"source": source, "events": events},
        )
        resp.raise_for_status()
        return [f"ingested-{i}" for i in range(len(events))]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        source: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Search for similar documents using text query.

        Returns list of dicts with 'content', 'metadata', 'score'.
        """
        body: dict[str, Any] = {"text": query, "k": k}
        if source:
            body["filters"] = {"source": source}

        resp = self._client.post("/api/v1/embed-and-search", json=body)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
    ) -> list[tuple[dict[str, Any], float]]:
        """Search and return (document, score) tuples."""
        results = self.similarity_search(query, k=k)
        return [(r, r.get("score", 0.0)) for r in results]
