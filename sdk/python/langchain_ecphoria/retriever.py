"""Ecphoria retriever for LangChain — text-to-documents in one call."""

from __future__ import annotations

from typing import Any, Optional

from langchain_ecphoria.vector_store import EcphoriaVectorStore


class EcphoriaRetriever:
    """LangChain-compatible retriever backed by Ecphoria's embed-and-search.

    Usage::

        from langchain_ecphoria import EcphoriaRetriever

        retriever = EcphoriaRetriever(url="http://localhost:8432", k=5)

        # Use in a LangChain chain
        docs = retriever.get_relevant_documents("billing issue")
        for doc in docs:
            print(doc["content"], doc["score"])
    """

    def __init__(
        self,
        url: str = "http://localhost:8432",
        api_key: Optional[str] = None,
        k: int = 5,
        source: Optional[str] = None,
    ) -> None:
        self._store = EcphoriaVectorStore(url=url, api_key=api_key)
        self.k = k
        self.source = source

    def get_relevant_documents(self, query: str) -> list[dict[str, Any]]:
        """Retrieve documents relevant to the query.

        Returns list of dicts with 'content', 'metadata', 'score'.
        """
        return self._store.similarity_search(
            query, k=self.k, source=self.source
        )

    async def aget_relevant_documents(self, query: str) -> list[dict[str, Any]]:
        """Async version of get_relevant_documents."""
        # For async, use the async SDK directly
        import httpx

        body: dict[str, Any] = {"text": query, "k": self.k}
        if self.source:
            body["filters"] = {"source": self.source}

        async with httpx.AsyncClient(
            base_url=self._store.url, timeout=30.0
        ) as client:
            resp = await client.post("/api/v1/embed-and-search", json=body)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
