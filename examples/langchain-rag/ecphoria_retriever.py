"""Custom LangChain retriever backed by Ecphoria's embed-and-search API."""

from __future__ import annotations

from typing import Optional

import httpx
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import Field


class EcphoriaRetriever(BaseRetriever):
    """Retrieve documents from Ecphoria using semantic search.

    Sends the query text to Ecphoria's /api/v1/embed-and-search endpoint,
    which embeds the text and performs HNSW vector search in one call.
    """

    ecphoria_url: str = Field(default="http://localhost:8432")
    k: int = Field(default=5, description="Number of results to return")
    source_filter: Optional[str] = Field(
        default=None,
        description="Optional source filter (e.g. 'langchain-rag')",
    )

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        body: dict = {"text": query, "k": self.k}
        if self.source_filter:
            body["filters"] = {"source": self.source_filter}

        resp = httpx.post(
            f"{self.ecphoria_url}/api/v1/embed-and-search",
            json=body,
            timeout=30.0,
        )
        resp.raise_for_status()

        documents = []
        for result in resp.json().get("results", []):
            metadata = result.get("metadata", {})
            metadata["score"] = result.get("score", 0.0)
            metadata["ecphoria_id"] = result.get("id", "")
            documents.append(
                Document(
                    page_content=result.get("content", ""),
                    metadata=metadata,
                )
            )

        return documents
