"""Shared Ecphoria client and utilities for the multi-agent support system."""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

ECPHORIA_URL = os.environ.get("ECPHORIA_URL", "http://localhost:8432")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)


class EcphoriaClient:
    """Thin async wrapper around Ecphoria's REST API."""

    def __init__(self, base_url: str = ECPHORIA_URL) -> None:
        self.base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=30.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def ingest(self, source: str, events: list[dict]) -> int:
        resp = await self._client.post(
            "/api/v1/ingest",
            json={"source": source, "events": events},
        )
        resp.raise_for_status()
        return resp.json().get("ingested", 0)

    async def query(self, sql: str) -> list[dict]:
        resp = await self._client.post(
            "/api/v1/query",
            json={"sql": sql},
        )
        resp.raise_for_status()
        return resp.json().get("rows", [])

    async def search(self, text: str, k: int = 5) -> list[dict]:
        try:
            resp = await self._client.post(
                "/api/v1/embed-and-search",
                json={"text": text, "k": k},
            )
            resp.raise_for_status()
            return resp.json().get("results", [])
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 503:
                return []
            raise

    async def get_state(self, agent_id: str, key: str) -> dict[str, Any] | None:
        resp = await self._client.get(f"/api/v1/state/{agent_id}/{key}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def set_state(self, agent_id: str, key: str, value: dict) -> None:
        resp = await self._client.put(
            f"/api/v1/state/{agent_id}/{key}",
            json=value,
        )
        resp.raise_for_status()

    async def health(self) -> bool:
        try:
            resp = await self._client.get("/health")
            return resp.status_code == 200
        except httpx.ConnectError:
            return False
