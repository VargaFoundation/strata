"""Strata client — async HTTP client for the Strata context lake API."""

from __future__ import annotations

import asyncio
import random
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    Optional,
    Sequence,
    TypedDict,
    Union,
)

import httpx


class SearchFilters(TypedDict, total=False):
    source: str
    event_type: str


class SearchResult(TypedDict, total=False):
    id: str
    score: float
    content: str
    metadata: dict[str, Any]


class StateEntry(TypedDict, total=False):
    agent_id: str
    key: str
    value: Any
    version: int
    updated_at: str


class HealthResponse(TypedDict, total=False):
    status: str
    version: str


class ClusterStatusResponse(TypedDict, total=False):
    node_id: int
    state: str
    leader: Optional[int]
    term: int


# Status codes that trigger automatic retry with backoff.
_RETRYABLE_STATUSES = {429, 503}
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_BASE = 0.5
_DEFAULT_BACKOFF_MAX = 30.0


class StrataClient:
    """Async client for the Strata context lake REST API.

    Features retry logic with exponential backoff on 429 (rate-limited) and
    503 (service unavailable) responses.

    Usage::

        async with StrataClient("http://localhost:8432") as client:
            # Ingest events
            count = await client.ingest("my-app", [
                {"event_type": "user.signup", "user_id": "u1"},
            ])

            # Batch ingest (streaming, chunked)
            count = await client.batch_ingest("my-app", large_event_list, batch_size=500)

            # Query with SQL
            rows = await client.query("SELECT * FROM episodic LIMIT 10")

            # Semantic search
            results = await client.find("frustrated customer", k=5)

            # Embed text and search
            results = await client.embed("billing issue", k=5)

            # Agent state
            await client.state_set("bot-1", "mood", "happy")
            entry = await client.state_get("bot-1", "mood")
    """

    def __init__(
        self,
        url: str = "http://localhost:8432",
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_base: float = _DEFAULT_BACKOFF_BASE,
        backoff_max: float = _DEFAULT_BACKOFF_MAX,
    ) -> None:
        self.url: str = url.rstrip("/")
        self.max_retries: int = max_retries
        self.backoff_base: float = backoff_base
        self.backoff_max: float = backoff_max
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client: httpx.AsyncClient = httpx.AsyncClient(
            base_url=self.url,
            headers=headers,
            timeout=timeout,
        )

    async def __aenter__(self) -> StrataClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    # ── Retry helper ────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Any = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry + exponential backoff on 429/503."""
        last_resp: Optional[httpx.Response] = None

        for attempt in range(self.max_retries + 1):
            resp = await self._client.request(method, path, json=json, params=params)

            if resp.status_code not in _RETRYABLE_STATUSES:
                return resp

            last_resp = resp

            if attempt < self.max_retries:
                # Check for Retry-After header
                retry_after = resp.headers.get("retry-after")
                if retry_after is not None:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = self.backoff_base * (2**attempt)
                else:
                    delay = self.backoff_base * (2**attempt)

                # Add jitter (±25%)
                delay = min(delay, self.backoff_max)
                jitter = delay * 0.25 * (2 * random.random() - 1)
                await asyncio.sleep(delay + jitter)

        # All retries exhausted — raise on the last response
        assert last_resp is not None
        last_resp.raise_for_status()
        return last_resp  # unreachable, but keeps type checker happy

    # ── Health ───────────────────────────────────────────────────────

    async def health(self) -> HealthResponse:
        """Check server health."""
        resp = await self._request("GET", "/health")
        resp.raise_for_status()
        return resp.json()

    # ── Query ────────────────────────────────────────────────────────

    async def query(self, sql: str) -> list[dict[str, Any]]:
        """Execute a SQL query against the episodic store.

        Returns a list of row dicts.
        """
        resp = await self._request("POST", "/api/v1/query", json={"sql": sql})
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("rows", [])

    # ── Ingest ───────────────────────────────────────────────────────

    async def ingest(
        self,
        source: str,
        events: list[dict[str, Any]],
    ) -> int:
        """Ingest events into episodic memory.

        Returns the number of events ingested.
        """
        resp = await self._request(
            "POST",
            "/api/v1/ingest",
            json={"source": source, "events": events},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("ingested", 0)

    async def batch_ingest(
        self,
        source: str,
        events: Union[Sequence[dict[str, Any]], Iterable[dict[str, Any]]],
        *,
        batch_size: int = 500,
    ) -> int:
        """Ingest events in batches for large datasets.

        Splits ``events`` into chunks of ``batch_size`` and sends each chunk
        as a separate ingest request. Returns the total count of ingested events.

        Usage::

            total = await client.batch_ingest("logs", huge_list, batch_size=1000)
        """
        total = 0
        batch: list[dict[str, Any]] = []

        for event in events:
            batch.append(event)
            if len(batch) >= batch_size:
                total += await self.ingest(source, batch)
                batch = []

        if batch:
            total += await self.ingest(source, batch)

        return total

    # ── Search ───────────────────────────────────────────────────────

    async def search(
        self,
        vector: list[float],
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[SearchResult]:
        """Semantic search by pre-computed vector.

        For text-based search, use ``find()`` instead.
        """
        body: dict[str, Any] = {"vector": vector, "k": k}
        filters: dict[str, str] = {}
        if source:
            filters["source"] = source
        if event_type:
            filters["event_type"] = event_type
        if filters:
            body["filters"] = filters

        resp = await self._request("POST", "/api/v1/search", json=body)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("results", [])

    async def find(
        self,
        text: str,
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[SearchResult]:
        """Semantic search by natural language text (embed + search in one call).

        This is the recommended search method. Strata embeds the text
        using the configured provider and searches the vector index.

        Usage::

            results = await client.find("frustrated customer billing issue", k=5)
        """
        body: dict[str, Any] = {"text": text, "k": k}
        filters: dict[str, str] = {}
        if source:
            filters["source"] = source
        if event_type:
            filters["event_type"] = event_type
        if filters:
            body["filters"] = filters

        resp = await self._request("POST", "/api/v1/embed-and-search", json=body)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("results", [])

    async def embed(
        self,
        text: str,
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[SearchResult]:
        """Embed text and search — convenience alias for ``find()``.

        Calls ``/api/v1/embed-and-search`` with just a text string.
        Strata handles embedding via the configured provider (Ollama/OpenAI).

        Usage::

            results = await client.embed("what went wrong in production?", k=10)
        """
        return await self.find(text, k=k, source=source, event_type=event_type)

    # ── Query Builder ────────────────────────────────────────────────

    async def events(
        self,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        order: str = "DESC",
    ) -> list[dict[str, Any]]:
        """Query episodic events with a fluent API (no raw SQL needed).

        Usage::

            # Get last 10 events from 'my-app'
            events = await client.events(source="my-app", limit=10)

            # Get all 'user.signup' events
            signups = await client.events(event_type="user.signup")
        """
        conditions: list[str] = []
        if source:
            conditions.append(f"source = '{source}'")
        if event_type:
            conditions.append(f"event_type = '{event_type}'")

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM episodic{where} ORDER BY ts {order} LIMIT {limit}"
        return await self.query(sql)

    async def sources(self) -> list[str]:
        """List all event sources."""
        resp = await self._request("GET", "/api/v1/schema/sources")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data.get("sources", [])

    async def agents(self) -> list[str]:
        """List all agent IDs."""
        resp = await self._request("GET", "/api/v1/schema/agents")
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        return data.get("agents", [])

    # ── State ────────────────────────────────────────────────────────

    async def state_get(
        self, agent_id: str, key: str
    ) -> Optional[StateEntry]:
        """Get agent state. Returns None if not found."""
        resp = await self._request("GET", f"/api/v1/state/{agent_id}/{key}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            return None
        return data  # type: ignore[return-value]

    async def state_set(
        self, agent_id: str, key: str, value: Any
    ) -> int:
        """Set agent state. Returns the new version number."""
        resp = await self._request(
            "PUT",
            f"/api/v1/state/{agent_id}/{key}",
            json=value,
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        if "error" in data:
            raise StrataError(data["error"])
        return data.get("version", 0)

    async def state_delete(self, agent_id: str, key: str) -> None:
        """Delete agent state."""
        resp = await self._request("DELETE", f"/api/v1/state/{agent_id}/{key}")
        resp.raise_for_status()

    # ── Memory (cognition layer) ─────────────────────────────────────

    async def memory_add(
        self,
        content: str,
        *,
        subject: Optional[str] = None,
        importance: Optional[float] = None,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Add a memory through the cognition pipeline (dedup / contradiction / importance).

        Returns the resulting memory + outcome (Inserted / Confirmed / Merged / Superseded).
        """
        body: dict[str, Any] = {"content": content}
        for k, v in (
            ("subject", subject),
            ("importance", importance),
            ("user_id", user_id),
            ("agent_id", agent_id),
            ("session_id", session_id),
            ("tenant_id", tenant_id),
            ("metadata", metadata),
        ):
            if v is not None:
                body[k] = v
        resp = await self._request("POST", "/api/v1/memories", json=body)
        resp.raise_for_status()
        return resp.json()

    async def memory_search(
        self,
        query: str,
        *,
        k: int = 5,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Hybrid (BM25 + vector) search over a scope's memories. Returns ranked hits."""
        body: dict[str, Any] = {"query": query, "k": k}
        for key, v in (
            ("user_id", user_id),
            ("agent_id", agent_id),
            ("session_id", session_id),
            ("tenant_id", tenant_id),
        ):
            if v is not None:
                body[key] = v
        resp = await self._request("POST", "/api/v1/memories/search", json=body)
        resp.raise_for_status()
        return resp.json().get("results", [])

    async def memory_list(
        self,
        *,
        limit: int = 50,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """List active memories in a scope."""
        params: dict[str, Any] = {"limit": limit}
        for key, v in (
            ("user_id", user_id),
            ("agent_id", agent_id),
            ("session_id", session_id),
            ("tenant_id", tenant_id),
        ):
            if v is not None:
                params[key] = v
        resp = await self._request("GET", "/api/v1/memories", params=params)
        resp.raise_for_status()
        return resp.json().get("memories", [])

    async def memory_get(self, memory_id: str) -> Optional[dict[str, Any]]:
        """Get a memory by id. Returns None if not found (or not in your tenant)."""
        resp = await self._request("GET", f"/api/v1/memories/{memory_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    async def memory_history(self, memory_id: str) -> list[dict[str, Any]]:
        """Bi-temporal history for a memory's subject (oldest first)."""
        resp = await self._request("GET", f"/api/v1/memories/{memory_id}/history")
        resp.raise_for_status()
        return resp.json().get("history", [])

    async def memory_delete(self, memory_id: str) -> bool:
        """Delete a memory by id. Returns False if it didn't exist (or not in your tenant)."""
        resp = await self._request("DELETE", f"/api/v1/memories/{memory_id}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    # ── Sessions ─────────────────────────────────────────────────────

    async def session_start(
        self,
        session_id: str,
        agent_id: str,
        *,
        parent_session_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Start a conversation session."""
        body: dict[str, Any] = {"session_id": session_id, "agent_id": agent_id}
        if parent_session_id is not None:
            body["parent_session_id"] = parent_session_id
        if metadata is not None:
            body["metadata"] = metadata
        resp = await self._request("POST", "/api/v1/sessions", json=body)
        resp.raise_for_status()
        return resp.json()

    async def session_end(
        self, session_id: str, *, summary: Optional[str] = None
    ) -> dict[str, Any]:
        """End a session, optionally attaching a summary."""
        body: dict[str, Any] = {}
        if summary is not None:
            body["summary"] = summary
        resp = await self._request(
            "POST", f"/api/v1/sessions/{session_id}/end", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def session_recall(self, session_id: str) -> list[dict[str, Any]]:
        """Recall all events recorded in a session."""
        resp = await self._request("GET", f"/api/v1/sessions/{session_id}/recall")
        resp.raise_for_status()
        return resp.json().get("events", [])

    # ── Admin ────────────────────────────────────────────────────────

    async def backup(self) -> dict[str, Any]:
        """Trigger a backup of all stores."""
        resp = await self._request("POST", "/api/v1/admin/backup")
        resp.raise_for_status()
        return resp.json()

    async def enforce_retention(self) -> dict[str, Any]:
        """Enforce data retention policy."""
        resp = await self._request("POST", "/api/v1/admin/retention")
        resp.raise_for_status()
        return resp.json()

    # ── Cluster ──────────────────────────────────────────────────────

    async def cluster_status(self) -> ClusterStatusResponse:
        """Get Raft cluster status."""
        resp = await self._request("GET", "/cluster/status")
        resp.raise_for_status()
        return resp.json()

    # ── WebSocket Watcher ────────────────────────────────────────────

    async def watch_state(
        self, agent_id: str
    ) -> AsyncIterator[dict[str, Any]]:
        """Watch state changes for an agent via WebSocket.

        Yields StateChange dicts as they occur.

        Usage::

            async for change in client.watch_state("bot-1"):
                print(f"{change['key']} = {change['value']}")
        """
        import json
        import websockets

        ws_url = self.url.replace("http://", "ws://").replace(
            "https://", "wss://"
        )
        uri = f"{ws_url}/api/v1/state/{agent_id}/watch"

        async with websockets.connect(uri) as ws:
            async for message in ws:
                yield json.loads(message)


class StrataError(Exception):
    """Error returned by the Strata API."""

    code: str
    message: str
    request_id: Optional[str]

    def __init__(self, error: Any) -> None:
        if isinstance(error, dict):
            self.code = error.get("code", "UNKNOWN")
            self.message = error.get("message", str(error))
            self.request_id = error.get("request_id")
            super().__init__(self.message)
        else:
            self.code = "UNKNOWN"
            self.message = str(error)
            self.request_id = None
            super().__init__(self.message)
