"""Ecphoria client — async HTTP client for the Ecphoria agentic memory platform API."""

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


def _sql_str(value: object) -> str:
    """Escape a value for safe inclusion inside a single-quoted SQL string literal
    (doubles embedded single quotes). The server exposes a raw-SQL query API with no
    bind parameters, so the SDK must escape values it interpolates into SQL."""
    return str(value).replace("'", "''")


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


class EcphoriaClient:
    """Async client for the Ecphoria agentic memory platform REST API.

    Features retry logic with exponential backoff on 429 (rate-limited) and
    503 (service unavailable) responses.

    Usage::

        async with EcphoriaClient("http://localhost:8432") as client:
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

    async def __aenter__(self) -> EcphoriaClient:
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
            raise EcphoriaError(data["error"])
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
            raise EcphoriaError(data["error"])
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
            raise EcphoriaError(data["error"])
        return data.get("results", [])

    async def find(
        self,
        text: str,
        k: int = 5,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> list[SearchResult]:
        """Semantic search by natural language text (embed + search in one call).

        This is the recommended search method. Ecphoria embeds the text
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
            raise EcphoriaError(data["error"])
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
        Ecphoria handles embedding via the configured provider (Ollama/OpenAI).

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
            conditions.append(f"source = '{_sql_str(source)}'")
        if event_type:
            conditions.append(f"event_type = '{_sql_str(event_type)}'")

        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        # Validate the non-string parts too: `order` against a whitelist and `limit` as an int, so
        # neither can be used to inject SQL.
        order_sql = "ASC" if str(order).strip().upper() == "ASC" else "DESC"
        sql = f"SELECT * FROM episodic{where} ORDER BY ts {order_sql} LIMIT {int(limit)}"
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
            raise EcphoriaError(data["error"])
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
        offset: int = 0,
        user_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        session_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        mem_type: Optional[str] = None,
        min_importance: Optional[float] = None,
        updated_after: Optional[str] = None,
        updated_before: Optional[str] = None,
        metadata_key: Optional[str] = None,
        metadata_value: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """List active memories in a scope, with optional filters + offset pagination.

        Filters (all conjunctive): ``mem_type`` (exact), ``min_importance``,
        ``updated_after``/``updated_before`` (RFC3339), and an exact metadata match via
        ``metadata_key`` + ``metadata_value``.
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        for key, v in (
            ("user_id", user_id),
            ("agent_id", agent_id),
            ("session_id", session_id),
            ("tenant_id", tenant_id),
            ("mem_type", mem_type),
            ("min_importance", min_importance),
            ("updated_after", updated_after),
            ("updated_before", updated_before),
            ("metadata_key", metadata_key),
            ("metadata_value", metadata_value),
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

    async def memory_update(
        self,
        memory_id: str,
        *,
        content: Optional[str] = None,
        importance: Optional[float] = None,
        mem_type: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Partially correct a memory — only the provided fields change (content is re-embedded).

        Returns the updated memory, or None if it doesn't exist (or isn't in your tenant). The
        ``subject`` is not editable: to change what a memory is about, add a new one.
        """
        body: dict[str, Any] = {}
        for key, v in (
            ("content", content),
            ("importance", importance),
            ("mem_type", mem_type),
            ("metadata", metadata),
        ):
            if v is not None:
                body[key] = v
        if not body:
            raise EcphoriaError("memory_update requires at least one field to change")
        resp = await self._request("PATCH", f"/api/v1/memories/{memory_id}", json=body)
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

    async def _get_json(self, path: str, params: Any = None) -> dict[str, Any]:
        resp = await self._request("GET", path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _post_json(self, path: str, body: Any) -> dict[str, Any]:
        resp = await self._request("POST", path, json=body)
        resp.raise_for_status()
        return resp.json()

    # ── Cognition (provenance / feedback / contradictions) ───────────

    async def memory_provenance(self, memory_id: str) -> dict[str, Any]:
        """A memory's source events + supersession chain (the audit trail behind a fact)."""
        return await self._get_json(f"/api/v1/memories/{memory_id}/provenance")

    async def memory_feedback(self, memory_id: str, verdict: str) -> dict[str, Any]:
        """Give feedback so ranking learns: 'helpful' reinforces, 'wrong'/'obsolete' retires."""
        return await self._post_json(
            f"/api/v1/memories/{memory_id}/feedback", {"verdict": verdict}
        )

    async def memory_contradictions(
        self, user_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """Subjects with more than one active memory (the review queue)."""
        params = {"user_id": user_id} if user_id else None
        return (await self._get_json("/api/v1/memories/contradictions", params)).get(
            "contradictions", []
        )

    async def memory_resolve_contradiction(
        self, subject: str, keep_id: str, user_id: Optional[str] = None
    ) -> dict[str, Any]:
        """Resolve a contradiction: keep `keep_id`, supersede the rest for `subject`."""
        body: dict[str, Any] = {"subject": subject, "keep_id": keep_id}
        if user_id:
            body["user_id"] = user_id
        return await self._post_json("/api/v1/memories/contradictions/resolve", body)

    # ── Knowledge graph ──────────────────────────────────────────────

    async def memory_link(
        self, src: str, relation: str, dst: str, supersede: bool = False
    ) -> dict[str, Any]:
        """Add a graph edge (src -[relation]-> dst). supersede closes the prior (src, relation)."""
        return await self._post_json(
            "/api/v1/memories/link",
            {"src": src, "relation": relation, "dst": dst, "supersede": supersede},
        )

    async def graph_neighbors(
        self, entity: str, depth: int = 1, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Edges around an entity (depth>1 expands the subgraph)."""
        return (
            await self._get_json(
                "/api/v1/memories/graph",
                {"entity": entity, "depth": depth, "limit": limit},
            )
        ).get("edges", [])

    async def graph_edges(self, limit: int = 10000) -> list[dict[str, Any]]:
        """All knowledge-graph edges (bulk view)."""
        return (
            await self._get_json("/api/v1/memories/edges", {"limit": limit})
        ).get("edges", [])

    async def graph_centrality(
        self, as_of: Optional[str] = None, limit: Optional[int] = None
    ) -> list[dict[str, Any]]:
        """Degree + PageRank per node, optionally as-of a time."""
        params: dict[str, Any] = {}
        if as_of:
            params["as_of"] = as_of
        if limit:
            params["limit"] = limit
        return (await self._get_json("/api/v1/memories/graph/centrality", params)).get(
            "nodes", []
        )

    async def graph_path(
        self, src: str, dst: str, as_of: Optional[str] = None
    ) -> Optional[list[str]]:
        """Shortest directed path between two entities (None if unreachable)."""
        params: dict[str, Any] = {"src": src, "dst": dst}
        if as_of:
            params["as_of"] = as_of
        return (await self._get_json("/api/v1/memories/graph/path", params)).get("path")

    async def graph_communities(
        self, as_of: Optional[str] = None
    ) -> list[list[str]]:
        """Community detection (connected clusters), optionally as-of a time."""
        params = {"as_of": as_of} if as_of else None
        return (
            await self._get_json("/api/v1/memories/graph/communities", params)
        ).get("communities", [])

    # ── Templates ────────────────────────────────────────────────────

    async def memory_templates(self) -> list[dict[str, Any]]:
        """Built-in memory templates."""
        return (await self._get_json("/api/v1/memory-templates")).get("templates", [])

    async def memory_from_template(
        self,
        template: str,
        fields: dict[str, Any],
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a memory from a template + field values."""
        body: dict[str, Any] = {"template": template, "fields": fields}
        if user_id:
            body["user_id"] = user_id
        return await self._post_json("/api/v1/memories/from-template", body)

    # ── Attachments (multimodal) ─────────────────────────────────────

    async def attachment_upload(
        self,
        data: bytes,
        content_type: str = "application/octet-stream",
        filename: Optional[str] = None,
        memory_id: Optional[str] = None,
        caption: Optional[str] = None,
    ) -> dict[str, Any]:
        """Upload a blob (image/PDF/audio). Optional caption stores a searchable memory."""
        params: dict[str, Any] = {}
        for k, v in (
            ("filename", filename),
            ("memory_id", memory_id),
            ("caption", caption),
        ):
            if v is not None:
                params[k] = v
        # Raw bytes with the caller's content-type (not JSON), so httpx.post is used directly.
        resp = await self._client.post(
            "/api/v1/attachments",
            params=params or None,
            content=data,
            headers={"content-type": content_type},
        )
        resp.raise_for_status()
        return resp.json()

    async def attachment_get(self, attachment_id: str) -> bytes:
        """Download an attachment's bytes."""
        resp = await self._request("GET", f"/api/v1/attachments/{attachment_id}")
        resp.raise_for_status()
        return resp.content

    async def attachment_list(
        self, memory_id: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """List attachments (optionally for one memory)."""
        params = {"memory_id": memory_id} if memory_id else None
        return (await self._get_json("/api/v1/attachments", params)).get(
            "attachments", []
        )

    async def attachment_delete(self, attachment_id: str) -> bool:
        """Delete an attachment. False if it didn't exist."""
        resp = await self._request("DELETE", f"/api/v1/attachments/{attachment_id}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    # ── Cross-scope sharing (grants) ─────────────────────────────────

    async def grant_create(self, grantee: str, grantor: str) -> dict[str, Any]:
        """Let `grantee` additionally read `grantor`'s memories (tenant-strict)."""
        return await self._post_json(
            "/api/v1/memories/grants", {"grantee": grantee, "grantor": grantor}
        )

    async def grant_list(self, grantee: str) -> list[dict[str, Any]]:
        """List the grantors a user may additionally read."""
        return (
            await self._get_json("/api/v1/memories/grants", {"grantee": grantee})
        ).get("grants", [])

    async def grant_revoke(self, grant_id: str) -> bool:
        """Revoke a grant. False if it didn't exist."""
        resp = await self._request("DELETE", f"/api/v1/memories/grants/{grant_id}")
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    # ── Admin memory ops ─────────────────────────────────────────────

    async def memory_reembed(self, limit: int = 1000) -> dict[str, Any]:
        """Re-embed active memories with the current provider (after a model change)."""
        return await self._post_json("/api/v1/admin/memory/reembed", {"limit": limit})

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

    async def session_distill(self, session_id: str) -> dict[str, Any]:
        """Consolidate a session's events into memory."""
        return await self._post_json(f"/api/v1/sessions/{session_id}/distill", {})

    # ── Agentic platform (runs, agents, triggers, tools) ─────────────

    async def run_create(
        self,
        *,
        agent_id: Optional[str] = None,
        input: Optional[dict[str, Any]] = None,
        parent_run_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Create a durable agent/workflow run. Returns the run."""
        body: dict[str, Any] = {}
        for k, v in (
            ("agent_id", agent_id),
            ("input", input),
            ("parent_run_id", parent_run_id),
        ):
            if v is not None:
                body[k] = v
        resp = await self._request("POST", "/api/v1/runs", json=body)
        resp.raise_for_status()
        return resp.json().get("run", {})

    async def run_get(self, run_id: str) -> Optional[dict[str, Any]]:
        """Get a run by id. None if not found (or not in your tenant)."""
        resp = await self._request("GET", f"/api/v1/runs/{run_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json().get("run")

    async def run_list(
        self, *, status: Optional[str] = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        """List runs (newest first), optionally filtered by status."""
        params: dict[str, Any] = {"limit": limit}
        if status is not None:
            params["status"] = status
        resp = await self._request("GET", "/api/v1/runs", params=params)
        resp.raise_for_status()
        return resp.json().get("runs", [])

    async def run_trace(self, run_id: str) -> list[dict[str, Any]]:
        """Full step trace of a run (LLM/tool/HITL steps)."""
        resp = await self._request("GET", f"/api/v1/runs/{run_id}/trace")
        resp.raise_for_status()
        return resp.json().get("steps", [])

    async def run_cancel(self, run_id: str) -> dict[str, Any]:
        """Cancel a run."""
        resp = await self._request("POST", f"/api/v1/runs/{run_id}/cancel")
        resp.raise_for_status()
        return resp.json()

    async def run_agent(
        self, agent_id: str, question: str, *, max_turns: Optional[int] = None
    ) -> dict[str, Any]:
        """Run an agent end-to-end (durable LLM↔tool loop). Returns the resulting run.

        The run pauses (status ``waiting_approval``) if the agent requests human approval;
        approve with ``run_approve`` then ``run_resume``.
        """
        body: dict[str, Any] = {"agent_id": agent_id, "question": question}
        if max_turns is not None:
            body["max_turns"] = max_turns
        resp = await self._request("POST", "/api/v1/agents/run", json=body)
        resp.raise_for_status()
        return resp.json().get("run", {})

    async def run_request_approval(
        self, run_id: str, *, prompt: str = ""
    ) -> dict[str, Any]:
        """Pause a run for human approval (HITL)."""
        resp = await self._request(
            "POST", f"/api/v1/runs/{run_id}/request-approval", json={"prompt": prompt}
        )
        resp.raise_for_status()
        return resp.json()

    async def run_approve(self, run_id: str, approve: bool = True) -> dict[str, Any]:
        """Approve or reject a run awaiting approval (HITL)."""
        resp = await self._request(
            "POST", f"/api/v1/runs/{run_id}/approve", json={"approve": approve}
        )
        resp.raise_for_status()
        return resp.json()

    async def run_resume(self, run_id: str) -> dict[str, Any]:
        """Resume an approved run (durable resume)."""
        resp = await self._request("POST", f"/api/v1/runs/{run_id}/resume")
        resp.raise_for_status()
        return resp.json().get("run", {})

    async def trigger_register(
        self,
        name: str,
        agent_id: str,
        *,
        source: str = "*",
        event_type: str = "*",
    ) -> dict[str, Any]:
        """Register an event trigger: matching events start a run of ``agent_id``."""
        resp = await self._request(
            "POST",
            "/api/v1/triggers",
            json={
                "name": name,
                "agent_id": agent_id,
                "source": source,
                "event_type": event_type,
            },
        )
        resp.raise_for_status()
        return resp.json()

    async def trigger_list(self) -> list[dict[str, Any]]:
        """List registered event triggers."""
        resp = await self._request("GET", "/api/v1/triggers")
        resp.raise_for_status()
        return resp.json().get("triggers", [])

    async def tool_register(self, name: str, url: str) -> dict[str, Any]:
        """Register a downstream MCP tool server."""
        resp = await self._request(
            "POST", "/api/v1/tools", json={"name": name, "url": url}
        )
        resp.raise_for_status()
        return resp.json()

    async def tool_list(self) -> list[dict[str, Any]]:
        """List registered downstream MCP tool servers."""
        resp = await self._request("GET", "/api/v1/tools")
        resp.raise_for_status()
        return resp.json().get("servers", [])

    async def tool_call(
        self, server: str, tool: str, arguments: Optional[dict[str, Any]] = None
    ) -> Any:
        """Invoke a tool on a registered downstream MCP server."""
        resp = await self._request(
            "POST",
            f"/api/v1/tools/{server}/call",
            json={"tool": tool, "arguments": arguments or {}},
        )
        resp.raise_for_status()
        return resp.json().get("result")

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


class EcphoriaError(Exception):
    """Error returned by the Ecphoria API."""

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
