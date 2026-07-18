"""CrewAI tool wrappers around the Ecphoria Python SDK.

These tools let CrewAI agents interact with Ecphoria's memory stores:
- EcphoriaSearchTool: Semantic search over episodic events
- EcphoriaQueryTool: SQL queries against the episodic store
- EcphoriaStateTool: Agent state get/set
- EcphoriaIngestTool: Ingest new events
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

from crewai.tools import BaseTool
from pydantic import Field

from ecphoria import EcphoriaClient


def _run_async(coro: Any) -> Any:
    """Run an async coroutine from sync context."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    # If there's already an event loop, create a new one in a thread
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


class EcphoriaSearchTool(BaseTool):
    """Search Ecphoria's memory using natural language."""

    name: str = "ecphoria_search"
    description: str = (
        "Search Ecphoria's episodic memory using natural language. "
        "Input should be a search query string. Returns semantically similar events."
    )
    url: str = Field(default="http://localhost:8432")

    def _run(self, query: str) -> str:
        async def _search() -> list[dict[str, Any]]:
            async with EcphoriaClient(self.url) as client:
                return await client.find(query, k=5)

        results = _run_async(_search())
        return json.dumps(results, indent=2, default=str)


class EcphoriaQueryTool(BaseTool):
    """Execute SQL queries against Ecphoria's episodic store."""

    name: str = "ecphoria_query"
    description: str = (
        "Execute a SQL query against Ecphoria's episodic event store. "
        "Input should be a valid SQL SELECT statement. "
        "The table is called 'episodic' with columns: ts, source, event_type, payload."
    )
    url: str = Field(default="http://localhost:8432")

    def _run(self, sql: str) -> str:
        async def _query() -> list[dict[str, Any]]:
            async with EcphoriaClient(self.url) as client:
                return await client.query(sql)

        rows = _run_async(_query())
        return json.dumps(rows, indent=2, default=str)


class EcphoriaStateTool(BaseTool):
    """Get or set agent state in Ecphoria."""

    name: str = "ecphoria_state"
    description: str = (
        "Get or set agent state in Ecphoria's key-value store. "
        "Input format: 'get <agent_id> <key>' or 'set <agent_id> <key> <json_value>'. "
        "Examples: 'get analyst last_run', 'set reporter last_report {\"ts\": \"2024-01-01\"}'."
    )
    url: str = Field(default="http://localhost:8432")

    def _run(self, command: str) -> str:
        parts = command.strip().split(None, 3)
        if len(parts) < 3:
            return "Error: format is 'get <agent_id> <key>' or 'set <agent_id> <key> <value>'"

        action, agent_id, key = parts[0], parts[1], parts[2]

        async def _execute() -> Any:
            async with EcphoriaClient(self.url) as client:
                if action == "get":
                    return await client.state_get(agent_id, key)
                elif action == "set":
                    value = json.loads(parts[3]) if len(parts) > 3 else {}
                    version = await client.state_set(agent_id, key, value)
                    return {"version": version}
                else:
                    return {"error": f"Unknown action: {action}"}

        result = _run_async(_execute())
        return json.dumps(result, indent=2, default=str)


class EcphoriaIngestTool(BaseTool):
    """Ingest events into Ecphoria's episodic memory."""

    name: str = "ecphoria_ingest"
    description: str = (
        "Ingest a new event into Ecphoria's episodic memory. "
        "Input should be a JSON object with at least 'event_type' field. "
        "Example: '{\"event_type\": \"report\", \"content\": \"Summary of findings...\"}'"
    )
    url: str = Field(default="http://localhost:8432")
    source: str = Field(default="crewai")

    def _run(self, event_json: str) -> str:
        try:
            event = json.loads(event_json)
        except json.JSONDecodeError:
            # Treat as plain text content
            event = {"event_type": "note", "content": event_json}

        async def _ingest() -> int:
            async with EcphoriaClient(self.url) as client:
                return await client.ingest(self.source, [event])

        count = _run_async(_ingest())
        return f"Ingested {count} event(s)"
