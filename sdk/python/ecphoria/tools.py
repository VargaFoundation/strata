"""Framework-agnostic agent tools backed by Ecphoria.

These are plain async callables you can register with any agent framework's function-tool
mechanism — the OpenAI Agents SDK (``@function_tool``), Pydantic AI (``Tool``), LangChain
(``StructuredTool``), CrewAI, etc. Each takes a :class:`~ecphoria.client.EcphoriaClient`; bind it with
:func:`memory_toolset` or ``functools.partial``.

Example — OpenAI Agents SDK::

    from agents import function_tool
    from ecphoria import EcphoriaClient
    from ecphoria.tools import search_memory, remember

    client = EcphoriaClient("http://localhost:8432")

    @function_tool
    async def memory_search(query: str) -> list:
        '''Search the user's long-term memory.'''
        return await search_memory(client, query)

Example — Pydantic AI::

    from pydantic_ai import Agent, Tool
    from ecphoria import EcphoriaClient
    from ecphoria.tools import memory_toolset

    tools = memory_toolset(EcphoriaClient("http://localhost:8432"))
    agent = Agent("openai:gpt-4o", tools=[Tool(tools["search_memory"]), Tool(tools["remember"])])
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Optional

from .client import EcphoriaClient


async def search_memory(
    client: EcphoriaClient,
    query: str,
    *,
    user_id: Optional[str] = None,
    k: int = 5,
) -> list[dict[str, Any]]:
    """Search the agent's long-term memory for facts relevant to ``query`` (hybrid retrieval)."""
    return await client.memory_search(query, k=k, user_id=user_id)


async def remember(
    client: EcphoriaClient,
    content: str,
    *,
    user_id: Optional[str] = None,
    subject: Optional[str] = None,
) -> dict[str, Any]:
    """Store a fact in long-term memory. Deduped; a newer fact about the same ``subject``
    supersedes the old one (kept as history)."""
    return await client.memory_add(content, user_id=user_id, subject=subject)


async def run_subagent(
    client: EcphoriaClient, agent_id: str, question: str
) -> dict[str, Any]:
    """Delegate a question to a durable Ecphoria sub-agent; returns its run (status + result)."""
    return await client.run_agent(agent_id, question)


def memory_toolset(client: EcphoriaClient) -> dict[str, Callable[..., Any]]:
    """Bind the tools to a client, returning ``{name: async_callable}`` — framework-agnostic."""
    return {
        "search_memory": functools.partial(search_memory, client),
        "remember": functools.partial(remember, client),
        "run_subagent": functools.partial(run_subagent, client),
    }
