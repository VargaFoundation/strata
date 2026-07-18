"""AutoGen + Ecphoria: Using Ecphoria as a persistent memory backend for AI agents.

This example demonstrates how AutoGen agents can use Ecphoria for:
- Retrieving relevant past context via semantic search
- Saving observations and findings to episodic memory
- Persisting agent state across conversations
"""

from __future__ import annotations

import asyncio
import json
from typing import Annotated, Any

import autogen
from ecphoria import EcphoriaClient

ECPHORIA_URL = "http://localhost:8432"

# ── Sample data ──────────────────────────────────────────────────

SAMPLE_KNOWLEDGE = [
    {
        "event_type": "knowledge",
        "topic": "architecture",
        "content": "Our payment service uses Stripe for processing. "
        "It handles webhooks via an async queue with Redis-backed idempotency.",
    },
    {
        "event_type": "knowledge",
        "topic": "architecture",
        "content": "The auth service uses JWT tokens with a 15-minute access token "
        "and 7-day refresh token. Sessions are cached in Redis with LRU eviction.",
    },
    {
        "event_type": "knowledge",
        "topic": "runbook",
        "content": "When payment timeouts spike: 1) Check Stripe status page, "
        "2) Look at DB connection pool metrics, 3) Check for N+1 queries in APM, "
        "4) Scale payment-worker horizontally if CPU > 80%.",
    },
    {
        "event_type": "knowledge",
        "topic": "team",
        "content": "The payments team owns the billing, invoicing, and payment-processing "
        "services. On-call rotation is weekly, paging via PagerDuty.",
    },
]


async def seed_ecphoria() -> None:
    """Ingest sample knowledge into Ecphoria."""
    async with EcphoriaClient(ECPHORIA_URL) as client:
        count = await client.ingest("knowledge-base", SAMPLE_KNOWLEDGE)
        print(f"Seeded {count} knowledge entries into Ecphoria")


# ── Ecphoria-backed functions for AutoGen ──────────────────────────

def _run_async(coro: Any) -> Any:
    """Run an async coroutine from sync context."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def recall_context(
    query: Annotated[str, "Natural language query to search memory"],
    k: Annotated[int, "Number of results to return"] = 5,
) -> str:
    """Search Ecphoria's memory for relevant past context using semantic search."""
    async def _search() -> list[dict[str, Any]]:
        async with EcphoriaClient(ECPHORIA_URL) as client:
            return await client.find(query, k=k)

    results = _run_async(_search())
    if not results:
        return "No relevant context found in memory."
    return json.dumps(results, indent=2, default=str)


def save_memory(
    content: Annotated[str, "The content to save"],
    event_type: Annotated[str, "Type of event (e.g., 'observation', 'finding', 'note')"] = "observation",
) -> str:
    """Save an observation or finding to Ecphoria's episodic memory for future recall."""
    async def _ingest() -> int:
        async with EcphoriaClient(ECPHORIA_URL) as client:
            return await client.ingest("autogen", [
                {"event_type": event_type, "content": content}
            ])

    count = _run_async(_ingest())
    return f"Saved to memory ({count} event ingested)"


def get_agent_state(
    agent_id: Annotated[str, "Agent identifier"],
    key: Annotated[str, "State key to retrieve"],
) -> str:
    """Retrieve persistent agent state from Ecphoria."""
    async def _get() -> Any:
        async with EcphoriaClient(ECPHORIA_URL) as client:
            return await client.state_get(agent_id, key)

    result = _run_async(_get())
    if result is None:
        return f"No state found for {agent_id}/{key}"
    return json.dumps(result, indent=2, default=str)


def set_agent_state(
    agent_id: Annotated[str, "Agent identifier"],
    key: Annotated[str, "State key"],
    value: Annotated[str, "JSON value to store"],
) -> str:
    """Persist agent state in Ecphoria for future conversations."""
    async def _set() -> int:
        parsed = json.loads(value) if value.startswith(("{", "[", '"')) else value
        async with EcphoriaClient(ECPHORIA_URL) as client:
            return await client.state_set(agent_id, key, parsed)

    version = _run_async(_set())
    return f"State saved (version {version})"


# ── AutoGen setup ────────────────────────────────────────────────

def build_agents() -> tuple[autogen.AssistantAgent, autogen.UserProxyAgent]:
    llm_config = {
        "config_list": [{"model": "gpt-4o-mini", "api_key": "YOUR_KEY"}],
        "temperature": 0,
    }

    assistant = autogen.AssistantAgent(
        name="ecphoria_assistant",
        system_message=(
            "You are a helpful assistant with access to a persistent memory system (Ecphoria). "
            "Before answering questions, use recall_context to search for relevant past knowledge. "
            "After making important observations, use save_memory to persist them for future use. "
            "Use get_agent_state and set_agent_state to maintain your own persistent state."
        ),
        llm_config=llm_config,
    )

    user_proxy = autogen.UserProxyAgent(
        name="user",
        human_input_mode="TERMINATE",
        max_consecutive_auto_reply=5,
        code_execution_config=False,
    )

    # Register Ecphoria functions with both agents
    for func in [recall_context, save_memory, get_agent_state, set_agent_state]:
        assistant.register_for_llm(description=func.__doc__)(func)
        user_proxy.register_for_execution()(func)

    return assistant, user_proxy


def main() -> None:
    # Seed data
    asyncio.run(seed_ecphoria())

    # Build agents
    assistant, user_proxy = build_agents()

    # Start conversation
    user_proxy.initiate_chat(
        assistant,
        message=(
            "I'm investigating payment timeout issues. "
            "Can you search our memory for relevant context about payments "
            "and timeouts, then summarize what you find?"
        ),
    )


if __name__ == "__main__":
    main()
