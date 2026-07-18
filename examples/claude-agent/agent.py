"""
5-Minute Agent with Claude + Ecphoria

A simple conversational agent that uses Claude as the LLM and Ecphoria as its
persistent memory backend. Every message is ingested as an episodic event,
semantic search retrieves relevant past context, and key-value state tracks
the agent's evolving understanding of the conversation.

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python agent.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import anthropic
import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ECPHORIA_URL = os.environ.get("ECPHORIA_URL", "http://localhost:8432")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-20250514")
AGENT_ID = "claude-agent"
STATE_KEY = "context"

# ---------------------------------------------------------------------------
# Ecphoria client helpers
# ---------------------------------------------------------------------------


async def ecphoria_ingest(
    client: httpx.AsyncClient,
    event_type: str,
    payload: dict,
) -> int:
    """Ingest a single event into Ecphoria's episodic memory."""
    resp = await client.post(
        f"{ECPHORIA_URL}/api/v1/ingest",
        json={
            "source": AGENT_ID,
            "events": [{"event_type": event_type, **payload}],
        },
    )
    resp.raise_for_status()
    return resp.json().get("ingested", 0)


async def ecphoria_search(
    client: httpx.AsyncClient,
    text: str,
    k: int = 3,
) -> list[dict]:
    """Embed text and search semantic memory for related past events."""
    try:
        resp = await client.post(
            f"{ECPHORIA_URL}/api/v1/embed-and-search",
            json={"text": text, "k": k},
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
    except httpx.HTTPStatusError as exc:
        # Embedding provider may not be configured -- degrade gracefully.
        if exc.response.status_code == 503:
            return []
        raise


async def ecphoria_state_get(client: httpx.AsyncClient) -> dict | None:
    """Read the agent's current state from Ecphoria."""
    resp = await client.get(
        f"{ECPHORIA_URL}/api/v1/state/{AGENT_ID}/{STATE_KEY}"
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


async def ecphoria_state_set(client: httpx.AsyncClient, state: dict) -> None:
    """Write the agent's updated state back to Ecphoria."""
    resp = await client.put(
        f"{ECPHORIA_URL}/api/v1/state/{AGENT_ID}/{STATE_KEY}",
        json=state,
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------


def build_system_prompt(memories: list[dict], state: dict | None) -> str:
    """Assemble a system prompt with retrieved context and current state."""
    parts = [
        "You are a helpful assistant with persistent memory powered by Ecphoria.",
        "You remember past conversations and can reference them naturally.",
    ]

    if state:
        parts.append(f"\nYour current internal state: {json.dumps(state)}")

    if memories:
        parts.append("\nRelevant memories from past interactions:")
        for i, mem in enumerate(memories, 1):
            content = mem.get("content", "")
            score = mem.get("score", 0)
            parts.append(f"  {i}. (relevance {score:.2f}) {content}")

    parts.append(
        "\nUse your memories when they are relevant, but don't force them "
        "into the conversation. Be concise and natural."
    )
    return "\n".join(parts)


async def run_agent() -> None:
    """Main agent conversation loop."""
    if not ANTHROPIC_API_KEY:
        print("Error: set ANTHROPIC_API_KEY environment variable.")
        sys.exit(1)

    claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    async with httpx.AsyncClient(timeout=30.0) as http:
        # Quick health check against Ecphoria.
        try:
            health = await http.get(f"{ECPHORIA_URL}/health")
            health.raise_for_status()
            print(f"Connected to Ecphoria at {ECPHORIA_URL}")
        except httpx.ConnectError:
            print(f"Error: cannot reach Ecphoria at {ECPHORIA_URL}. Is it running?")
            sys.exit(1)

        print("Type a message (or 'quit' to exit).\n")
        decision_count = 0

        while True:
            try:
                user_input = input("You: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break

            if not user_input or user_input.lower() in ("quit", "exit"):
                print("Goodbye!")
                break

            # Step 1 -- Ingest the user message as an episodic event.
            await ecphoria_ingest(
                http,
                "user.message",
                {"payload": {"content": user_input}},
            )

            # Step 2 -- Search semantic memory for related context.
            memories = await ecphoria_search(http, user_input, k=3)

            # Step 3 -- Read current agent state.
            state = await ecphoria_state_get(http)

            # Step 4 -- Call Claude with context.
            system = build_system_prompt(memories, state)
            response = claude.messages.create(
                model=MODEL,
                max_tokens=1024,
                system=system,
                messages=[{"role": "user", "content": user_input}],
            )
            assistant_text = response.content[0].text

            # Step 5 -- Update agent state.
            decision_count += 1
            new_state = {
                "mood": "engaged",
                "topic": user_input[:80],
                "decision_count": decision_count,
                "last_summary": assistant_text[:200],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await ecphoria_state_set(http, new_state)

            # Step 6 -- Ingest the assistant response as an event.
            await ecphoria_ingest(
                http,
                "assistant.message",
                {"payload": {"content": assistant_text}},
            )

            # Step 7 -- Print.
            print(f"\nAgent: {assistant_text}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(run_agent())
