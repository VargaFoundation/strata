"""Triage agent — classifies and routes incoming support tickets."""

from __future__ import annotations

import asyncio
import logging

from .shared import POLL_INTERVAL, EcphoriaClient

log = logging.getLogger("triage")

# Simple keyword rules for classification.
CRITICAL_KEYWORDS = ["outage", "down", "data loss", "security", "breach", "crash"]
HIGH_KEYWORDS = ["urgent", "broken", "cannot access", "payment failed", "locked out"]
MEDIUM_KEYWORDS = ["slow", "error", "bug", "issue", "problem", "not working"]

CATEGORY_KEYWORDS = {
    "billing": ["billing", "invoice", "charge", "payment", "subscription", "refund"],
    "account": ["login", "password", "access", "locked", "account", "2fa"],
    "technical": ["api", "integration", "webhook", "error", "bug", "crash"],
    "general": [],
}


def classify_priority(text: str) -> str:
    lower = text.lower()
    if any(kw in lower for kw in CRITICAL_KEYWORDS):
        return "critical"
    if any(kw in lower for kw in HIGH_KEYWORDS):
        return "high"
    if any(kw in lower for kw in MEDIUM_KEYWORDS):
        return "medium"
    return "low"


def classify_category(text: str) -> str:
    lower = text.lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return cat
    return "general"


async def process_ticket(client: EcphoriaClient, ticket: dict) -> None:
    ticket_id = ticket.get("id", "unknown")
    content = ticket.get("payload", {}).get("content", "")
    subject = ticket.get("payload", {}).get("subject", "")
    text = f"{subject} {content}"

    priority = classify_priority(text)
    category = classify_category(text)
    assigned_to = "l2" if priority in ("critical", "high") else "l1"

    log.info(
        "Ticket %s → priority=%s category=%s → %s",
        ticket_id, priority, category, assigned_to,
    )

    # Store triage result in state.
    await client.set_state("triage", ticket_id, {
        "priority": priority,
        "category": category,
        "assigned_to": assigned_to,
        "subject": subject,
        "status": "triaged",
    })

    # Ingest triage event.
    await client.ingest("triage-agent", [{
        "event_type": "ticket.triaged",
        "payload": {
            "ticket_id": ticket_id,
            "priority": priority,
            "category": category,
            "assigned_to": assigned_to,
        },
    }])


async def run(client: EcphoriaClient | None = None) -> None:
    own_client = client is None
    if own_client:
        client = EcphoriaClient()

    log.info("Triage agent started — polling for new tickets")
    seen: set[str] = set()

    try:
        while True:
            rows = await client.query(
                "SELECT id, event_type, payload, ts FROM episodic "
                "WHERE source = 'customer' AND event_type = 'ticket.created' "
                "ORDER BY ts DESC LIMIT 50"
            )

            for row in rows:
                tid = row.get("id", "")
                if tid in seen:
                    continue
                seen.add(tid)

                # Check if already triaged.
                existing = await client.get_state("triage", tid)
                if existing is not None:
                    continue

                await process_ticket(client, row)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        if own_client:
            await client.close()


if __name__ == "__main__":
    asyncio.run(run())
