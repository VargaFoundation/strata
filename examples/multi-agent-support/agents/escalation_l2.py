"""L2 Escalation agent — gathers full context for human review."""

from __future__ import annotations

import asyncio
import logging

from .shared import POLL_INTERVAL, EcphoriaClient

log = logging.getLogger("escalation-l2")


async def handle_ticket(client: EcphoriaClient, ticket_id: str, info: dict) -> None:
    # Gather the original ticket.
    original = await client.query(
        f"SELECT ts, payload FROM episodic "
        f"WHERE source = 'customer' AND event_type = 'ticket.created' "
        f"ORDER BY ts DESC LIMIT 50"
    )

    ticket_content = ""
    for row in original:
        payload = row.get("payload", {})
        if isinstance(payload, str):
            import json
            payload = json.loads(payload)
        if payload.get("ticket_id") == ticket_id or row.get("id") == ticket_id:
            ticket_content = payload.get("content", "")
            break

    # Gather triage history.
    triage_events = await client.query(
        "SELECT ts, payload FROM episodic "
        "WHERE source = 'triage-agent' "
        "ORDER BY ts DESC LIMIT 20"
    )

    # Gather L1 attempt history.
    l1_events = await client.query(
        "SELECT ts, payload FROM episodic "
        "WHERE source = 'l1-agent' "
        "ORDER BY ts DESC LIMIT 20"
    )

    context_summary = {
        "ticket_id": ticket_id,
        "original_content": ticket_content[:500],
        "triage": {
            "priority": info.get("priority"),
            "category": info.get("category"),
        },
        "l1_note": info.get("l1_note", "Direct escalation — skipped L1"),
        "triage_events_found": len(triage_events),
        "l1_events_found": len(l1_events),
    }

    log.info(
        "Ticket %s — full context gathered, marking pending_human (priority=%s)",
        ticket_id, info.get("priority"),
    )

    await client.set_state("triage", ticket_id, {
        **info,
        "status": "pending_human",
        "assigned_to": "l2",
        "context_summary": context_summary,
    })

    await client.ingest("l2-agent", [{
        "event_type": "ticket.pending_human",
        "payload": context_summary,
    }])


async def run(client: EcphoriaClient | None = None) -> None:
    own_client = client is None
    if own_client:
        client = EcphoriaClient()

    log.info("L2 Escalation agent started — polling for escalated tickets")
    processed: set[str] = set()

    try:
        while True:
            # Find tickets escalated to L2 (from L1 or triage).
            rows = await client.query(
                "SELECT payload FROM episodic "
                "WHERE (source = 'l1-agent' AND event_type = 'ticket.escalated') "
                "   OR (source = 'triage-agent' AND event_type = 'ticket.triaged') "
                "ORDER BY ts DESC LIMIT 50"
            )

            for row in rows:
                payload = row.get("payload", {})
                if isinstance(payload, str):
                    import json
                    payload = json.loads(payload)

                ticket_id = payload.get("ticket_id", "")
                if not ticket_id or ticket_id in processed:
                    continue

                info = await client.get_state("triage", ticket_id)
                if info is None:
                    continue

                # Process tickets assigned to L2 that are triaged or escalated.
                if info.get("assigned_to") != "l2":
                    continue
                if info.get("status") not in ("triaged", "escalated"):
                    continue

                processed.add(ticket_id)
                await handle_ticket(client, ticket_id, info)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        if own_client:
            await client.close()


if __name__ == "__main__":
    asyncio.run(run())
