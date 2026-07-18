"""L1 Support agent — resolves tickets using semantic search for similar past issues."""

from __future__ import annotations

import asyncio
import logging

from .shared import POLL_INTERVAL, EcphoriaClient

log = logging.getLogger("support-l1")

SIMILARITY_THRESHOLD = 0.80


async def handle_ticket(client: EcphoriaClient, ticket_id: str, info: dict) -> None:
    subject = info.get("subject", "")
    category = info.get("category", "general")

    # Search for similar past resolutions.
    query = f"{category} {subject}"
    results = await client.search(query, k=3)

    similar = [r for r in results if r.get("score", 0) >= SIMILARITY_THRESHOLD]

    if similar:
        best = similar[0]
        resolution = best.get("content", "No content")
        score = best.get("score", 0)

        log.info(
            "Ticket %s — auto-resolved (score=%.2f): %s",
            ticket_id, score, resolution[:80],
        )

        await client.set_state("triage", ticket_id, {
            **info,
            "status": "resolved",
            "assigned_to": "l1",
            "resolution": resolution[:500],
            "resolution_score": score,
        })

        await client.ingest("l1-agent", [{
            "event_type": "ticket.resolved",
            "payload": {
                "ticket_id": ticket_id,
                "resolution": resolution[:500],
                "similarity_score": score,
                "method": "auto",
            },
        }])
    else:
        log.info("Ticket %s — no similar resolution, escalating to L2", ticket_id)

        await client.set_state("triage", ticket_id, {
            **info,
            "status": "escalated",
            "assigned_to": "l2",
            "l1_note": "No similar past resolution found",
        })

        await client.ingest("l1-agent", [{
            "event_type": "ticket.escalated",
            "payload": {
                "ticket_id": ticket_id,
                "reason": "no_similar_resolution",
                "search_results_count": len(results),
                "best_score": results[0].get("score", 0) if results else 0,
            },
        }])


async def run(client: EcphoriaClient | None = None) -> None:
    own_client = client is None
    if own_client:
        client = EcphoriaClient()

    log.info("L1 Support agent started — polling for assigned tickets")
    processed: set[str] = set()

    try:
        while True:
            rows = await client.query(
                "SELECT id FROM episodic "
                "WHERE source = 'triage-agent' AND event_type = 'ticket.triaged' "
                "ORDER BY ts DESC LIMIT 50"
            )

            for row in rows:
                # The payload contains the ticket_id.
                tid_event = row.get("id", "")
                if tid_event in processed:
                    continue
                processed.add(tid_event)

                # Read triage state to find tickets assigned to L1.
                # We need to get the ticket_id from the triage event.
                detail = await client.query(
                    f"SELECT payload FROM episodic WHERE id = '{tid_event}' LIMIT 1"
                )
                if not detail:
                    continue

                payload = detail[0].get("payload", {})
                if isinstance(payload, str):
                    import json
                    payload = json.loads(payload)

                ticket_id = payload.get("ticket_id", "")
                if not ticket_id:
                    continue

                info = await client.get_state("triage", ticket_id)
                if info is None:
                    continue

                if info.get("assigned_to") != "l1" or info.get("status") != "triaged":
                    continue

                await handle_ticket(client, ticket_id, info)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        if own_client:
            await client.close()


if __name__ == "__main__":
    asyncio.run(run())
