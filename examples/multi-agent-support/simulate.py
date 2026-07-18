"""
Simulation runner — generates sample tickets and runs all three agents.

Usage:
    python simulate.py
"""

from __future__ import annotations

import asyncio
import logging
import uuid

from agents.shared import EcphoriaClient
from agents import triage, support_l1, escalation_l2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("simulate")

SAMPLE_TICKETS = [
    {"subject": "Cannot login to my account", "content": "I keep getting 'invalid credentials' even after resetting my password. I'm locked out."},
    {"subject": "Payment failed on checkout", "content": "My credit card is being declined when I try to upgrade to the Pro plan. Payment failed."},
    {"subject": "API rate limiting too aggressive", "content": "We're hitting 429 errors on the API even though we're under our plan limits. This is a bug."},
    {"subject": "Dashboard loading slow", "content": "The analytics dashboard takes 30+ seconds to load. It used to be fast. Something is slow."},
    {"subject": "URGENT: Production outage", "content": "Our entire production environment is down. Complete outage since 3am. All services affected."},
    {"subject": "Webhook not firing", "content": "Our Stripe webhook integration stopped working yesterday. No events coming through."},
    {"subject": "Feature request: dark mode", "content": "Would love dark mode support in the dashboard. My eyes hurt during late night sessions."},
    {"subject": "Data export broken", "content": "CSV export is producing corrupted files. Cannot access our data. This is urgent for compliance."},
    {"subject": "Billing discrepancy", "content": "I was charged twice for the same invoice this month. Need a refund for the duplicate charge."},
    {"subject": "Security concern: suspicious login", "content": "I see login attempts from an IP I don't recognize. Possible security breach on my account."},
]

RUN_DURATION = 30  # seconds


async def generate_tickets(client: EcphoriaClient) -> list[str]:
    """Ingest sample tickets and return their IDs."""
    ticket_ids = []

    for ticket in SAMPLE_TICKETS:
        ticket_id = str(uuid.uuid4())
        ticket_ids.append(ticket_id)

        await client.ingest("customer", [{
            "event_type": "ticket.created",
            "payload": {
                "ticket_id": ticket_id,
                "subject": ticket["subject"],
                "content": ticket["content"],
            },
        }])
        log.info("Created ticket %s: %s", ticket_id[:8], ticket["subject"])

    return ticket_ids


async def run_agents(client: EcphoriaClient, duration: int) -> None:
    """Run all three agents concurrently for a fixed duration."""

    async def with_timeout(coro):
        try:
            await asyncio.wait_for(coro, timeout=duration)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(
        with_timeout(triage.run(client)),
        with_timeout(support_l1.run(client)),
        with_timeout(escalation_l2.run(client)),
    )


async def print_summary(client: EcphoriaClient, ticket_ids: list[str]) -> None:
    """Print final status of all tickets."""
    resolved = 0
    escalated = 0
    pending = 0
    triaged_only = 0

    print("\n" + "=" * 70)
    print("SIMULATION SUMMARY")
    print("=" * 70)

    for tid in ticket_ids:
        state = await client.get_state("triage", tid)
        if state is None:
            print(f"  {tid[:8]}  NOT PROCESSED")
            continue

        status = state.get("status", "unknown")
        priority = state.get("priority", "?")
        category = state.get("category", "?")
        assigned = state.get("assigned_to", "?")

        icon = {
            "resolved": "[OK]",
            "pending_human": "[L2]",
            "escalated": "[>>]",
            "triaged": "[..] ",
        }.get(status, "[??]")

        print(f"  {icon} {tid[:8]}  {priority:<9} {category:<12} → {status}")

        if status == "resolved":
            resolved += 1
        elif status == "pending_human":
            pending += 1
        elif status == "escalated":
            escalated += 1
        else:
            triaged_only += 1

    print("-" * 70)
    print(f"  Total: {len(ticket_ids)}  |  Auto-resolved: {resolved}  |  "
          f"Pending human: {pending}  |  Escalated: {escalated}  |  "
          f"In progress: {triaged_only}")
    print("=" * 70)


async def main() -> None:
    client = EcphoriaClient()

    if not await client.health():
        print("Error: cannot reach Ecphoria. Is it running?")
        print("Start it with: docker run -d -p 8432:8432 -p 5432:5432 "
              "ghcr.io/varga-foundation/ecphoria:latest")
        return

    log.info("Connected to Ecphoria — generating %d sample tickets", len(SAMPLE_TICKETS))
    ticket_ids = await generate_tickets(client)

    log.info("Starting agents for %d seconds...", RUN_DURATION)
    await run_agents(client, RUN_DURATION)

    await print_summary(client, ticket_ids)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
