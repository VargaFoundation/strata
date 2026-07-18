# Agentic platform

Ecphoria is not just an agent **memory** engine — it also **runs and remembers** your agents. On top
of the memory substrate (episodic + semantic + state + cognition) it adds a durable, HA, observable
**agent runtime**: runs, a built-in agent loop, sub-agent workflows, human-in-the-loop approvals,
event triggers, a governed MCP tool-gateway, and Prometheus metrics — all reusing the same single
binary, Raft replication, auth/RBAC/audit, and multi-tenancy.

> "The memory engine that also runs — and remembers — your agents."

## Concepts

- **Run** — one execution of an agent/workflow: `status` (pending → running → waiting_approval →
  succeeded/failed/cancelled), `input`/`result`/`cursor`, `parent_run_id` (sub-agent tree). Durable
  (SQLite) and **replicated through Raft** (survives leader failover).
- **Step** — an LLM call, tool call, or HITL request. Steps are episodic events tagged with the run
  id, so the full trace is just `GET /runs/{id}/trace` (and is queryable in SQL).
- **Agent loop** — `run_agent` drives an LLM↔tool loop until the model answers, pauses for approval,
  or hits max turns, journaling every step. It is re-entrant: a paused run **resumes** from its
  journaled trace.

## REST API

```bash
# Create a run (the durable record); steps are appended as the agent works.
curl -X POST localhost:8432/api/v1/runs -d '{"agent_id":"support","input":{"q":"refund?"}}'

# Run an agent end-to-end (durable LLM↔tool loop). Needs a completion provider.
curl -X POST localhost:8432/api/v1/agents/run -d '{"agent_id":"support","question":"What plan is cust_42 on?"}'

# Inspect runs + their step trace.
curl localhost:8432/api/v1/runs
curl localhost:8432/api/v1/runs/<id>
curl localhost:8432/api/v1/runs/<id>/trace
curl -X POST localhost:8432/api/v1/runs/<id>/cancel

# Human-in-the-loop: a paused run (status waiting_approval) resumes after approval.
curl -X POST localhost:8432/api/v1/runs/<id>/approve  -d '{"approve":true}'
curl -X POST localhost:8432/api/v1/runs/<id>/resume

# Event triggers: a matching event (e.g. a webhook) starts an agent run.
curl -X POST localhost:8432/api/v1/triggers -d '{"name":"on_pr","source":"github","event_type":"pull_request.opened","agent_id":"reviewer"}'
curl -X POST localhost:8432/api/v1/webhook/github -d '{ ...github payload... }'   # → "triggered_runs": [...]

# MCP tool-gateway: register downstream MCP servers and call their tools (governed by auth/RBAC/audit).
curl -X POST localhost:8432/api/v1/tools -d '{"name":"github","url":"http://gh-mcp:9001"}'
curl localhost:8432/api/v1/tools
curl -X POST localhost:8432/api/v1/tools/github/call -d '{"tool":"create_issue","arguments":{"title":"bug"}}'
```

## SDK (Python)

```python
from ecphoria import EcphoriaClient

async with EcphoriaClient("http://localhost:8432") as s:
    run = await s.run_agent("support", "What plan is cust_42 on?")
    print(run["status"], run["result"])
    trace = await s.run_trace(run["id"])           # LLM/tool/HITL steps

    # HITL
    if run["status"] == "waiting_approval":
        await s.run_approve(run["id"], True)
        run = await s.run_resume(run["id"])

    # Event-driven agents
    await s.trigger_register("on_pr", "reviewer", source="github", event_type="pull_request.opened")

    # Downstream MCP tools
    await s.tool_register("github", "http://gh-mcp:9001")
    result = await s.tool_call("github", "create_issue", {"title": "bug"})
```

The TypeScript SDK (`@ecphoria/client`) exposes the same surface (`runAgent`, `runApprove`,
`runResume`, `triggerRegister`, `toolCall`, …).

## Workflows (DAG + sub-agents)

`engine.run_workflow(tenant, nodes)` runs a DAG of sub-agents: each `WorkflowNode { id, agent_id,
question, deps }` runs once its `deps` complete (topological order), as a child run linked via
`parent_run_id`. The parent run aggregates the children and succeeds/fails accordingly.

## Observability

Prometheus at `/metrics`: `ecphoria_runs_created_total`, `ecphoria_runs_completed_total{status}`,
`ecphoria_run_steps_total{type}`. Because steps are episodic events, run analytics (cost, latency,
success rate per agent) are also plain SQL over the episodic store.

## What's HA vs leader-local

The run **ledger** (create/update) replicates through Raft (`RunCreate`/`RunUpdate`) and converges
on every node — verified by a 3-node in-process test. The agent **loop driver** currently runs on
the leader and writes its steps locally; replicating each step through the log (so a failover mid-run
resumes exactly) is the next increment.
