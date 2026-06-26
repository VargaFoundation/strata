# Connecting Claude to Strata

Strata gives Claude **persistent, self-hosted memory**. There are three ways to wire them
together. They're listed best-first — and honestly, including current limitations.

> When `gateway.auth_enabled = true`, every path below needs `Authorization: Bearer <token>`
> (an API key or a JWT). With a tenant-scoped JWT, Claude only ever sees that tenant's data.

---

## 1. REST + Claude tool-use  ✅ recommended (works today, ~5 minutes)

Define Anthropic tools that call Strata's REST API. This is the most robust path and needs no
proxy. A complete working example lives in [`examples/claude-agent/`](../examples/claude-agent/).

```python
import anthropic, httpx

STRATA = "http://localhost:8432"
client = anthropic.Anthropic()

tools = [
    {
        "name": "remember",
        "description": "Remember a fact about the user for future conversations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "user_id": {"type": "string"},
            },
            "required": ["text", "user_id"],
        },
    },
    {
        "name": "recall",
        "description": "Search what we remember about the user.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "user_id": {"type": "string"},
            },
            "required": ["query", "user_id"],
        },
    },
]

def run_tool(name, args):
    if name == "remember":
        return httpx.post(f"{STRATA}/api/v1/memories", json={
            "content": args["text"], "user_id": args["user_id"]}).json()
    if name == "recall":
        return httpx.post(f"{STRATA}/api/v1/memories/search", json={
            "query": args["query"], "user_id": args["user_id"]}).json()

# Standard Anthropic tool-use loop: call client.messages.create(model=..., tools=tools, ...),
# execute any tool_use blocks via run_tool(), feed results back as tool_result. Done.
```

Bi-temporal bonus: `GET /api/v1/memories/{id}/history` shows every superseded version of a
memory — "what did we believe, and when".

---

## 2. MCP  ✅ Streamable HTTP — Claude Code and Claude Desktop both connect natively

Strata exposes an MCP **Streamable HTTP** endpoint at `/mcp`: `POST` for JSON-RPC 2.0
(`initialize` — which returns an `Mcp-Session-Id` header — `tools/list`, `tools/call`,
`resources/list`, `prompts/list`) and `GET` for the server→client SSE stream. It includes 6
memory tools — `add_memory`, `search_memory`, `get_memories`, `memory_history`, `delete_memory`,
`remember` — plus query / ingest / state / session tools.

**Claude Code** (HTTP MCP) — add to your MCP config:
```json
{ "mcpServers": { "strata": { "url": "http://localhost:8432/mcp" } } }
```

**Claude Desktop** — it speaks MCP Streamable HTTP (HTTP GET + SSE + `Mcp-Session-Id`), which
Strata now serves, so point it at the URL directly. (If your client only supports stdio MCP, the
[`mcp-remote`](https://github.com/geelen/mcp-remote) bridge —
`npx -y mcp-remote http://localhost:8432/mcp` — still works.)

> Note: Strata is a stateless tool server, so the GET/SSE stream is an idle keep-alive (no
> server-initiated notifications) — sufficient for the request/response tool calls clients make.

---

## 3. LLM proxy (auto-RAG)  ✅ great for chat; SSE streaming supported

Point any OpenAI client at Strata and ask for a `claude-*` model. Strata injects relevant
memories (scoped by the OpenAI `user` field) and forwards to Anthropic. Set `ANTHROPIC_API_KEY`
in the server's environment.

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8432/v1", api_key="unused")
resp = client.chat.completions.create(
    model="claude-sonnet-4-6",
    user="cust_42",                       # → memories for this user are auto-injected
    messages=[{"role": "user", "content": "what plan am I on?"}],
)
```

What works: format translation (OpenAI ↔ Anthropic), auto-RAG, semantic response cache,
**single-turn tool-use** (`tools`/`tool_choice` are translated; `tool_calls` come back), and
**SSE streaming** — `stream: true` returns `text/event-stream`; for Claude, Anthropic's streaming
events are translated into OpenAI `chat.completion.chunk`s (text deltas + finish_reason),
terminated by `data: [DONE]` (caching is skipped while streaming).

Limitations (honest):
- **Streaming tool-use deltas** aren't translated yet — text streams fine; for streamed tool calls
  use path 1.
- **Multi-turn tool-result passing** through the proxy is not yet supported — use path 1 for
  agentic loops.
