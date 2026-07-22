import { EcphoriaError } from "./errors.js";
import type {
  BackupResponse,
  ClusterStatus,
  Event,
  FindRequest,
  HealthResponse,
  IngestResponse,
  MemoryScope,
  QueryResponse,
  RetentionResponse,
  SearchFilters,
  SearchRequest,
  SearchResult,
  StateEntry,
  StateSetResponse,
  EcphoriaApiError,
  EcphoriaClientOptions,
} from "./types.js";

/**
 * Ecphoria client — fetch-based HTTP client for the Ecphoria agentic memory platform API.
 *
 * Zero runtime dependencies. Uses the global `fetch` API (Node 18+, Deno, Bun, browsers).
 *
 * @example
 * ```ts
 * const client = new EcphoriaClient({ url: "http://localhost:8432" });
 *
 * // Ingest events
 * const count = await client.ingest("my-app", [
 *   { event_type: "user.signup", user_id: "u1" },
 * ]);
 *
 * // Query with SQL
 * const rows = await client.query("SELECT * FROM episodic LIMIT 10");
 *
 * // Semantic search by text
 * const results = await client.find("frustrated customer", { k: 5 });
 *
 * // Agent state
 * await client.stateSet("bot-1", "mood", "happy");
 * const entry = await client.stateGet("bot-1", "mood");
 * ```
 */
export class EcphoriaClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeout: number;

  constructor(options: EcphoriaClientOptions = {}) {
    this.baseUrl = (options.url ?? "http://localhost:8432").replace(/\/+$/, "");
    this.headers = { "Content-Type": "application/json" };
    if (options.apiKey) {
      this.headers["Authorization"] = `Bearer ${options.apiKey}`;
    }
    this.timeout = options.timeout ?? 30_000;
  }

  // ── Internal helpers ─────────────────────────────────────────────

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    try {
      const resp = await fetch(url, {
        method,
        headers: this.headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      if (!resp.ok) {
        let apiErr: EcphoriaApiError | undefined;
        try {
          const json = await resp.json();
          if (json && typeof json === "object" && "message" in json) {
            apiErr = json as EcphoriaApiError;
          }
        } catch {
          // not JSON
        }
        if (apiErr) {
          throw EcphoriaError.fromApiError(apiErr, resp.status);
        }
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }

      return (await resp.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  private async get<T>(path: string): Promise<T> {
    return this.request<T>("GET", path);
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("POST", path, body);
  }

  private async put<T>(path: string, body: unknown): Promise<T> {
    return this.request<T>("PUT", path, body);
  }

  private async del(path: string): Promise<void> {
    const url = `${this.baseUrl}${path}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "DELETE",
        headers: this.headers,
        signal: controller.signal,
      });
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
    } finally {
      clearTimeout(timer);
    }
  }

  // ── Health ───────────────────────────────────────────────────────

  /** Check server health. */
  async health(): Promise<HealthResponse> {
    return this.get<HealthResponse>("/health");
  }

  // ── Query ────────────────────────────────────────────────────────

  /** Execute a SQL query against the episodic store. Returns row dicts. */
  async query(sql: string): Promise<Record<string, unknown>[]> {
    const data = await this.post<QueryResponse>("/api/v1/query", { sql });
    return data.rows ?? [];
  }

  // ── Ingest ───────────────────────────────────────────────────────

  /** Ingest events into episodic memory. Returns the count of events ingested. */
  async ingest(source: string, events: Event[]): Promise<number> {
    const data = await this.post<IngestResponse>("/api/v1/ingest", {
      source,
      events,
    });
    return data.ingested ?? 0;
  }

  // ── Search ───────────────────────────────────────────────────────

  /** Semantic search by pre-computed vector. */
  async search(
    vector: number[],
    options: { k?: number; filters?: SearchFilters } = {},
  ): Promise<SearchResult[]> {
    const body: SearchRequest = { vector, k: options.k ?? 5 };
    if (options.filters) body.filters = options.filters;
    const data = await this.post<{ results: SearchResult[] }>("/api/v1/search", body);
    return data.results ?? [];
  }

  /** Semantic search by natural language text (embed + search in one call). */
  async find(
    text: string,
    options: { k?: number; filters?: SearchFilters } = {},
  ): Promise<SearchResult[]> {
    const body: FindRequest = { text, k: options.k ?? 5 };
    if (options.filters) body.filters = options.filters;
    const data = await this.post<{ results: SearchResult[] }>(
      "/api/v1/embed-and-search",
      body,
    );
    return data.results ?? [];
  }

  // ── State ────────────────────────────────────────────────────────

  /** Get agent state. Returns null if not found. */
  async stateGet(agentId: string, key: string): Promise<StateEntry | null> {
    const url = `${this.baseUrl}/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        headers: this.headers,
        signal: controller.signal,
      });
      if (resp.status === 404) return null;
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return (await resp.json()) as StateEntry;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Set agent state. Returns the new version number. */
  async stateSet(agentId: string, key: string, value: unknown): Promise<number> {
    const data = await this.put<StateSetResponse>(
      `/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`,
      value,
    );
    return data.version ?? 0;
  }

  /** Delete agent state. */
  async stateDelete(agentId: string, key: string): Promise<void> {
    await this.del(
      `/api/v1/state/${encodeURIComponent(agentId)}/${encodeURIComponent(key)}`,
    );
  }

  // ── Schema ───────────────────────────────────────────────────────

  /** List all event sources. */
  async sources(): Promise<string[]> {
    const data = await this.get<{ sources: string[] }>("/api/v1/schema/sources");
    return data.sources ?? [];
  }

  /** List all agent IDs. */
  async agents(): Promise<string[]> {
    const data = await this.get<{ agents: string[] }>("/api/v1/schema/agents");
    return data.agents ?? [];
  }

  // ── Admin ────────────────────────────────────────────────────────

  /** Trigger a backup of all stores. */
  async backup(): Promise<BackupResponse> {
    return this.post<BackupResponse>("/api/v1/admin/backup", {});
  }

  /** Enforce data retention policy. */
  async enforceRetention(): Promise<RetentionResponse> {
    return this.post<RetentionResponse>("/api/v1/admin/retention", {});
  }

  // ── Memory (cognition layer) ─────────────────────────────────────

  /** Add a memory through the cognition pipeline (dedup / contradiction / importance). */
  async memoryAdd(
    content: string,
    opts: MemoryScope & {
      subject?: string;
      importance?: number;
      metadata?: Record<string, unknown>;
    } = {},
  ): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>("/api/v1/memories", { content, ...opts });
  }

  /** Hybrid (BM25 + vector) search over a scope's memories. Returns ranked hits. */
  async memorySearch(
    query: string,
    opts: MemoryScope & { k?: number } = {},
  ): Promise<Record<string, unknown>[]> {
    const { k = 5, ...scope } = opts;
    const data = await this.post<{ results: Record<string, unknown>[] }>(
      "/api/v1/memories/search",
      { query, k, ...scope },
    );
    return data.results ?? [];
  }

  /** List active memories in a scope. */
  async memoryList(
    opts: MemoryScope & {
      limit?: number;
      offset?: number;
      mem_type?: string;
      min_importance?: number;
      updated_after?: string;
      updated_before?: string;
      metadata_key?: string;
      metadata_value?: string;
    } = {},
  ): Promise<Record<string, unknown>[]> {
    const params = new URLSearchParams();
    params.set("limit", String(opts.limit ?? 50));
    params.set("offset", String(opts.offset ?? 0));
    for (const key of ["tenant_id", "user_id", "agent_id", "session_id"] as const) {
      const v = opts[key];
      if (v !== undefined) params.set(key, v);
    }
    for (const key of [
      "mem_type",
      "updated_after",
      "updated_before",
      "metadata_key",
      "metadata_value",
    ] as const) {
      const v = opts[key];
      if (v !== undefined) params.set(key, v);
    }
    if (opts.min_importance !== undefined) {
      params.set("min_importance", String(opts.min_importance));
    }
    const data = await this.get<{ memories: Record<string, unknown>[] }>(
      `/api/v1/memories?${params.toString()}`,
    );
    return data.memories ?? [];
  }

  /**
   * Partially correct a memory — only the provided fields change (content is re-embedded).
   * Returns the updated memory, or null if it doesn't exist (or isn't in your tenant). The
   * subject is not editable: to change what a memory is about, add a new one.
   */
  async memoryUpdate(
    id: string,
    patch: {
      content?: string;
      importance?: number;
      mem_type?: string;
      metadata?: Record<string, unknown>;
    },
  ): Promise<Record<string, unknown> | null> {
    const url = `${this.baseUrl}/api/v1/memories/${encodeURIComponent(id)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "PATCH",
        headers: this.headers,
        body: JSON.stringify(patch),
        signal: controller.signal,
      });
      if (resp.status === 404) return null;
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return (await resp.json()) as Record<string, unknown>;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Get a memory by id. Returns null if not found (or not in your tenant). */
  async memoryGet(id: string): Promise<Record<string, unknown> | null> {
    const url = `${this.baseUrl}/api/v1/memories/${encodeURIComponent(id)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, { headers: this.headers, signal: controller.signal });
      if (resp.status === 404) return null;
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return (await resp.json()) as Record<string, unknown>;
    } finally {
      clearTimeout(timer);
    }
  }

  /** Bi-temporal history for a memory's subject (oldest first). */
  async memoryHistory(id: string): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ history: Record<string, unknown>[] }>(
      `/api/v1/memories/${encodeURIComponent(id)}/history`,
    );
    return data.history ?? [];
  }

  /** Delete a memory by id. Returns false if it didn't exist (or not in your tenant). */
  async memoryDelete(id: string): Promise<boolean> {
    const url = `${this.baseUrl}/api/v1/memories/${encodeURIComponent(id)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "DELETE",
        headers: this.headers,
        signal: controller.signal,
      });
      if (resp.status === 404) return false;
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return true;
    } finally {
      clearTimeout(timer);
    }
  }

  // ── Cognition (provenance / feedback / contradictions) ───────────

  /** A memory's source events + supersession chain (the audit trail behind a fact). */
  async memoryProvenance(id: string): Promise<Record<string, unknown>> {
    return this.get(`/api/v1/memories/${encodeURIComponent(id)}/provenance`);
  }

  /** Feedback so ranking learns: 'helpful' reinforces, 'wrong'/'obsolete' retires. */
  async memoryFeedback(id: string, verdict: string): Promise<Record<string, unknown>> {
    return this.post(`/api/v1/memories/${encodeURIComponent(id)}/feedback`, { verdict });
  }

  /** Subjects with more than one active memory (the review queue). */
  async memoryContradictions(userId?: string): Promise<Record<string, unknown>[]> {
    const q = userId ? `?user_id=${encodeURIComponent(userId)}` : "";
    const data = await this.get<{ contradictions: Record<string, unknown>[] }>(
      `/api/v1/memories/contradictions${q}`,
    );
    return data.contradictions ?? [];
  }

  /** Resolve a contradiction: keep `keepId`, supersede the rest for `subject`. */
  async memoryResolveContradiction(
    subject: string,
    keepId: string,
    userId?: string,
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { subject, keep_id: keepId };
    if (userId) body.user_id = userId;
    return this.post("/api/v1/memories/contradictions/resolve", body);
  }

  // ── Knowledge graph ──────────────────────────────────────────────

  /** Add a graph edge (src -[relation]-> dst). supersede closes the prior (src, relation). */
  async memoryLink(
    src: string,
    relation: string,
    dst: string,
    supersede = false,
  ): Promise<Record<string, unknown>> {
    return this.post("/api/v1/memories/link", { src, relation, dst, supersede });
  }

  /** Edges around an entity (depth>1 expands the subgraph). */
  async graphNeighbors(
    entity: string,
    depth = 1,
    limit = 50,
  ): Promise<Record<string, unknown>[]> {
    const p = new URLSearchParams({
      entity,
      depth: String(depth),
      limit: String(limit),
    });
    const data = await this.get<{ edges: Record<string, unknown>[] }>(
      `/api/v1/memories/graph?${p.toString()}`,
    );
    return data.edges ?? [];
  }

  /** All knowledge-graph edges (bulk view). */
  async graphEdges(limit = 10000): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ edges: Record<string, unknown>[] }>(
      `/api/v1/memories/edges?limit=${limit}`,
    );
    return data.edges ?? [];
  }

  /** Degree + PageRank per node, optionally as-of a time. */
  async graphCentrality(
    asOf?: string,
    limit?: number,
  ): Promise<Record<string, unknown>[]> {
    const p = new URLSearchParams();
    if (asOf) p.set("as_of", asOf);
    if (limit) p.set("limit", String(limit));
    const qs = p.toString();
    const data = await this.get<{ nodes: Record<string, unknown>[] }>(
      `/api/v1/memories/graph/centrality${qs ? `?${qs}` : ""}`,
    );
    return data.nodes ?? [];
  }

  /** Shortest directed path between two entities (null if unreachable). */
  async graphPath(src: string, dst: string, asOf?: string): Promise<string[] | null> {
    const p = new URLSearchParams({ src, dst });
    if (asOf) p.set("as_of", asOf);
    const data = await this.get<{ path: string[] | null }>(
      `/api/v1/memories/graph/path?${p.toString()}`,
    );
    return data.path ?? null;
  }

  /** Community detection (connected clusters), optionally as-of a time. */
  async graphCommunities(asOf?: string): Promise<string[][]> {
    const qs = asOf ? `?as_of=${encodeURIComponent(asOf)}` : "";
    const data = await this.get<{ communities: string[][] }>(
      `/api/v1/memories/graph/communities${qs}`,
    );
    return data.communities ?? [];
  }

  // ── Templates ────────────────────────────────────────────────────

  /** Built-in memory templates. */
  async memoryTemplates(): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ templates: Record<string, unknown>[] }>(
      "/api/v1/memory-templates",
    );
    return data.templates ?? [];
  }

  /** Create a memory from a template + field values. */
  async memoryFromTemplate(
    template: string,
    fields: Record<string, unknown>,
    userId?: string,
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { template, fields };
    if (userId) body.user_id = userId;
    return this.post("/api/v1/memories/from-template", body);
  }

  // ── Attachments (multimodal) ─────────────────────────────────────

  /** Upload a blob (image/PDF/audio). Optional caption stores a searchable memory. */
  async attachmentUpload(
    data: Uint8Array,
    opts: {
      contentType?: string;
      filename?: string;
      memoryId?: string;
      caption?: string;
    } = {},
  ): Promise<Record<string, unknown>> {
    const p = new URLSearchParams();
    if (opts.filename) p.set("filename", opts.filename);
    if (opts.memoryId) p.set("memory_id", opts.memoryId);
    if (opts.caption) p.set("caption", opts.caption);
    const qs = p.toString();
    const url = `${this.baseUrl}/api/v1/attachments${qs ? `?${qs}` : ""}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "POST",
        headers: {
          ...this.headers,
          "Content-Type": opts.contentType ?? "application/octet-stream",
        },
        // Uint8Array is a valid BodyInit at runtime; the DOM lib's type is narrower here.
        body: data as unknown as BodyInit,
        signal: controller.signal,
      });
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return (await resp.json()) as Record<string, unknown>;
    } finally {
      clearTimeout(timer);
    }
  }

  /** List attachments (optionally for one memory). */
  async attachmentList(memoryId?: string): Promise<Record<string, unknown>[]> {
    const qs = memoryId ? `?memory_id=${encodeURIComponent(memoryId)}` : "";
    const data = await this.get<{ attachments: Record<string, unknown>[] }>(
      `/api/v1/attachments${qs}`,
    );
    return data.attachments ?? [];
  }

  /** Delete an attachment. Returns false if it didn't exist. */
  async attachmentDelete(id: string): Promise<boolean> {
    const url = `${this.baseUrl}/api/v1/attachments/${encodeURIComponent(id)}`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);
    try {
      const resp = await fetch(url, {
        method: "DELETE",
        headers: this.headers,
        signal: controller.signal,
      });
      if (resp.status === 404) return false;
      if (!resp.ok) {
        throw new EcphoriaError(
          `HTTP ${resp.status}: ${resp.statusText}`,
          "HTTP_ERROR",
          resp.status,
        );
      }
      return true;
    } finally {
      clearTimeout(timer);
    }
  }

  // ── Admin / sessions extras ──────────────────────────────────────

  /** Re-embed active memories with the current provider (after a model change). */
  async memoryReembed(limit = 1000): Promise<Record<string, unknown>> {
    return this.post("/api/v1/admin/memory/reembed", { limit });
  }

  /** Consolidate a session's events into memory. */
  async sessionDistill(sessionId: string): Promise<Record<string, unknown>> {
    return this.post(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/distill`,
      {},
    );
  }

  // ── Sessions ─────────────────────────────────────────────────────

  /** Start a conversation session. */
  async sessionStart(
    sessionId: string,
    agentId: string,
    opts: { parentSessionId?: string; metadata?: Record<string, unknown> } = {},
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = {
      session_id: sessionId,
      agent_id: agentId,
    };
    if (opts.parentSessionId) body.parent_session_id = opts.parentSessionId;
    if (opts.metadata) body.metadata = opts.metadata;
    return this.post<Record<string, unknown>>("/api/v1/sessions", body);
  }

  /** End a session, optionally attaching a summary. */
  async sessionEnd(
    sessionId: string,
    summary?: string,
  ): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/end`,
      summary ? { summary } : {},
    );
  }

  /** Recall all events recorded in a session. */
  async sessionRecall(sessionId: string): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ events: Record<string, unknown>[] }>(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/recall`,
    );
    return data.events ?? [];
  }

  // ── Agentic platform (runs, agents, triggers, tools) ─────────────

  /** Create a durable agent/workflow run. Returns the run. */
  async runCreate(
    opts: { agentId?: string; input?: Record<string, unknown>; parentRunId?: string } = {},
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = {};
    if (opts.agentId !== undefined) body.agent_id = opts.agentId;
    if (opts.input !== undefined) body.input = opts.input;
    if (opts.parentRunId !== undefined) body.parent_run_id = opts.parentRunId;
    const data = await this.post<{ run: Record<string, unknown> }>("/api/v1/runs", body);
    return data.run ?? {};
  }

  /** Get a run by id. */
  async runGet(id: string): Promise<Record<string, unknown> | null> {
    const data = await this.get<{ run?: Record<string, unknown> }>(
      `/api/v1/runs/${encodeURIComponent(id)}`,
    );
    return data.run ?? null;
  }

  /** List runs (newest first), optionally filtered by status. */
  async runList(
    opts: { status?: string; limit?: number } = {},
  ): Promise<Record<string, unknown>[]> {
    const params = new URLSearchParams();
    params.set("limit", String(opts.limit ?? 50));
    if (opts.status) params.set("status", opts.status);
    const data = await this.get<{ runs: Record<string, unknown>[] }>(
      `/api/v1/runs?${params.toString()}`,
    );
    return data.runs ?? [];
  }

  /** Full step trace of a run (LLM/tool/HITL steps). */
  async runTrace(id: string): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ steps: Record<string, unknown>[] }>(
      `/api/v1/runs/${encodeURIComponent(id)}/trace`,
    );
    return data.steps ?? [];
  }

  /** Cancel a run. */
  async runCancel(id: string): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>(
      `/api/v1/runs/${encodeURIComponent(id)}/cancel`,
      {},
    );
  }

  /** Run an agent end-to-end (durable LLM↔tool loop). Returns the resulting run. */
  async runAgent(
    agentId: string,
    question: string,
    opts: { maxTurns?: number } = {},
  ): Promise<Record<string, unknown>> {
    const body: Record<string, unknown> = { agent_id: agentId, question };
    if (opts.maxTurns !== undefined) body.max_turns = opts.maxTurns;
    const data = await this.post<{ run: Record<string, unknown> }>("/api/v1/agents/run", body);
    return data.run ?? {};
  }

  /** Approve or reject a run awaiting approval (HITL). */
  async runApprove(id: string, approve = true): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>(
      `/api/v1/runs/${encodeURIComponent(id)}/approve`,
      { approve },
    );
  }

  /** Resume an approved run (durable resume after HITL). */
  async runResume(id: string): Promise<Record<string, unknown>> {
    const data = await this.post<{ run: Record<string, unknown> }>(
      `/api/v1/runs/${encodeURIComponent(id)}/resume`,
      {},
    );
    return data.run ?? {};
  }

  /** Register an event trigger: matching events start a run of `agentId`. */
  async triggerRegister(
    name: string,
    agentId: string,
    opts: { source?: string; eventType?: string } = {},
  ): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>("/api/v1/triggers", {
      name,
      agent_id: agentId,
      source: opts.source ?? "*",
      event_type: opts.eventType ?? "*",
    });
  }

  /** List registered event triggers. */
  async triggerList(): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ triggers: Record<string, unknown>[] }>("/api/v1/triggers");
    return data.triggers ?? [];
  }

  /** Register a downstream MCP tool server. */
  async toolRegister(name: string, url: string): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>("/api/v1/tools", { name, url });
  }

  /** List registered downstream MCP tool servers. */
  async toolList(): Promise<Record<string, unknown>[]> {
    const data = await this.get<{ servers: Record<string, unknown>[] }>("/api/v1/tools");
    return data.servers ?? [];
  }

  /** Invoke a tool on a registered downstream MCP server. */
  async toolCall(
    server: string,
    tool: string,
    args: Record<string, unknown> = {},
  ): Promise<unknown> {
    const data = await this.post<{ result: unknown }>(
      `/api/v1/tools/${encodeURIComponent(server)}/call`,
      { tool, arguments: args },
    );
    return data.result;
  }

  // ── Cluster ──────────────────────────────────────────────────────

  /** Get Raft cluster status. */
  async clusterStatus(): Promise<ClusterStatus> {
    return this.get<ClusterStatus>("/cluster/status");
  }
}
