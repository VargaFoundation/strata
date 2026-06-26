import { StrataError } from "./errors.js";
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
  StrataApiError,
  StrataClientOptions,
} from "./types.js";

/**
 * Strata client — fetch-based HTTP client for the Strata context lake API.
 *
 * Zero runtime dependencies. Uses the global `fetch` API (Node 18+, Deno, Bun, browsers).
 *
 * @example
 * ```ts
 * const client = new StrataClient({ url: "http://localhost:8432" });
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
export class StrataClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeout: number;

  constructor(options: StrataClientOptions = {}) {
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
        let apiErr: StrataApiError | undefined;
        try {
          const json = await resp.json();
          if (json && typeof json === "object" && "message" in json) {
            apiErr = json as StrataApiError;
          }
        } catch {
          // not JSON
        }
        if (apiErr) {
          throw StrataError.fromApiError(apiErr, resp.status);
        }
        throw new StrataError(
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
        throw new StrataError(
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
        throw new StrataError(
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
    opts: MemoryScope & { limit?: number } = {},
  ): Promise<Record<string, unknown>[]> {
    const params = new URLSearchParams();
    params.set("limit", String(opts.limit ?? 50));
    for (const key of ["tenant_id", "user_id", "agent_id", "session_id"] as const) {
      const v = opts[key];
      if (v !== undefined) params.set(key, v);
    }
    const data = await this.get<{ memories: Record<string, unknown>[] }>(
      `/api/v1/memories?${params.toString()}`,
    );
    return data.memories ?? [];
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
        throw new StrataError(
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
        throw new StrataError(
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

  // ── Cluster ──────────────────────────────────────────────────────

  /** Get Raft cluster status. */
  async clusterStatus(): Promise<ClusterStatus> {
    return this.get<ClusterStatus>("/cluster/status");
  }
}
