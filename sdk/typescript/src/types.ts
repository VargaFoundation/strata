/** Configuration options for the Strata client. */
export interface StrataClientOptions {
  /** Base URL of the Strata server (default: "http://localhost:8432"). */
  url?: string;
  /** API key for Bearer authentication. */
  apiKey?: string;
  /** Request timeout in milliseconds (default: 30000). */
  timeout?: number;
}

/** An event to ingest into episodic memory. */
export interface Event {
  event_type: string;
  [key: string]: unknown;
}

/** Body for ingest requests. */
export interface IngestRequest {
  source: string;
  events: Event[];
}

/** Response from ingest endpoint. */
export interface IngestResponse {
  ingested: number;
}

/** Body for query requests. */
export interface QueryRequest {
  sql: string;
}

/** Response from query endpoint. */
export interface QueryResponse {
  rows: Record<string, unknown>[];
  row_count: number;
}

/** Body for vector search requests. */
export interface SearchRequest {
  vector: number[];
  k?: number;
  filters?: SearchFilters;
}

/** Body for text-based embed-and-search requests. */
export interface FindRequest {
  text: string;
  k?: number;
  filters?: SearchFilters;
}

/** Optional search filters. */
export interface SearchFilters {
  source?: string;
  event_type?: string;
}

/** A single search result. */
export interface SearchResult {
  id: string;
  score: number;
  content?: string;
  metadata?: Record<string, unknown>;
  [key: string]: unknown;
}

/** Response from search endpoints. */
export interface SearchResponse {
  results: SearchResult[];
}

/** Agent state entry. */
export interface StateEntry {
  agent_id: string;
  key: string;
  value: unknown;
  version: number;
  updated_at?: string;
}

/** Response from state set. */
export interface StateSetResponse {
  version: number;
}

/** Health check response. */
export interface HealthResponse {
  status: string;
  version?: string;
  [key: string]: unknown;
}

/** Cluster status response. */
export interface ClusterStatus {
  node_id: number;
  state: string;
  leader?: number;
  term: number;
  [key: string]: unknown;
}

/** Error returned by the Strata API. */
export interface StrataApiError {
  code: string;
  message: string;
  request_id?: string;
}

/** Admin backup response. */
export interface BackupResponse {
  [key: string]: unknown;
}

/** Admin retention response. */
export interface RetentionResponse {
  [key: string]: unknown;
}

/** Scope tuple for the memory cognition layer (all fields optional; default tenant). */
export interface MemoryScope {
  tenant_id?: string;
  user_id?: string;
  agent_id?: string;
  session_id?: string;
}
