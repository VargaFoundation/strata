// Package ecphoria provides an HTTP client for the Ecphoria agentic memory platform API.
//
// Zero external dependencies — uses only the standard library (net/http, encoding/json).
//
//	client := ecphoria.NewClient("http://localhost:8432", nil)
//
//	// Ingest events
//	n, _ := client.Ingest(ctx, "my-app", []ecphoria.Event{
//	    {"event_type": "user.signup", "user_id": "u1"},
//	})
//
//	// Query with SQL
//	rows, _ := client.Query(ctx, "SELECT * FROM episodic LIMIT 10")
//
//	// Semantic search
//	results, _ := client.Find(ctx, "billing issue", 5, nil)
//
//	// Agent state
//	_ = client.StateSet(ctx, "bot-1", "mood", "happy")
//	entry, _ := client.StateGet(ctx, "bot-1", "mood")
package ecphoria

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ClientOptions configures the Ecphoria client.
type ClientOptions struct {
	// APIKey for Bearer authentication. Empty means no auth.
	APIKey string
	// Timeout for HTTP requests (default: 30s).
	Timeout time.Duration
	// HTTPClient overrides the default http.Client.
	HTTPClient *http.Client
}

// Client is an HTTP client for the Ecphoria agentic memory platform REST API.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// Error is returned when the Ecphoria API responds with an error.
type Error struct {
	Code      string
	Message   string
	RequestID string
	Status    int
}

func (e *Error) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("ecphoria: %s (code=%s, status=%d, request_id=%s)", e.Message, e.Code, e.Status, e.RequestID)
	}
	return fmt.Sprintf("ecphoria: %s (code=%s, status=%d)", e.Message, e.Code, e.Status)
}

// NewClient creates a new Ecphoria client. Pass nil for opts to use defaults.
func NewClient(baseURL string, opts *ClientOptions) *Client {
	baseURL = strings.TrimRight(baseURL, "/")
	c := &Client{baseURL: baseURL}

	if opts != nil {
		c.apiKey = opts.APIKey
		if opts.HTTPClient != nil {
			c.httpClient = opts.HTTPClient
		} else {
			timeout := opts.Timeout
			if timeout == 0 {
				timeout = 30 * time.Second
			}
			c.httpClient = &http.Client{Timeout: timeout}
		}
	} else {
		c.httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	return c
}

// ── Internal helpers ─────────────────────────────────────────────

func (c *Client) doRequest(ctx context.Context, method, path string, body any) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("ecphoria: marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("ecphoria: create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("ecphoria: do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("ecphoria: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var ae apiError
		if json.Unmarshal(respBody, &ae) == nil && ae.Message != "" {
			return nil, resp.StatusCode, &Error{
				Code:      ae.Code,
				Message:   ae.Message,
				RequestID: ae.RequestID,
				Status:    resp.StatusCode,
			}
		}
		return nil, resp.StatusCode, &Error{
			Code:    "HTTP_ERROR",
			Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
			Status:  resp.StatusCode,
		}
	}

	return respBody, resp.StatusCode, nil
}

// ── Health ───────────────────────────────────────────────────────

// Health checks server health.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, err
	}
	var r HealthResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode health: %w", err)
	}
	return &r, nil
}

// ── Query ────────────────────────────────────────────────────────

// Query executes a SQL query against the episodic store.
func (c *Client) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/query", QueryRequest{SQL: sql})
	if err != nil {
		return nil, err
	}
	var r QueryResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode query: %w", err)
	}
	return r.Rows, nil
}

// ── Ingest ───────────────────────────────────────────────────────

// Ingest ingests events into episodic memory. Returns the count of events ingested.
func (c *Client) Ingest(ctx context.Context, source string, events []Event) (int, error) {
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/ingest", IngestRequest{
		Source: source,
		Events: events,
	})
	if err != nil {
		return 0, err
	}
	var r IngestResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("ecphoria: decode ingest: %w", err)
	}
	return r.Ingested, nil
}

// ── Search ───────────────────────────────────────────────────────

// Search performs semantic search by pre-computed vector.
func (c *Client) Search(ctx context.Context, vector []float64, k int, filters *SearchFilters) ([]SearchResult, error) {
	req := SearchRequest{Vector: vector, K: k, Filters: filters}
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/search", req)
	if err != nil {
		return nil, err
	}
	var r SearchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode search: %w", err)
	}
	return r.Results, nil
}

// Find performs semantic search by text (embed + search in one call).
func (c *Client) Find(ctx context.Context, text string, k int, filters *SearchFilters) ([]SearchResult, error) {
	req := FindRequest{Text: text, K: k, Filters: filters}
	body, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/embed-and-search", req)
	if err != nil {
		return nil, err
	}
	var r SearchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode find: %w", err)
	}
	return r.Results, nil
}

// ── State ────────────────────────────────────────────────────────

// StateGet retrieves agent state. Returns nil, nil if not found.
func (c *Client) StateGet(ctx context.Context, agentID, key string) (*StateEntry, error) {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	body, status, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		if e, ok := err.(*Error); ok && e.Status == 404 {
			return nil, nil
		}
		return nil, err
	}
	if status == 404 {
		return nil, nil
	}
	var r StateEntry
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode state: %w", err)
	}
	return &r, nil
}

// StateSet sets agent state. Returns the new version number.
func (c *Client) StateSet(ctx context.Context, agentID, key string, value any) (int, error) {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	body, _, err := c.doRequest(ctx, http.MethodPut, path, value)
	if err != nil {
		return 0, err
	}
	var r StateSetResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return 0, fmt.Errorf("ecphoria: decode state set: %w", err)
	}
	return r.Version, nil
}

// StateDelete deletes agent state.
func (c *Client) StateDelete(ctx context.Context, agentID, key string) error {
	path := fmt.Sprintf("/api/v1/state/%s/%s", url.PathEscape(agentID), url.PathEscape(key))
	_, _, err := c.doRequest(ctx, http.MethodDelete, path, nil)
	return err
}

// ── Schema ───────────────────────────────────────────────────────

// Sources lists all event sources.
func (c *Client) Sources(ctx context.Context) ([]string, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/schema/sources", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Sources []string `json:"sources"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode sources: %w", err)
	}
	return r.Sources, nil
}

// Agents lists all agent IDs.
func (c *Client) Agents(ctx context.Context) ([]string, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/schema/agents", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Agents []string `json:"agents"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode agents: %w", err)
	}
	return r.Agents, nil
}

// ── Admin ────────────────────────────────────────────────────────

// Backup triggers a backup of all stores.
func (c *Client) Backup(ctx context.Context) error {
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/admin/backup", struct{}{})
	return err
}

// EnforceRetention enforces the data retention policy.
func (c *Client) EnforceRetention(ctx context.Context) error {
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/admin/retention", struct{}{})
	return err
}

// ── Cluster ──────────────────────────────────────────────────────

// ClusterStatus returns the Raft cluster status.
func (c *Client) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	body, _, err := c.doRequest(ctx, http.MethodGet, "/cluster/status", nil)
	if err != nil {
		return nil, err
	}
	var r ClusterStatus
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode cluster status: %w", err)
	}
	return &r, nil
}

// ── Memory cognition layer ──────────────────────────────────────────

// MemoryScope scopes a memory operation (all fields optional; default tenant).
type MemoryScope struct {
	TenantID  string
	UserID    string
	AgentID   string
	SessionID string
}

func (s MemoryScope) apply(m map[string]any) {
	if s.TenantID != "" {
		m["tenant_id"] = s.TenantID
	}
	if s.UserID != "" {
		m["user_id"] = s.UserID
	}
	if s.AgentID != "" {
		m["agent_id"] = s.AgentID
	}
	if s.SessionID != "" {
		m["session_id"] = s.SessionID
	}
}

func (s MemoryScope) query() url.Values {
	q := url.Values{}
	if s.TenantID != "" {
		q.Set("tenant_id", s.TenantID)
	}
	if s.UserID != "" {
		q.Set("user_id", s.UserID)
	}
	if s.AgentID != "" {
		q.Set("agent_id", s.AgentID)
	}
	if s.SessionID != "" {
		q.Set("session_id", s.SessionID)
	}
	return q
}

// MemoryAdd adds a memory through the cognition pipeline (dedup / contradiction / importance).
func (c *Client) MemoryAdd(ctx context.Context, content string, scope MemoryScope, subject string, importance *float64) (map[string]any, error) {
	body := map[string]any{"content": content}
	scope.apply(body)
	if subject != "" {
		body["subject"] = subject
	}
	if importance != nil {
		body["importance"] = *importance
	}
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/memories", body)
	if err != nil {
		return nil, err
	}
	var r map[string]any
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_add: %w", err)
	}
	return r, nil
}

// MemorySearch does a hybrid (BM25 + vector) search over a scope's memories.
func (c *Client) MemorySearch(ctx context.Context, query string, k int, scope MemoryScope) ([]map[string]any, error) {
	body := map[string]any{"query": query, "k": k}
	scope.apply(body)
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/memories/search", body)
	if err != nil {
		return nil, err
	}
	var r struct {
		Results []map[string]any `json:"results"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_search: %w", err)
	}
	return r.Results, nil
}

// MemoryList lists active memories in a scope.
// MemoryFilter narrows a memory listing (all conjunctive; the zero value adds no constraint).
type MemoryFilter struct {
	MemType       string   // exact mem_type (semantic|episodic|procedural)
	MinImportance *float64 // pointer so a 0.0 threshold is expressible
	UpdatedAfter  string   // RFC3339 — keep memories updated at/after
	UpdatedBefore string   // RFC3339 — keep memories updated strictly before
	MetadataKey   string   // exact metadata match (pair with MetadataValue)
	MetadataValue string
}

// MemoryListOptions parameterizes MemoryListWith (filters + offset pagination).
type MemoryListOptions struct {
	Limit  int
	Offset int
	Scope  MemoryScope
	Filter MemoryFilter
}

// MemoryListWith lists active memories in a scope with optional filters + offset pagination.
func (c *Client) MemoryListWith(ctx context.Context, opts MemoryListOptions) ([]map[string]any, error) {
	q := opts.Scope.query()
	limit := opts.Limit
	if limit <= 0 {
		limit = 50
	}
	q.Set("limit", fmt.Sprintf("%d", limit))
	q.Set("offset", fmt.Sprintf("%d", opts.Offset))
	if opts.Filter.MemType != "" {
		q.Set("mem_type", opts.Filter.MemType)
	}
	if opts.Filter.MinImportance != nil {
		q.Set("min_importance", strconv.FormatFloat(*opts.Filter.MinImportance, 'g', -1, 64))
	}
	if opts.Filter.UpdatedAfter != "" {
		q.Set("updated_after", opts.Filter.UpdatedAfter)
	}
	if opts.Filter.UpdatedBefore != "" {
		q.Set("updated_before", opts.Filter.UpdatedBefore)
	}
	if opts.Filter.MetadataKey != "" && opts.Filter.MetadataValue != "" {
		q.Set("metadata_key", opts.Filter.MetadataKey)
		q.Set("metadata_value", opts.Filter.MetadataValue)
	}
	data, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/memories?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Memories []map[string]any `json:"memories"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_list: %w", err)
	}
	return r.Memories, nil
}

// MemoryList lists active memories in a scope (unfiltered; see MemoryListWith for filters/paging).
func (c *Client) MemoryList(ctx context.Context, limit int, scope MemoryScope) ([]map[string]any, error) {
	return c.MemoryListWith(ctx, MemoryListOptions{Limit: limit, Scope: scope})
}

// MemoryGet returns a memory by id, or nil if not found (or not in your tenant).
func (c *Client) MemoryGet(ctx context.Context, id string) (map[string]any, error) {
	data, status, err := c.doRequest(ctx, http.MethodGet, "/api/v1/memories/"+url.PathEscape(id), nil)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r map[string]any
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_get: %w", err)
	}
	return r, nil
}

// MemoryPatch is a partial correction; only the set (non-nil) fields change. Content is re-embedded.
// The subject is not editable — add a new memory to change what a memory is about.
type MemoryPatch struct {
	Content    *string        `json:"content,omitempty"`
	Importance *float64       `json:"importance,omitempty"`
	MemType    *string        `json:"mem_type,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// MemoryUpdate partially corrects a memory by id. Returns nil if it doesn't exist (or isn't in your
// tenant).
func (c *Client) MemoryUpdate(ctx context.Context, id string, patch MemoryPatch) (map[string]any, error) {
	data, status, err := c.doRequest(ctx, http.MethodPatch, "/api/v1/memories/"+url.PathEscape(id), patch)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var r map[string]any
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_update: %w", err)
	}
	return r, nil
}

// MemoryHistory returns the bi-temporal history for a memory's subject (oldest first).
func (c *Client) MemoryHistory(ctx context.Context, id string) ([]map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/memories/"+url.PathEscape(id)+"/history", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		History []map[string]any `json:"history"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode memory_history: %w", err)
	}
	return r.History, nil
}

// MemoryDelete deletes a memory by id; returns false if it didn't exist (or not in your tenant).
func (c *Client) MemoryDelete(ctx context.Context, id string) (bool, error) {
	_, status, err := c.doRequest(ctx, http.MethodDelete, "/api/v1/memories/"+url.PathEscape(id), nil)
	if status == http.StatusNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ── Cognition (provenance / feedback / contradictions) ──────────────

// MemoryProvenance returns a memory's source events + supersession chain.
func (c *Client) MemoryProvenance(ctx context.Context, id string) (map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/memories/"+url.PathEscape(id)+"/provenance", nil)
	if err != nil {
		return nil, err
	}
	var r map[string]any
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode provenance: %w", err)
	}
	return r, nil
}

// MemoryFeedback gives feedback so ranking learns: "helpful" reinforces, "wrong"/"obsolete" retires.
func (c *Client) MemoryFeedback(ctx context.Context, id, verdict string) (map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/memories/"+url.PathEscape(id)+"/feedback", map[string]any{"verdict": verdict})
	if err != nil {
		return nil, err
	}
	return decodeMap(data, "feedback")
}

// MemoryContradictions lists subjects with more than one active memory (the review queue).
func (c *Client) MemoryContradictions(ctx context.Context, userID string) ([]map[string]any, error) {
	path := "/api/v1/memories/contradictions"
	if userID != "" {
		path += "?user_id=" + url.QueryEscape(userID)
	}
	return c.getList(ctx, path, "contradictions")
}

// ── Knowledge graph ─────────────────────────────────────────────────

// MemoryLink adds a graph edge (src -[relation]-> dst). supersede closes the prior (src, relation).
func (c *Client) MemoryLink(ctx context.Context, src, relation, dst string, supersede bool) (map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/memories/link", map[string]any{
		"src": src, "relation": relation, "dst": dst, "supersede": supersede,
	})
	if err != nil {
		return nil, err
	}
	return decodeMap(data, "link")
}

// GraphNeighbors returns the edges around an entity (depth>1 expands the subgraph).
func (c *Client) GraphNeighbors(ctx context.Context, entity string, depth, limit int) ([]map[string]any, error) {
	q := url.Values{"entity": {entity}, "depth": {strconv.Itoa(depth)}, "limit": {strconv.Itoa(limit)}}
	return c.getList(ctx, "/api/v1/memories/graph?"+q.Encode(), "edges")
}

// GraphEdges returns all knowledge-graph edges (bulk view).
func (c *Client) GraphEdges(ctx context.Context, limit int) ([]map[string]any, error) {
	return c.getList(ctx, "/api/v1/memories/edges?limit="+strconv.Itoa(limit), "edges")
}

// GraphCentrality ranks nodes by degree + PageRank, optionally as-of a time (RFC3339, empty for now).
func (c *Client) GraphCentrality(ctx context.Context, asOf string, limit int) ([]map[string]any, error) {
	q := url.Values{}
	if asOf != "" {
		q.Set("as_of", asOf)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	path := "/api/v1/memories/graph/centrality"
	if e := q.Encode(); e != "" {
		path += "?" + e
	}
	return c.getList(ctx, path, "nodes")
}

// GraphPath returns the shortest directed path between two entities (nil if unreachable).
func (c *Client) GraphPath(ctx context.Context, src, dst, asOf string) ([]string, error) {
	q := url.Values{"src": {src}, "dst": {dst}}
	if asOf != "" {
		q.Set("as_of", asOf)
	}
	data, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/memories/graph/path?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Path []string `json:"path"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode graph_path: %w", err)
	}
	return r.Path, nil
}

// GraphCommunities detects communities (connected clusters), optionally as-of a time.
func (c *Client) GraphCommunities(ctx context.Context, asOf string) ([][]string, error) {
	path := "/api/v1/memories/graph/communities"
	if asOf != "" {
		path += "?as_of=" + url.QueryEscape(asOf)
	}
	data, _, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Communities [][]string `json:"communities"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode graph_communities: %w", err)
	}
	return r.Communities, nil
}

// ── Templates ───────────────────────────────────────────────────────

// MemoryTemplates lists the built-in memory templates.
func (c *Client) MemoryTemplates(ctx context.Context) ([]map[string]any, error) {
	return c.getList(ctx, "/api/v1/memory-templates", "templates")
}

// MemoryFromTemplate creates a memory from a template + field values.
func (c *Client) MemoryFromTemplate(ctx context.Context, template string, fields map[string]any, userID string) (map[string]any, error) {
	body := map[string]any{"template": template, "fields": fields}
	if userID != "" {
		body["user_id"] = userID
	}
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/memories/from-template", body)
	if err != nil {
		return nil, err
	}
	return decodeMap(data, "from_template")
}

// ── Attachments (multimodal) ────────────────────────────────────────

// AttachmentUpload uploads a blob (image/PDF/audio). caption (optional) stores a searchable memory.
func (c *Client) AttachmentUpload(ctx context.Context, data []byte, contentType, filename, memoryID, caption string) (map[string]any, error) {
	q := url.Values{}
	for k, v := range map[string]string{"filename": filename, "memory_id": memoryID, "caption": caption} {
		if v != "" {
			q.Set(k, v)
		}
	}
	path := "/api/v1/attachments"
	if e := q.Encode(); e != "" {
		path += "?" + e
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("ecphoria: create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ecphoria: do request: %w", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("ecphoria: attachment upload: HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return decodeMap(respBody, "attachment")
}

// AttachmentList lists attachments (optionally for one memory).
func (c *Client) AttachmentList(ctx context.Context, memoryID string) ([]map[string]any, error) {
	path := "/api/v1/attachments"
	if memoryID != "" {
		path += "?memory_id=" + url.QueryEscape(memoryID)
	}
	return c.getList(ctx, path, "attachments")
}

// AttachmentDelete deletes an attachment; returns false if it didn't exist.
func (c *Client) AttachmentDelete(ctx context.Context, id string) (bool, error) {
	_, status, err := c.doRequest(ctx, http.MethodDelete, "/api/v1/attachments/"+url.PathEscape(id), nil)
	if status == http.StatusNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// MemoryReembed re-embeds active memories with the current provider (after a model change).
func (c *Client) MemoryReembed(ctx context.Context, limit int) (map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/admin/memory/reembed", map[string]any{"limit": limit})
	if err != nil {
		return nil, err
	}
	return decodeMap(data, "reembed")
}

// getList GETs `path` and returns the array under `key`.
func (c *Client) getList(ctx context.Context, path, key string) ([]map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var r map[string]json.RawMessage
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode %s: %w", key, err)
	}
	var out []map[string]any
	if raw, ok := r[key]; ok {
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("ecphoria: decode %s list: %w", key, err)
		}
	}
	return out, nil
}

func decodeMap(data []byte, what string) (map[string]any, error) {
	var r map[string]any
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode %s: %w", what, err)
	}
	return r, nil
}

// ── Sessions ────────────────────────────────────────────────────────

// SessionStart starts a conversation session.
func (c *Client) SessionStart(ctx context.Context, sessionID, agentID string) error {
	body := map[string]any{"session_id": sessionID, "agent_id": agentID}
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/sessions", body)
	return err
}

// SessionEnd ends a session, optionally attaching a summary.
func (c *Client) SessionEnd(ctx context.Context, sessionID, summary string) error {
	body := map[string]any{}
	if summary != "" {
		body["summary"] = summary
	}
	_, _, err := c.doRequest(ctx, http.MethodPost, "/api/v1/sessions/"+url.PathEscape(sessionID)+"/end", body)
	return err
}

// SessionRecall recalls all events recorded in a session.
func (c *Client) SessionRecall(ctx context.Context, sessionID string) ([]map[string]any, error) {
	data, _, err := c.doRequest(ctx, http.MethodGet, "/api/v1/sessions/"+url.PathEscape(sessionID)+"/recall", nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("ecphoria: decode session_recall: %w", err)
	}
	return r.Events, nil
}
