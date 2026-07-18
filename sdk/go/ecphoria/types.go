package ecphoria

import "encoding/json"

// Event is an event to ingest into episodic memory.
type Event map[string]any

// IngestRequest is the body for ingest requests.
type IngestRequest struct {
	Source string  `json:"source"`
	Events []Event `json:"events"`
}

// IngestResponse is returned from the ingest endpoint.
type IngestResponse struct {
	Ingested int `json:"ingested"`
}

// QueryRequest is the body for query requests.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse is returned from the query endpoint.
type QueryResponse struct {
	Rows     []map[string]any `json:"rows"`
	RowCount int              `json:"row_count"`
}

// SearchFilters are optional filters for search requests.
type SearchFilters struct {
	Source    string `json:"source,omitempty"`
	EventType string `json:"event_type,omitempty"`
}

// SearchRequest is the body for vector search.
type SearchRequest struct {
	Vector  []float64      `json:"vector"`
	K       int            `json:"k,omitempty"`
	Filters *SearchFilters `json:"filters,omitempty"`
}

// FindRequest is the body for text-based embed-and-search.
type FindRequest struct {
	Text    string         `json:"text"`
	K       int            `json:"k,omitempty"`
	Filters *SearchFilters `json:"filters,omitempty"`
}

// SearchResult is a single search result.
type SearchResult struct {
	ID       string         `json:"id"`
	Score    float64        `json:"score"`
	Content  string         `json:"content,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// SearchResponse is returned from search endpoints.
type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

// StateEntry is an agent state entry.
type StateEntry struct {
	AgentID   string          `json:"agent_id"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int             `json:"version"`
	UpdatedAt string          `json:"updated_at,omitempty"`
}

// StateSetResponse is returned from state set.
type StateSetResponse struct {
	Version int `json:"version"`
}

// HealthResponse is returned from the health endpoint.
type HealthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version,omitempty"`
}

// ClusterStatus is returned from the cluster status endpoint.
type ClusterStatus struct {
	NodeID int    `json:"node_id"`
	State  string `json:"state"`
	Leader *int   `json:"leader,omitempty"`
	Term   int    `json:"term"`
}

// apiError is the error structure returned by the API.
type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}
