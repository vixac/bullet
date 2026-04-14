package model

// ===== REQUESTS =====

type GroveCreateNodeRequest struct {
	NodeID   string                 `json:"node_id"`
	ParentID *string                `json:"parent_id,omitempty"`
	Position *float64               `json:"position,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type GroveMoveNodeRequest struct {
	NewParentID *string  `json:"new_parent_id,omitempty"`
	NewPosition *float64 `json:"new_position,omitempty"`
}

type GroveApplyMutationRequest struct {
	MutationID string           `json:"mutation_id"`
	Deltas     map[string]int64 `json:"deltas"`
}

type GroveBulkNodesRequest struct {
	NodeIDs []string `json:"node_ids"`
}

// ===== RESPONSES =====

type GroveExistsResponse struct {
	Exists bool `json:"exists"`
}

type GroveNodeInfoResponse struct {
	ID       string                 `json:"id"`
	ParentID *string                `json:"parent_id,omitempty"`
	Position *float64               `json:"position,omitempty"`
	Depth    int                    `json:"depth"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type GroveChildrenResponse struct {
	Children []string `json:"children"`
}

type GroveAncestorsResponse struct {
	Ancestors []string `json:"ancestors"`
}

type GroveAncestorsBulkResponse struct {
	Ancestors map[string][]string `json:"ancestors"`
	Missing   []string            `json:"missing"`
}

type GroveNodeWithDepth struct {
	NodeID string `json:"node_id"`
	Depth  int    `json:"depth"`
}

type GroveDescendantsResponse struct {
	Descendants []GroveNodeWithDepth `json:"descendants"`
}

type GroveAggregatesResponse struct {
	Aggregates map[string]int64 `json:"aggregates"`
}

type GroveAggregatesBulkResponse struct {
	Aggregates map[string]map[string]int64 `json:"aggregates"`
	Missing    []string                    `json:"missing"`
}
