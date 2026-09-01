package model

import "time"

type LedgerAppendRequest struct {
	AppendID string `json:"append_id"`
	Payload  string `json:"payload"`
}

type LedgerAppendManyRequest struct {
	Items []LedgerAppendRequest `json:"items"`
}

type LedgerSelectorRequest struct {
	All       bool     `json:"all"`
	LedgerIDs []string `json:"ledger_ids,omitempty"`
}

type LedgerReadBackwardRequest struct {
	LedgerSelectorRequest
	Cursor *string `json:"cursor,omitempty"`
	Limit  int     `json:"limit"`
}

type LedgerReadForwardRequest struct {
	LedgerSelectorRequest
	AfterPosition   string  `json:"after_position,omitempty"`
	ThroughPosition *string `json:"through_position,omitempty"`
	Limit           int     `json:"limit"`
}

type LedgerRecordResponse struct {
	LedgerID  string    `json:"ledger_id"`
	Position  string    `json:"position"`
	AppendID  string    `json:"append_id"`
	CreatedAt time.Time `json:"created_at"`
	Payload   string    `json:"payload"`
}

type LedgerAppendManyResponse struct {
	Records []LedgerRecordResponse `json:"records"`
}

type LedgerPageResponse struct {
	Records    []LedgerRecordResponse `json:"records"`
	NextCursor *string                `json:"next_cursor,omitempty"`
}

type LedgerReadForwardResponse struct {
	Records []LedgerRecordResponse `json:"records"`
}
