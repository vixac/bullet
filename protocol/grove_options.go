package protocol

import "github.com/vixac/bullet/model"

// Grove read options are JSON-encoded in the pagination or options query parameter.
type PaginationRequest struct {
	Limit  int     `json:"limit"`
	Cursor *string `json:"cursor,omitempty"`
}
type PaginationResponse struct {
	NextCursor *string `json:"next_cursor,omitempty"`
}
type DescendantOptionsRequest struct {
	MaxDepth     *int               `json:"max_depth,omitempty"`
	IncludeDepth bool               `json:"include_depth"`
	BreadthFirst bool               `json:"breadth_first"`
	Pagination   *PaginationRequest `json:"pagination,omitempty"`
}

func PaginationRequestFromModel(p *model.PaginationParams) *PaginationRequest {
	if p == nil {
		return nil
	}
	return &PaginationRequest{Limit: p.Limit, Cursor: p.Cursor}
}
func (p *PaginationRequest) Model() *model.PaginationParams {
	if p == nil {
		return nil
	}
	return &model.PaginationParams{Limit: p.Limit, Cursor: p.Cursor}
}
func PaginationResponseFromModel(p *model.PaginationResult) *PaginationResponse {
	if p == nil {
		return nil
	}
	return &PaginationResponse{NextCursor: p.NextCursor}
}
func (p *PaginationResponse) Model() *model.PaginationResult {
	if p == nil {
		return nil
	}
	return &model.PaginationResult{NextCursor: p.NextCursor}
}
func DescendantOptionsFromModel(p *model.DescendantOptions) *DescendantOptionsRequest {
	if p == nil {
		return nil
	}
	return &DescendantOptionsRequest{MaxDepth: p.MaxDepth, IncludeDepth: p.IncludeDepth, BreadthFirst: p.BreadthFirst, Pagination: PaginationRequestFromModel(p.Pagination)}
}
func (p *DescendantOptionsRequest) Model() *model.DescendantOptions {
	if p == nil {
		return nil
	}
	return &model.DescendantOptions{MaxDepth: p.MaxDepth, IncludeDepth: p.IncludeDepth, BreadthFirst: p.BreadthFirst, Pagination: p.Pagination.Model()}
}
