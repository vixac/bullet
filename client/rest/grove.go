package rest

import (
	"encoding/json"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	"net/http"
	"net/url"
	"strings"
)

func escapePathSegment(value string) string {
	return strings.ReplaceAll(url.PathEscape(value), "+", "%2B")
}

func treePath(id model.TreeID) string { return "/grove/trees/" + escapePathSegment(string(id)) }
func nodePath(tree model.TreeID, id model.NodeID) string {
	return treePath(tree) + "/nodes/" + escapePathSegment(string(id))
}
func nodeStrings(ids []model.NodeID) []string {
	if ids == nil {
		return nil
	}
	r := make([]string, len(ids))
	for i, id := range ids {
		r[i] = string(id)
	}
	return r
}
func nodeIDs(ids []string) []model.NodeID {
	if ids == nil {
		return nil
	}
	r := make([]model.NodeID, len(ids))
	for i, id := range ids {
		r[i] = model.NodeID(id)
	}
	return r
}
func nodePtr(id *model.NodeID) *string {
	if id == nil {
		return nil
	}
	s := string(*id)
	return &s
}
func positionPtr(p *model.ChildPosition) *float64 {
	if p == nil {
		return nil
	}
	v := float64(*p)
	return &v
}
func (c *Client) CreateNode(tree model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition, metadata *model.NodeMetadata) error {
	req := protocol.GroveCreateNodeRequest{NodeID: string(node), ParentID: nodePtr(parent), Position: positionPtr(position)}
	if metadata != nil {
		req.Metadata = map[string]interface{}(*metadata)
	}
	return c.call(http.MethodPost, treePath(tree)+"/nodes", req, nil, http.StatusCreated)
}
func (c *Client) DeleteNode(tree model.TreeID, node model.NodeID, soft bool) error {
	path := nodePath(tree, node)
	if soft {
		path += "?soft=true"
	}
	return c.call(http.MethodDelete, path, nil, nil, http.StatusNoContent)
}
func (c *Client) MoveNode(tree model.TreeID, node model.NodeID, parent *model.NodeID, position *model.ChildPosition) error {
	return c.call(http.MethodPatch, nodePath(tree, node), protocol.GroveMoveNodeRequest{NewParentID: nodePtr(parent), NewPosition: positionPtr(position)}, nil, http.StatusOK)
}
func (c *Client) Exists(tree model.TreeID, node model.NodeID) (bool, error) {
	var r protocol.GroveExistsResponse
	if err := c.call(http.MethodGet, nodePath(tree, node)+"/exists", nil, &r, http.StatusOK); err != nil {
		return false, err
	}
	return r.Exists, nil
}
func (c *Client) GetNodeInfo(tree model.TreeID, node model.NodeID) (*model.NodeInfo, error) {
	var r protocol.GroveNodeInfoResponse
	if err := c.call(http.MethodGet, nodePath(tree, node), nil, &r, http.StatusOK); err != nil {
		return nil, err
	}
	info := &model.NodeInfo{ID: model.NodeID(r.ID), Depth: r.Depth}
	if r.ParentID != nil {
		v := model.NodeID(*r.ParentID)
		info.Parent = &v
	}
	if r.Position != nil {
		v := model.ChildPosition(*r.Position)
		info.Position = &v
	}
	if r.Metadata != nil {
		v := model.NodeMetadata(r.Metadata)
		info.Metadata = &v
	}
	return info, nil
}
func optionsPath(path, key string, options any) (string, error) {
	b, err := json.Marshal(options)
	if err != nil {
		return "", err
	}
	if string(b) == "null" {
		return path, nil
	}
	return path + "?" + url.Values{key: []string{string(b)}}.Encode(), nil
}
func (c *Client) GetChildren(tree model.TreeID, node model.NodeID, p *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	path, err := optionsPath(nodePath(tree, node)+"/children", "pagination", protocol.PaginationRequestFromModel(p))
	if err != nil {
		return nil, nil, err
	}
	var r protocol.GroveChildrenResponse
	if err := c.call(http.MethodGet, path, nil, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	return nodeIDs(r.Children), r.Pagination.Model(), nil
}
func (c *Client) GetAncestors(tree model.TreeID, node model.NodeID, p *model.PaginationParams) ([]model.NodeID, *model.PaginationResult, error) {
	path, err := optionsPath(nodePath(tree, node)+"/ancestors", "pagination", protocol.PaginationRequestFromModel(p))
	if err != nil {
		return nil, nil, err
	}
	var r protocol.GroveAncestorsResponse
	if err := c.call(http.MethodGet, path, nil, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	return nodeIDs(r.Ancestors), r.Pagination.Model(), nil
}
func (c *Client) GetDescendants(tree model.TreeID, node model.NodeID, opts *model.DescendantOptions) ([]model.NodeWithDepth, *model.PaginationResult, error) {
	path, err := optionsPath(nodePath(tree, node)+"/descendants", "options", protocol.DescendantOptionsFromModel(opts))
	if err != nil {
		return nil, nil, err
	}
	var r protocol.GroveDescendantsResponse
	if err := c.call(http.MethodGet, path, nil, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	var ds []model.NodeWithDepth
	if r.Descendants != nil {
		ds = make([]model.NodeWithDepth, len(r.Descendants))
		for i, v := range r.Descendants {
			ds[i] = model.NodeWithDepth{NodeID: model.NodeID(v.NodeID), Depth: v.Depth}
		}
	}
	return ds, r.Pagination.Model(), nil
}
func (c *Client) ApplyAggregateMutation(tree model.TreeID, mutation model.MutationID, node model.NodeID, deltas model.AggregateDeltas) error {
	ds := make(map[string]int64, len(deltas))
	for k, v := range deltas {
		ds[string(k)] = int64(v)
	}
	return c.call(http.MethodPost, nodePath(tree, node)+"/mutations", protocol.GroveApplyMutationRequest{MutationID: string(mutation), Deltas: ds}, nil, http.StatusOK)
}
func aggregates(values map[string]int64) map[model.AggregateKey]model.AggregateValue {
	if values == nil {
		return nil
	}
	r := make(map[model.AggregateKey]model.AggregateValue, len(values))
	for k, v := range values {
		r[model.AggregateKey(k)] = model.AggregateValue(v)
	}
	return r
}
func (c *Client) getAggregates(tree model.TreeID, node model.NodeID, suffix string) (map[model.AggregateKey]model.AggregateValue, error) {
	var r protocol.GroveAggregatesResponse
	if err := c.call(http.MethodGet, nodePath(tree, node)+suffix, nil, &r, http.StatusOK); err != nil {
		return nil, err
	}
	return aggregates(r.Aggregates), nil
}
func (c *Client) GetNodeLocalAggregates(tree model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return c.getAggregates(tree, node, "/aggregates/local")
}
func (c *Client) GetNodeWithDescendantsAggregates(tree model.TreeID, node model.NodeID) (map[model.AggregateKey]model.AggregateValue, error) {
	return c.getAggregates(tree, node, "/aggregates")
}
func (c *Client) GetAncestorsBulk(tree model.TreeID, nodes []model.NodeID) (map[model.NodeID][]model.NodeID, []model.NodeID, error) {
	var r protocol.GroveAncestorsBulkResponse
	if err := c.call(http.MethodPost, treePath(tree)+"/bulk/ancestors", protocol.GroveBulkNodesRequest{NodeIDs: nodeStrings(nodes)}, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	var values map[model.NodeID][]model.NodeID
	if r.Ancestors != nil {
		values = make(map[model.NodeID][]model.NodeID, len(r.Ancestors))
		for k, v := range r.Ancestors {
			values[model.NodeID(k)] = nodeIDs(v)
		}
	}
	return values, nodeIDs(r.Missing), nil
}
func (c *Client) getAggregatesBulk(tree model.TreeID, nodes []model.NodeID, suffix string) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	var r protocol.GroveAggregatesBulkResponse
	if err := c.call(http.MethodPost, treePath(tree)+suffix, protocol.GroveBulkNodesRequest{NodeIDs: nodeStrings(nodes)}, &r, http.StatusOK); err != nil {
		return nil, nil, err
	}
	var values map[model.NodeID]map[model.AggregateKey]model.AggregateValue
	if r.Aggregates != nil {
		values = make(map[model.NodeID]map[model.AggregateKey]model.AggregateValue, len(r.Aggregates))
		for k, v := range r.Aggregates {
			values[model.NodeID(k)] = aggregates(v)
		}
	}
	return values, nodeIDs(r.Missing), nil
}
func (c *Client) GetNodeLocalAggregatesBulk(tree model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return c.getAggregatesBulk(tree, nodes, "/bulk/aggregates/local")
}
func (c *Client) GetNodeWithDescendantsAggregatesBulk(tree model.TreeID, nodes []model.NodeID) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	return c.getAggregatesBulk(tree, nodes, "/bulk/aggregates")
}
