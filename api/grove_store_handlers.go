package api

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

type groveHandler struct {
	store store_interface.GroveStore
}

// SetupGroveRouter registers all grove endpoints under the given prefix.
//
// Endpoints:
//
//	POST   {prefix}/trees/:treeId/nodes                          — create node
//	DELETE {prefix}/trees/:treeId/nodes/:nodeId                  — delete node (?soft=true for soft delete)
//	PATCH  {prefix}/trees/:treeId/nodes/:nodeId                  — move node
//	GET    {prefix}/trees/:treeId/nodes/:nodeId                  — get node info
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/exists           — check existence
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/children         — get children
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/ancestors        — get ancestors
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/descendants      — get descendants
//	POST   {prefix}/trees/:treeId/nodes/:nodeId/mutations        — apply aggregate mutation
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/aggregates       — subtree aggregates
//	GET    {prefix}/trees/:treeId/nodes/:nodeId/aggregates/local — local aggregates only
//	POST   {prefix}/trees/:treeId/bulk/ancestors                 — bulk ancestors
//	POST   {prefix}/trees/:treeId/bulk/aggregates                — bulk subtree aggregates
//	POST   {prefix}/trees/:treeId/bulk/aggregates/local          — bulk local aggregates
func SetupGroveRouter(store store_interface.GroveStore, prefix string, engine *gin.Engine) *gin.Engine {
	engine.UseRawPath = true
	engine.UnescapePathValues = true
	h := &groveHandler{store: store}
	g := engine.Group(prefix)
	g.POST("/trees/:treeId/nodes", h.createNode)
	g.DELETE("/trees/:treeId/nodes/:nodeId", h.deleteNode)
	g.PATCH("/trees/:treeId/nodes/:nodeId", h.moveNode)
	g.GET("/trees/:treeId/nodes/:nodeId", h.getNodeInfo)
	g.GET("/trees/:treeId/nodes/:nodeId/exists", h.exists)
	g.GET("/trees/:treeId/nodes/:nodeId/children", h.getChildren)
	g.GET("/trees/:treeId/nodes/:nodeId/ancestors", h.getAncestors)
	g.GET("/trees/:treeId/nodes/:nodeId/descendants", h.getDescendants)
	g.POST("/trees/:treeId/nodes/:nodeId/mutations", h.applyMutation)
	g.GET("/trees/:treeId/nodes/:nodeId/aggregates", h.getSubtreeAggregates)
	g.GET("/trees/:treeId/nodes/:nodeId/aggregates/local", h.getLocalAggregates)
	g.POST("/trees/:treeId/bulk/ancestors", h.getAncestorsBulk)
	g.POST("/trees/:treeId/bulk/aggregates", h.getSubtreeAggregatesBulk)
	g.POST("/trees/:treeId/bulk/aggregates/local", h.getLocalAggregatesBulk)
	return engine
}

func (h *groveHandler) createNode(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	var req protocol.GroveCreateNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	var parent *model.NodeID
	if req.ParentID != nil {
		n := model.NodeID(*req.ParentID)
		parent = &n
	}
	var position *model.ChildPosition
	if req.Position != nil {
		p := model.ChildPosition(*req.Position)
		position = &p
	}
	var metadata *model.NodeMetadata
	if req.Metadata != nil {
		m := model.NodeMetadata(req.Metadata)
		metadata = &m
	}
	if err := h.store.CreateNode(space, treeID, model.NodeID(req.NodeID), parent, position, metadata); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "written", 1)
	c.Status(http.StatusCreated)
}

func (h *groveHandler) deleteNode(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	soft := c.Query("soft") == "true"
	if err := h.store.DeleteNode(space, treeID, nodeID, soft); err != nil {
		respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *groveHandler) moveNode(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	var req protocol.GroveMoveNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	var newParent *model.NodeID
	if req.NewParentID != nil {
		n := model.NodeID(*req.NewParentID)
		newParent = &n
	}
	var newPosition *model.ChildPosition
	if req.NewPosition != nil {
		p := model.ChildPosition(*req.NewPosition)
		newPosition = &p
	}
	if err := h.store.MoveNode(space, treeID, nodeID, newParent, newPosition); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "written", 1)
	c.Status(http.StatusOK)
}

func (h *groveHandler) getNodeInfo(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	info, err := h.store.GetNodeInfo(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	resp := protocol.GroveNodeInfoResponse{
		ID:    string(info.ID),
		Depth: info.Depth,
	}
	if info.Parent != nil {
		s := string(*info.Parent)
		resp.ParentID = &s
	}
	if info.Position != nil {
		f := float64(*info.Position)
		resp.Position = &f
	}
	if info.Metadata != nil {
		resp.Metadata = map[string]interface{}(*info.Metadata)
	}
	c.JSON(http.StatusOK, resp)
}

func (h *groveHandler) exists(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	ok, err := h.store.Exists(space, treeID, nodeID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, protocol.GroveExistsResponse{Exists: ok})
}

func (h *groveHandler) getChildren(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	var pagination *protocol.PaginationRequest
	if raw, ok := c.GetQuery("pagination"); ok {
		if err := json.Unmarshal([]byte(raw), &pagination); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
	}
	children, page, err := h.store.GetChildren(space, treeID, nodeID, pagination.Model())
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(children))
	var strs []string
	if children != nil {
		strs = make([]string, len(children))
	}
	for i, ch := range children {
		strs[i] = string(ch)
	}
	c.JSON(http.StatusOK, protocol.GroveChildrenResponse{Children: strs, Pagination: protocol.PaginationResponseFromModel(page)})
}

func (h *groveHandler) getAncestors(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	var pagination *protocol.PaginationRequest
	if raw, ok := c.GetQuery("pagination"); ok {
		if err := json.Unmarshal([]byte(raw), &pagination); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
	}
	ancestors, page, err := h.store.GetAncestors(space, treeID, nodeID, pagination.Model())
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(ancestors))
	var strs []string
	if ancestors != nil {
		strs = make([]string, len(ancestors))
	}
	for i, a := range ancestors {
		strs[i] = string(a)
	}
	c.JSON(http.StatusOK, protocol.GroveAncestorsResponse{Ancestors: strs, Pagination: protocol.PaginationResponseFromModel(page)})
}

func (h *groveHandler) getDescendants(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	var options *protocol.DescendantOptionsRequest
	if raw, ok := c.GetQuery("options"); ok {
		if err := json.Unmarshal([]byte(raw), &options); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
	}
	descendants, page, err := h.store.GetDescendants(space, treeID, nodeID, options.Model())
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(descendants))
	var items []protocol.GroveNodeWithDepth
	if descendants != nil {
		items = make([]protocol.GroveNodeWithDepth, len(descendants))
	}
	for i, d := range descendants {
		items[i] = protocol.GroveNodeWithDepth{NodeID: string(d.NodeID), Depth: d.Depth}
	}
	c.JSON(http.StatusOK, protocol.GroveDescendantsResponse{Descendants: items, Pagination: protocol.PaginationResponseFromModel(page)})
}

func (h *groveHandler) applyMutation(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	var req protocol.GroveApplyMutationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	deltas := make(model.AggregateDeltas, len(req.Deltas))
	for k, v := range req.Deltas {
		deltas[model.AggregateKey(k)] = model.AggregateValue(v)
	}
	if err := h.store.ApplyAggregateMutation(space, treeID, model.MutationID(req.MutationID), nodeID, deltas); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "written", 1)
	c.Status(http.StatusOK)
}

func (h *groveHandler) getSubtreeAggregates(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	aggs, err := h.store.GetNodeWithDescendantsAggregates(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, protocol.GroveAggregatesResponse{Aggregates: aggregatesToMap(aggs)})
}

func (h *groveHandler) getLocalAggregates(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	nodeID := model.NodeID(c.Param("nodeId"))
	aggs, err := h.store.GetNodeLocalAggregates(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, protocol.GroveAggregatesResponse{Aggregates: aggregatesToMap(aggs)})
}

func (h *groveHandler) getAncestorsBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	var req protocol.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	ancestorsMap, missing, err := h.store.GetAncestorsBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "grove", "read", len(ancestorsMap))
	result := make(map[string][]string, len(ancestorsMap))
	for node, ancestors := range ancestorsMap {
		var strs []string
		if ancestors != nil {
			strs = make([]string, len(ancestors))
		}
		for i, a := range ancestors {
			strs[i] = string(a)
		}
		result[string(node)] = strs
	}
	c.JSON(http.StatusOK, protocol.GroveAncestorsBulkResponse{
		Ancestors: result,
		Missing:   nodeIDsToStrings(missing),
	})
}

func (h *groveHandler) getSubtreeAggregatesBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	var req protocol.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	aggsMap, missing, err := h.store.GetNodeWithDescendantsAggregatesBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "grove", "read", len(aggsMap))
	c.JSON(http.StatusOK, protocol.GroveAggregatesBulkResponse{
		Aggregates: aggregatesBulkToMap(aggsMap),
		Missing:    nodeIDsToStrings(missing),
	})
}

func (h *groveHandler) getLocalAggregatesBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	treeID := model.TreeID(c.Param("treeId"))
	var req protocol.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	aggsMap, missing, err := h.store.GetNodeLocalAggregatesBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "grove", "read", len(aggsMap))
	c.JSON(http.StatusOK, protocol.GroveAggregatesBulkResponse{
		Aggregates: aggregatesBulkToMap(aggsMap),
		Missing:    nodeIDsToStrings(missing),
	})
}

// helpers

func aggregatesToMap(aggs map[model.AggregateKey]model.AggregateValue) map[string]int64 {
	if aggs == nil {
		return nil
	}
	m := make(map[string]int64, len(aggs))
	for k, v := range aggs {
		m[string(k)] = int64(v)
	}
	return m
}

func aggregatesBulkToMap(bulk map[model.NodeID]map[model.AggregateKey]model.AggregateValue) map[string]map[string]int64 {
	if bulk == nil {
		return nil
	}
	result := make(map[string]map[string]int64, len(bulk))
	for node, aggs := range bulk {
		result[string(node)] = aggregatesToMap(aggs)
	}
	return result
}

func toNodeIDs(strs []string) []model.NodeID {
	ids := make([]model.NodeID, len(strs))
	for i, s := range strs {
		ids[i] = model.NodeID(s)
	}
	return ids
}

func nodeIDsToStrings(ids []model.NodeID) []string {
	if ids == nil {
		return nil
	}
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = string(id)
	}
	return strs
}
