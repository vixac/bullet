package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	var req model.GroveCreateNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var parent *store_interface.NodeID
	if req.ParentID != nil {
		n := store_interface.NodeID(*req.ParentID)
		parent = &n
	}
	var position *store_interface.ChildPosition
	if req.Position != nil {
		p := store_interface.ChildPosition(*req.Position)
		position = &p
	}
	var metadata *store_interface.NodeMetadata
	if req.Metadata != nil {
		m := store_interface.NodeMetadata(req.Metadata)
		metadata = &m
	}
	if err := h.store.CreateNode(space, treeID, store_interface.NodeID(req.NodeID), parent, position, metadata); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "written", 1)
	c.Status(http.StatusCreated)
}

func (h *groveHandler) deleteNode(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	var req model.GroveMoveNodeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var newParent *store_interface.NodeID
	if req.NewParentID != nil {
		n := store_interface.NodeID(*req.NewParentID)
		newParent = &n
	}
	var newPosition *store_interface.ChildPosition
	if req.NewPosition != nil {
		p := store_interface.ChildPosition(*req.NewPosition)
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	info, err := h.store.GetNodeInfo(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	resp := model.GroveNodeInfoResponse{
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	ok, err := h.store.Exists(space, treeID, nodeID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, model.GroveExistsResponse{Exists: ok})
}

func (h *groveHandler) getChildren(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	children, _, err := h.store.GetChildren(space, treeID, nodeID, nil)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(children))
	strs := make([]string, len(children))
	for i, ch := range children {
		strs[i] = string(ch)
	}
	c.JSON(http.StatusOK, model.GroveChildrenResponse{Children: strs})
}

func (h *groveHandler) getAncestors(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	ancestors, _, err := h.store.GetAncestors(space, treeID, nodeID, nil)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(ancestors))
	strs := make([]string, len(ancestors))
	for i, a := range ancestors {
		strs[i] = string(a)
	}
	c.JSON(http.StatusOK, model.GroveAncestorsResponse{Ancestors: strs})
}

func (h *groveHandler) getDescendants(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	descendants, _, err := h.store.GetDescendants(space, treeID, nodeID, nil)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", len(descendants))
	items := make([]model.GroveNodeWithDepth, len(descendants))
	for i, d := range descendants {
		items[i] = model.GroveNodeWithDepth{NodeID: string(d.NodeID), Depth: d.Depth}
	}
	c.JSON(http.StatusOK, model.GroveDescendantsResponse{Descendants: items})
}

func (h *groveHandler) applyMutation(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	var req model.GroveApplyMutationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	deltas := make(store_interface.AggregateDeltas, len(req.Deltas))
	for k, v := range req.Deltas {
		deltas[store_interface.AggregateKey(k)] = store_interface.AggregateValue(v)
	}
	if err := h.store.ApplyAggregateMutation(space, treeID, store_interface.MutationID(req.MutationID), nodeID, deltas); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "written", 1)
	c.Status(http.StatusOK)
}

func (h *groveHandler) getSubtreeAggregates(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	aggs, err := h.store.GetNodeWithDescendantsAggregates(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, model.GroveAggregatesResponse{Aggregates: aggregatesToMap(aggs)})
}

func (h *groveHandler) getLocalAggregates(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	nodeID := store_interface.NodeID(c.Param("nodeId"))
	aggs, err := h.store.GetNodeLocalAggregates(space, treeID, nodeID)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "grove", "read", 1)
	c.JSON(http.StatusOK, model.GroveAggregatesResponse{Aggregates: aggregatesToMap(aggs)})
}

func (h *groveHandler) getAncestorsBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	var req model.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	ancestorsMap, missing, err := h.store.GetAncestorsBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "grove", "read", len(ancestorsMap))
	result := make(map[string][]string, len(ancestorsMap))
	for node, ancestors := range ancestorsMap {
		strs := make([]string, len(ancestors))
		for i, a := range ancestors {
			strs[i] = string(a)
		}
		result[string(node)] = strs
	}
	c.JSON(http.StatusOK, model.GroveAncestorsBulkResponse{
		Ancestors: result,
		Missing:   nodeIDsToStrings(missing),
	})
}

func (h *groveHandler) getSubtreeAggregatesBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	var req model.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	aggsMap, missing, err := h.store.GetNodeWithDescendantsAggregatesBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "grove", "read", len(aggsMap))
	c.JSON(http.StatusOK, model.GroveAggregatesBulkResponse{
		Aggregates: aggregatesBulkToMap(aggsMap),
		Missing:    nodeIDsToStrings(missing),
	})
}

func (h *groveHandler) getLocalAggregatesBulk(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	treeID := store_interface.TreeID(c.Param("treeId"))
	var req model.GroveBulkNodesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	nodeIDs := toNodeIDs(req.NodeIDs)
	aggsMap, missing, err := h.store.GetNodeLocalAggregatesBulk(space, treeID, nodeIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	incrementObjects(c, "grove", "read", len(aggsMap))
	c.JSON(http.StatusOK, model.GroveAggregatesBulkResponse{
		Aggregates: aggregatesBulkToMap(aggsMap),
		Missing:    nodeIDsToStrings(missing),
	})
}

// helpers

func aggregatesToMap(aggs map[store_interface.AggregateKey]store_interface.AggregateValue) map[string]int64 {
	m := make(map[string]int64, len(aggs))
	for k, v := range aggs {
		m[string(k)] = int64(v)
	}
	return m
}

func aggregatesBulkToMap(bulk map[store_interface.NodeID]map[store_interface.AggregateKey]store_interface.AggregateValue) map[string]map[string]int64 {
	result := make(map[string]map[string]int64, len(bulk))
	for node, aggs := range bulk {
		result[string(node)] = aggregatesToMap(aggs)
	}
	return result
}

func toNodeIDs(strs []string) []store_interface.NodeID {
	ids := make([]store_interface.NodeID, len(strs))
	for i, s := range strs {
		ids[i] = store_interface.NodeID(s)
	}
	return ids
}

func nodeIDsToStrings(ids []store_interface.NodeID) []string {
	strs := make([]string, len(ids))
	for i, id := range ids {
		strs[i] = string(id)
	}
	return strs
}
