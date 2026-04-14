package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/ram"
)

func newGroveServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	store := ram.NewRamStore()
	engine := gin.New()
	SetupGroveRouter(store, "/grove", engine)
	srv := httptest.NewServer(engine.Handler())
	t.Cleanup(srv.Close)
	return srv, srv.URL + "/grove"
}

type groveClient struct {
	t      *testing.T
	srv    *httptest.Server
	treeID string
}

func (g *groveClient) do(method, path string, body any) *http.Response {
	g.t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, g.srv.URL+"/grove/trees/"+g.treeID+path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "1")
	req.Header.Set("X-Tenancy-Id", "2")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(g.t, err)
	return resp
}

func (g *groveClient) createNode(nodeID string, parentID *string) {
	g.t.Helper()
	resp := g.do(http.MethodPost, "/nodes", model.GroveCreateNodeRequest{
		NodeID:   nodeID,
		ParentID: parentID,
	})
	resp.Body.Close()
	require.Equal(g.t, http.StatusCreated, resp.StatusCode)
}

func ptr(s string) *string { return &s }

func TestGroveBasicTree(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree1"}

	// Build: root -> A -> B, root -> C
	c.createNode("root", nil)
	c.createNode("A", ptr("root"))
	c.createNode("B", ptr("A"))
	c.createNode("C", ptr("root"))

	// Exists
	existsResp := c.do(http.MethodGet, "/nodes/root/exists", nil)
	assert.Equal(t, http.StatusOK, existsResp.StatusCode)
	var existsBody model.GroveExistsResponse
	json.NewDecoder(existsResp.Body).Decode(&existsBody)
	existsResp.Body.Close()
	assert.True(t, existsBody.Exists)

	noExistsResp := c.do(http.MethodGet, "/nodes/ghost/exists", nil)
	var noExistsBody model.GroveExistsResponse
	json.NewDecoder(noExistsResp.Body).Decode(&noExistsBody)
	noExistsResp.Body.Close()
	assert.False(t, noExistsBody.Exists)

	// Node info for A
	infoResp := c.do(http.MethodGet, "/nodes/A", nil)
	assert.Equal(t, http.StatusOK, infoResp.StatusCode)
	var infoBody model.GroveNodeInfoResponse
	json.NewDecoder(infoResp.Body).Decode(&infoBody)
	infoResp.Body.Close()
	assert.Equal(t, "A", infoBody.ID)
	require.NotNil(t, infoBody.ParentID)
	assert.Equal(t, "root", *infoBody.ParentID)
	assert.Equal(t, 1, infoBody.Depth)

	// Children of root
	childResp := c.do(http.MethodGet, "/nodes/root/children", nil)
	assert.Equal(t, http.StatusOK, childResp.StatusCode)
	var childBody model.GroveChildrenResponse
	json.NewDecoder(childResp.Body).Decode(&childBody)
	childResp.Body.Close()
	assert.ElementsMatch(t, []string{"A", "C"}, childBody.Children)

	// Ancestors of B
	ancestorResp := c.do(http.MethodGet, "/nodes/B/ancestors", nil)
	assert.Equal(t, http.StatusOK, ancestorResp.StatusCode)
	var ancestorBody model.GroveAncestorsResponse
	json.NewDecoder(ancestorResp.Body).Decode(&ancestorBody)
	ancestorResp.Body.Close()
	assert.Contains(t, ancestorBody.Ancestors, "A")
	assert.Contains(t, ancestorBody.Ancestors, "root")

	// Descendants of root
	descResp := c.do(http.MethodGet, "/nodes/root/descendants", nil)
	assert.Equal(t, http.StatusOK, descResp.StatusCode)
	var descBody model.GroveDescendantsResponse
	json.NewDecoder(descResp.Body).Decode(&descBody)
	descResp.Body.Close()
	descIDs := make([]string, len(descBody.Descendants))
	for i, d := range descBody.Descendants {
		descIDs[i] = d.NodeID
	}
	assert.ElementsMatch(t, []string{"A", "B", "C"}, descIDs)
}

func TestGroveMoveNode(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree2"}

	c.createNode("root", nil)
	c.createNode("A", ptr("root"))
	c.createNode("C", ptr("root"))

	// Move C to be a child of A
	moveResp := c.do(http.MethodPatch, "/nodes/C", model.GroveMoveNodeRequest{NewParentID: ptr("A")})
	assert.Equal(t, http.StatusOK, moveResp.StatusCode)
	moveResp.Body.Close()

	// Ancestors of C should now include A and root
	ancestorResp := c.do(http.MethodGet, "/nodes/C/ancestors", nil)
	var ancestorBody model.GroveAncestorsResponse
	json.NewDecoder(ancestorResp.Body).Decode(&ancestorBody)
	ancestorResp.Body.Close()
	assert.Contains(t, ancestorBody.Ancestors, "A")
	assert.Contains(t, ancestorBody.Ancestors, "root")
}

func TestGroveDeleteNode(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree3"}

	c.createNode("root", nil)
	c.createNode("leaf", ptr("root"))

	delResp := c.do(http.MethodDelete, "/nodes/leaf", nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)
	delResp.Body.Close()

	existsResp := c.do(http.MethodGet, "/nodes/leaf/exists", nil)
	var existsBody model.GroveExistsResponse
	json.NewDecoder(existsResp.Body).Decode(&existsBody)
	existsResp.Body.Close()
	assert.False(t, existsBody.Exists)
}

func TestGroveAggregates(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree4"}

	c.createNode("root", nil)
	c.createNode("A", ptr("root"))
	c.createNode("B", ptr("A"))

	// Apply mutation to A
	mutResp := c.do(http.MethodPost, "/nodes/A/mutations", model.GroveApplyMutationRequest{
		MutationID: "mut1",
		Deltas:     map[string]int64{"score": 10, "count": 1},
	})
	assert.Equal(t, http.StatusOK, mutResp.StatusCode)
	mutResp.Body.Close()

	// Apply mutation to B
	mutResp2 := c.do(http.MethodPost, "/nodes/B/mutations", model.GroveApplyMutationRequest{
		MutationID: "mut2",
		Deltas:     map[string]int64{"score": 5},
	})
	assert.Equal(t, http.StatusOK, mutResp2.StatusCode)
	mutResp2.Body.Close()

	// Idempotency: applying same mutation again should be a no-op (409)
	idempResp := c.do(http.MethodPost, "/nodes/A/mutations", model.GroveApplyMutationRequest{
		MutationID: "mut1",
		Deltas:     map[string]int64{"score": 10},
	})
	assert.Equal(t, http.StatusConflict, idempResp.StatusCode)
	idempResp.Body.Close()

	// Local aggregates on A (score=10, count=1 — B's score not included)
	localResp := c.do(http.MethodGet, "/nodes/A/aggregates/local", nil)
	assert.Equal(t, http.StatusOK, localResp.StatusCode)
	var localBody model.GroveAggregatesResponse
	json.NewDecoder(localResp.Body).Decode(&localBody)
	localResp.Body.Close()
	assert.Equal(t, int64(10), localBody.Aggregates["score"])
	assert.Equal(t, int64(1), localBody.Aggregates["count"])

	// Subtree aggregates on A (score = 10+5 = 15)
	subtreeResp := c.do(http.MethodGet, "/nodes/A/aggregates", nil)
	assert.Equal(t, http.StatusOK, subtreeResp.StatusCode)
	var subtreeBody model.GroveAggregatesResponse
	json.NewDecoder(subtreeResp.Body).Decode(&subtreeBody)
	subtreeResp.Body.Close()
	assert.Equal(t, int64(15), subtreeBody.Aggregates["score"])
}

func TestGroveBulkOps(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree5"}

	c.createNode("root", nil)
	c.createNode("A", ptr("root"))
	c.createNode("B", ptr("A"))

	// Apply some mutations
	c.do(http.MethodPost, "/nodes/A/mutations", model.GroveApplyMutationRequest{
		MutationID: "m1", Deltas: map[string]int64{"pts": 20},
	}).Body.Close()
	c.do(http.MethodPost, "/nodes/B/mutations", model.GroveApplyMutationRequest{
		MutationID: "m2", Deltas: map[string]int64{"pts": 10},
	}).Body.Close()

	// Bulk ancestors for A and B
	bulkAncResp := c.do(http.MethodPost, "/bulk/ancestors", model.GroveBulkNodesRequest{
		NodeIDs: []string{"A", "B"},
	})
	assert.Equal(t, http.StatusOK, bulkAncResp.StatusCode)
	var bulkAncBody model.GroveAncestorsBulkResponse
	json.NewDecoder(bulkAncResp.Body).Decode(&bulkAncBody)
	bulkAncResp.Body.Close()
	assert.Contains(t, bulkAncBody.Ancestors["A"], "root")
	assert.Contains(t, bulkAncBody.Ancestors["B"], "A")
	assert.Contains(t, bulkAncBody.Ancestors["B"], "root")

	// Bulk local aggregates
	bulkLocalResp := c.do(http.MethodPost, "/bulk/aggregates/local", model.GroveBulkNodesRequest{
		NodeIDs: []string{"A", "B", "missing"},
	})
	assert.Equal(t, http.StatusOK, bulkLocalResp.StatusCode)
	var bulkLocalBody model.GroveAggregatesBulkResponse
	json.NewDecoder(bulkLocalResp.Body).Decode(&bulkLocalBody)
	bulkLocalResp.Body.Close()
	assert.Equal(t, int64(20), bulkLocalBody.Aggregates["A"]["pts"])
	assert.Equal(t, int64(10), bulkLocalBody.Aggregates["B"]["pts"])
	assert.Contains(t, bulkLocalBody.Missing, "missing")

	// Bulk subtree aggregates
	bulkSubResp := c.do(http.MethodPost, "/bulk/aggregates", model.GroveBulkNodesRequest{
		NodeIDs: []string{"A"},
	})
	assert.Equal(t, http.StatusOK, bulkSubResp.StatusCode)
	var bulkSubBody model.GroveAggregatesBulkResponse
	json.NewDecoder(bulkSubResp.Body).Decode(&bulkSubBody)
	bulkSubResp.Body.Close()
	// A subtree includes B, so total pts = 20+10 = 30
	assert.Equal(t, int64(30), bulkSubBody.Aggregates["A"]["pts"])
}

func TestGroveNotFound(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree6"}

	infoResp := c.do(http.MethodGet, "/nodes/ghost", nil)
	assert.Equal(t, http.StatusNotFound, infoResp.StatusCode)
	infoResp.Body.Close()
}

func TestGroveConflict(t *testing.T) {
	srv, _ := newGroveServer(t)
	c := &groveClient{t: t, srv: srv, treeID: "tree7"}

	c.createNode("root", nil)

	// Creating the same node twice → 409
	dupResp := c.do(http.MethodPost, "/nodes", model.GroveCreateNodeRequest{NodeID: "root"})
	assert.Equal(t, http.StatusConflict, dupResp.StatusCode)
	dupResp.Body.Close()
}
