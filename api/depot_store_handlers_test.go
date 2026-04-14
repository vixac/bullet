package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/ram"
)

func newDepotServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	store := ram.NewRamStore()
	engine := gin.New()
	SetupDepotRouter(store, "/depot", engine)
	srv := httptest.NewServer(engine.Handler())
	t.Cleanup(srv.Close)
	return srv, srv.URL + "/depot"
}

type depotClient struct {
	t   *testing.T
	srv *httptest.Server
}

func (d *depotClient) do(method, path string, body any) *http.Response {
	d.t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req, _ := http.NewRequest(method, d.srv.URL+"/depot"+path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "1")
	req.Header.Set("X-Tenancy-Id", "2")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(d.t, err)
	return resp
}

func TestDepotCreateAndGet(t *testing.T) {
	srv, _ := newDepotServer(t)
	c := &depotClient{t: t, srv: srv}

	const bucket = int32(42)

	// Create one
	createResp := c.do(http.MethodPost, "/items", model.DepotCreateRequest{BucketID: bucket, Value: "hello"})
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)
	var createBody model.DepotCreateResponse
	json.NewDecoder(createResp.Body).Decode(&createBody)
	createResp.Body.Close()
	id := createBody.ID
	assert.NotZero(t, id)

	// Get one
	getResp := c.do(http.MethodGet, "/items/"+strconv.FormatInt(id, 10), nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var getBody model.DepotGetResponse
	json.NewDecoder(getResp.Body).Decode(&getBody)
	getResp.Body.Close()
	assert.Equal(t, "hello", getBody.Value)

	// Get missing
	missingResp := c.do(http.MethodGet, "/items/99999", nil)
	assert.Equal(t, http.StatusNotFound, missingResp.StatusCode)
	missingResp.Body.Close()
}

func TestDepotUpdate(t *testing.T) {
	srv, _ := newDepotServer(t)
	c := &depotClient{t: t, srv: srv}

	createResp := c.do(http.MethodPost, "/items", model.DepotCreateRequest{BucketID: 1, Value: "original"})
	var createBody model.DepotCreateResponse
	json.NewDecoder(createResp.Body).Decode(&createBody)
	createResp.Body.Close()

	updateResp := c.do(http.MethodPut, "/items/"+strconv.FormatInt(createBody.ID, 10),
		model.DepotUpdateRequest{Value: "updated"})
	assert.Equal(t, http.StatusOK, updateResp.StatusCode)
	updateResp.Body.Close()

	getResp := c.do(http.MethodGet, "/items/"+strconv.FormatInt(createBody.ID, 10), nil)
	var getBody model.DepotGetResponse
	json.NewDecoder(getResp.Body).Decode(&getBody)
	getResp.Body.Close()
	assert.Equal(t, "updated", getBody.Value)
}

func TestDepotCreateManyAndGetMany(t *testing.T) {
	srv, _ := newDepotServer(t)
	c := &depotClient{t: t, srv: srv}

	createResp := c.do(http.MethodPost, "/items/batch", model.DepotCreateManyRequest{
		BucketID: 5, Values: []string{"alpha", "beta", "gamma"},
	})
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)
	var createBody model.DepotCreateManyResponse
	json.NewDecoder(createResp.Body).Decode(&createBody)
	createResp.Body.Close()
	assert.Len(t, createBody.IDs, 3)

	getManyResp := c.do(http.MethodPost, "/items/batch-get", model.DepotGetManyRequest{
		IDs: []int64{createBody.IDs[0], createBody.IDs[2], 99999},
	})
	assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
	var getManyBody model.DepotGetManyResponse
	json.NewDecoder(getManyResp.Body).Decode(&getManyBody)
	getManyResp.Body.Close()
	assert.Len(t, getManyBody.Values, 2)
	assert.Len(t, getManyBody.Missing, 1)
	assert.Contains(t, getManyBody.Missing, int64(99999))
}

func TestDepotDeleteOne(t *testing.T) {
	srv, _ := newDepotServer(t)
	c := &depotClient{t: t, srv: srv}

	createResp := c.do(http.MethodPost, "/items", model.DepotCreateRequest{BucketID: 1, Value: "bye"})
	var createBody model.DepotCreateResponse
	json.NewDecoder(createResp.Body).Decode(&createBody)
	createResp.Body.Close()

	delResp := c.do(http.MethodDelete, "/items/"+strconv.FormatInt(createBody.ID, 10), nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)
	delResp.Body.Close()

	getResp := c.do(http.MethodGet, "/items/"+strconv.FormatInt(createBody.ID, 10), nil)
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
	getResp.Body.Close()
}

func TestDepotDeleteByBucketAndGetAll(t *testing.T) {
	srv, _ := newDepotServer(t)
	c := &depotClient{t: t, srv: srv}

	const bucket = int32(77)

	// Create 3 items
	for _, v := range []string{"x", "y", "z"} {
		r := c.do(http.MethodPost, "/items", model.DepotCreateRequest{BucketID: bucket, Value: v})
		r.Body.Close()
	}

	// Get all by bucket
	getAllResp := c.do(http.MethodGet, "/bucket/77", nil)
	assert.Equal(t, http.StatusOK, getAllResp.StatusCode)
	var getAllBody model.DepotGetAllByBucketResponse
	json.NewDecoder(getAllResp.Body).Decode(&getAllBody)
	getAllResp.Body.Close()
	assert.Len(t, getAllBody.Values, 3)

	// Delete by bucket
	delBucketResp := c.do(http.MethodDelete, "/bucket/77", nil)
	assert.Equal(t, http.StatusNoContent, delBucketResp.StatusCode)
	delBucketResp.Body.Close()

	// Get all after delete — should be empty
	getAllAfter := c.do(http.MethodGet, "/bucket/77", nil)
	var emptyBody model.DepotGetAllByBucketResponse
	json.NewDecoder(getAllAfter.Body).Decode(&emptyBody)
	getAllAfter.Body.Close()
	assert.Len(t, emptyBody.Values, 0)
}
