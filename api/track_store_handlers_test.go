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

func newTrackServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	store := ram.NewRamStore()
	engine := gin.New()
	SetupTrackRouter(store, "/track", engine)
	srv := httptest.NewServer(engine.Handler())
	t.Cleanup(srv.Close)
	return srv, srv.URL + "/track"
}

func trackPost(t *testing.T, srv *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/track"+path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "1")
	req.Header.Set("X-Tenancy-Id", "2")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func trackDelete(t *testing.T, srv *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/track"+path, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "1")
	req.Header.Set("X-Tenancy-Id", "2")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func TestTrackUpsertAndGetOne(t *testing.T) {
	srv, _ := newTrackServer(t)

	tag := int64(7)
	metric := 3.14
	upsertResp := trackPost(t, srv, "/items", model.TrackRequest{
		BucketID: 10, Key: "hello", Value: 42, Tag: &tag, Metric: &metric,
	})
	assert.Equal(t, http.StatusOK, upsertResp.StatusCode)
	upsertResp.Body.Close()

	getResp := trackPost(t, srv, "/items/get", model.TrackRequest{BucketID: 10, Key: "hello"})
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var body map[string]int64
	json.NewDecoder(getResp.Body).Decode(&body)
	getResp.Body.Close()
	assert.Equal(t, int64(42), body["value"])
}

func TestTrackUpsertMany(t *testing.T) {
	srv, _ := newTrackServer(t)

	upsertResp := trackPost(t, srv, "/items/batch", model.TrackPutManyRequest{
		Buckets: []model.TrackPutItems{
			{BucketID: 5, Items: []model.TrackKeyValueItem{
				{Key: "a", Value: model.TrackValue{Value: 1}},
				{Key: "b", Value: model.TrackValue{Value: 2}},
			}},
		},
	})
	assert.Equal(t, http.StatusOK, upsertResp.StatusCode)
	upsertResp.Body.Close()

	getManyResp := trackPost(t, srv, "/items/batch-get", model.TrackGetManyRequest{
		Buckets: []model.TrackGetKeys{{BucketID: 5, Keys: []string{"a", "b", "missing"}}},
	})
	assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
	var body model.TrackGetManyResponse
	json.NewDecoder(getManyResp.Body).Decode(&body)
	getManyResp.Body.Close()

	assert.Equal(t, int64(1), body.Values["5"]["a"].Value)
	assert.Equal(t, int64(2), body.Values["5"]["b"].Value)
	assert.Contains(t, body.Missing["5"], "missing")
}

func TestTrackDeleteMany(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"k1", "k2", "k3"} {
		r := trackPost(t, srv, "/items", model.TrackRequest{BucketID: 10, Key: k, Value: 99})
		r.Body.Close()
	}

	delResp := trackDelete(t, srv, "/items", model.TrackDeleteManyRequest{
		Items: []model.TrackBucketKeyPair{
			{BucketID: 10, Key: "k1"},
			{BucketID: 10, Key: "k3"},
		},
	})
	assert.Equal(t, http.StatusOK, delResp.StatusCode)
	delResp.Body.Close()

	getManyResp := trackPost(t, srv, "/items/batch-get", model.TrackGetManyRequest{
		Buckets: []model.TrackGetKeys{{BucketID: 10, Keys: []string{"k1", "k2", "k3"}}},
	})
	assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
	var body model.TrackGetManyResponse
	json.NewDecoder(getManyResp.Body).Decode(&body)
	getManyResp.Body.Close()

	assert.Contains(t, body.Values["10"], "k2")
	assert.Contains(t, body.Missing["10"], "k1")
	assert.Contains(t, body.Missing["10"], "k3")
}

func TestTrackQueryByPrefix(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"foo:1", "foo:2", "bar:1"} {
		r := trackPost(t, srv, "/items", model.TrackRequest{BucketID: 1, Key: k, Value: 5})
		r.Body.Close()
	}

	queryResp := trackPost(t, srv, "/query", model.TrackGetItemsByPrefixRequest{
		BucketID: 1, Prefix: "foo:",
	})
	assert.Equal(t, http.StatusOK, queryResp.StatusCode)
	var body map[string][]model.TrackKeyValueItem
	json.NewDecoder(queryResp.Body).Decode(&body)
	queryResp.Body.Close()

	assert.Len(t, body["items"], 2)
}

func TestTrackQueryByPrefixes(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"foo:1", "bar:1", "baz:1"} {
		r := trackPost(t, srv, "/items", model.TrackRequest{BucketID: 1, Key: k, Value: 5})
		r.Body.Close()
	}

	queryResp := trackPost(t, srv, "/query/multi", model.TrackGetItemsByPrefixesRequest{
		BucketID: 1, Prefixes: []string{"foo:", "bar:"},
	})
	assert.Equal(t, http.StatusOK, queryResp.StatusCode)
	var body map[string][]model.TrackKeyValueItem
	json.NewDecoder(queryResp.Body).Decode(&body)
	queryResp.Body.Close()

	assert.Len(t, body["items"], 2)
}

func TestTrackMissingHeaders(t *testing.T) {
	store := ram.NewRamStore()
	engine := gin.New()
	SetupTrackRouter(store, "/track", engine)
	srv := httptest.NewServer(engine.Handler())
	defer srv.Close()

	b, _ := json.Marshal(model.TrackRequest{BucketID: 1, Key: "k", Value: 1})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/track/items", bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	// No X-App-Id or X-Tenancy-Id
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
