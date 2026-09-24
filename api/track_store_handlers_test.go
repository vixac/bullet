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
	"github.com/vixac/bullet/protocol"
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
	upsertResp := trackPost(t, srv, "/items", protocol.TrackRequest{
		BucketID: 10, Key: "hello", Value: 42, Tag: &tag, Metric: &metric,
	})
	assert.Equal(t, http.StatusOK, upsertResp.StatusCode)
	upsertResp.Body.Close()

	getResp := trackPost(t, srv, "/items/get", protocol.TrackGetRequest{BucketID: 10, Key: "hello"})
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var body protocol.TrackGetResponse
	json.NewDecoder(getResp.Body).Decode(&body)
	getResp.Body.Close()
	assert.Equal(t, int64(42), body.Value.Value)
	assert.Nil(t, body.Value.Payload)
}

func TestTrackPayloadHTTPContract(t *testing.T) {
	srv, _ := newTrackServer(t)
	payload := []byte("hello")
	put := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 1, Key: "k", Value: 7, Payload: &payload})
	require.Equal(t, http.StatusOK, put.StatusCode)
	put.Body.Close()

	without := trackPost(t, srv, "/items/get", protocol.TrackGetRequest{BucketID: 1, Key: "k"})
	require.Equal(t, http.StatusOK, without.StatusCode)
	var withoutBody map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(without.Body).Decode(&withoutBody))
	without.Body.Close()
	assert.NotContains(t, string(withoutBody["value"]), "Payload")

	with := trackPost(t, srv, "/items/get", protocol.TrackGetRequest{BucketID: 1, Key: "k", IncludePayload: true})
	require.Equal(t, http.StatusOK, with.StatusCode)
	var withBody protocol.TrackGetResponse
	require.NoError(t, json.NewDecoder(with.Body).Decode(&withBody))
	with.Body.Close()
	require.NotNil(t, withBody.Value.Payload)
	assert.Equal(t, payload, *withBody.Value.Payload)

	tooLarge := bytes.Repeat([]byte{'x'}, 64*1024+1)
	rejected := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 1, Key: "large", Payload: &tooLarge})
	require.Equal(t, http.StatusBadRequest, rejected.StatusCode)
	var failure protocol.ErrorResponse
	require.NoError(t, json.NewDecoder(rejected.Body).Decode(&failure))
	rejected.Body.Close()
	assert.Equal(t, "track_payload_too_large", failure.Code)
}

func TestTrackUpsertMany(t *testing.T) {
	srv, _ := newTrackServer(t)

	upsertResp := trackPost(t, srv, "/items/batch", protocol.TrackPutManyRequest{
		Buckets: []protocol.TrackPutItems{
			{BucketID: 5, Items: []protocol.TrackKeyValueItem{
				{Key: "a", Value: protocol.TrackValue{Value: 1}},
				{Key: "b", Value: protocol.TrackValue{Value: 2}},
			}},
		},
	})
	assert.Equal(t, http.StatusOK, upsertResp.StatusCode)
	upsertResp.Body.Close()

	getManyResp := trackPost(t, srv, "/items/batch-get", protocol.TrackGetManyRequest{
		Buckets: []protocol.TrackGetKeys{{BucketID: 5, Keys: []string{"a", "b", "missing"}}},
	})
	assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
	var body protocol.TrackGetManyResponse
	json.NewDecoder(getManyResp.Body).Decode(&body)
	getManyResp.Body.Close()

	assert.Equal(t, int64(1), body.Values["5"]["a"].Value)
	assert.Equal(t, int64(2), body.Values["5"]["b"].Value)
	assert.Contains(t, body.Missing["5"], "missing")
}

func TestTrackDeleteMany(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"k1", "k2", "k3"} {
		r := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 10, Key: k, Value: 99})
		r.Body.Close()
	}

	delResp := trackDelete(t, srv, "/items", protocol.TrackDeleteManyRequest{
		Items: []protocol.TrackBucketKeyPair{
			{BucketID: 10, Key: "k1"},
			{BucketID: 10, Key: "k3"},
		},
	})
	assert.Equal(t, http.StatusOK, delResp.StatusCode)
	delResp.Body.Close()

	getManyResp := trackPost(t, srv, "/items/batch-get", protocol.TrackGetManyRequest{
		Buckets: []protocol.TrackGetKeys{{BucketID: 10, Keys: []string{"k1", "k2", "k3"}}},
	})
	assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
	var body protocol.TrackGetManyResponse
	json.NewDecoder(getManyResp.Body).Decode(&body)
	getManyResp.Body.Close()

	assert.Contains(t, body.Values["10"], "k2")
	assert.Contains(t, body.Missing["10"], "k1")
	assert.Contains(t, body.Missing["10"], "k3")
}

func TestTrackQueryByPrefix(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"foo:1", "foo:2", "bar:1"} {
		r := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 1, Key: k, Value: 5})
		r.Body.Close()
	}

	queryResp := trackPost(t, srv, "/query", protocol.TrackGetItemsByPrefixRequest{
		BucketID: 1, Prefix: "foo:",
	})
	assert.Equal(t, http.StatusOK, queryResp.StatusCode)
	var body map[string][]protocol.TrackKeyValueItem
	json.NewDecoder(queryResp.Body).Decode(&body)
	queryResp.Body.Close()

	assert.Len(t, body["items"], 2)
}

func TestTrackQueryByPrefixes(t *testing.T) {
	srv, _ := newTrackServer(t)

	for _, k := range []string{"foo:1", "bar:1", "baz:1"} {
		r := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 1, Key: k, Value: 5})
		r.Body.Close()
	}

	queryResp := trackPost(t, srv, "/query/multi", protocol.TrackGetItemsByPrefixesRequest{
		BucketID: 1, Prefixes: []string{"foo:", "bar:"},
	})
	assert.Equal(t, http.StatusOK, queryResp.StatusCode)
	var body map[string][]protocol.TrackKeyValueItem
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

	b, _ := json.Marshal(protocol.TrackRequest{BucketID: 1, Key: "k", Value: 1})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/track/items", bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	// No X-App-Id or X-Tenancy-Id
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestTrackMutate(t *testing.T) {
	srv, _ := newTrackServer(t)
	seed := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 10, Key: "old", Value: 1})
	require.Equal(t, http.StatusOK, seed.StatusCode)
	seed.Body.Close()

	tag, metric := int64(7), 3.14
	req := protocol.TrackMutateRequest{
		MutationID: "mutation-1",
		Puts:       []protocol.TrackRequest{{BucketID: 20, Key: "new", Value: 9007199254740993, Tag: &tag, Metric: &metric}},
		Deletes:    []protocol.TrackBucketKeyPair{{BucketID: 10, Key: "old"}},
	}
	for _, applied := range []bool{true, false} {
		resp := trackPost(t, srv, "/mutate", req)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result protocol.TrackMutateResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		resp.Body.Close()
		assert.Equal(t, applied, result.Applied)
		// A retry with changed values must not apply again.
		req.Puts[0].Value = 2
	}
	resp := trackPost(t, srv, "/items/batch-get", protocol.TrackGetManyRequest{
		Buckets: []protocol.TrackGetKeys{{BucketID: 10, Keys: []string{"old"}}, {BucketID: 20, Keys: []string{"new"}}},
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result protocol.TrackGetManyResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Contains(t, result.Missing["10"], "old")
	assert.Equal(t, protocol.TrackValue{Value: 9007199254740993, Tag: &tag, Metric: &metric}, result.Values["20"]["new"])
}

func TestTrackMutateIfAbsent(t *testing.T) {
	srv, _ := newTrackServer(t)
	seed := trackPost(t, srv, "/items", protocol.TrackRequest{BucketID: 1, Key: "existing", Value: 1})
	require.Equal(t, http.StatusOK, seed.StatusCode)
	seed.Body.Close()

	req := protocol.TrackMutateRequest{MutationID: "if-absent", Puts: []protocol.TrackRequest{
		{BucketID: 1, Key: "new", Value: 2, IfAbsent: true},
		{BucketID: 1, Key: "existing", Value: 3, IfAbsent: true},
	}}
	resp := trackPost(t, srv, "/mutate", req)
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var failure protocol.ErrorResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&failure))
	resp.Body.Close()
	assert.Equal(t, "track_key_already_exists", failure.Code)

	result := trackPost(t, srv, "/items/batch-get", protocol.TrackGetManyRequest{Buckets: []protocol.TrackGetKeys{{BucketID: 1, Keys: []string{"new", "existing"}}}})
	defer result.Body.Close()
	require.Equal(t, http.StatusOK, result.StatusCode)
	var values protocol.TrackGetManyResponse
	require.NoError(t, json.NewDecoder(result.Body).Decode(&values))
	assert.Contains(t, values.Missing["1"], "new")
	assert.Equal(t, int64(1), values.Values["1"]["existing"].Value)
}

func TestTrackMutateInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		headers bool
		status  int
	}{
		{"missing headers", `{"mutationId":"m"}`, false, http.StatusUnauthorized},
		{"malformed JSON", `{`, true, http.StatusBadRequest},
		{"empty mutation ID", `{"puts":[]}`, true, http.StatusOK},
		{"invalid value", `{"mutationId":"m","puts":[{"value":"invalid"}]}`, true, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := gin.New()
			SetupTrackRouter(ram.NewRamStore(), "/track", engine)
			req := httptest.NewRequest(http.MethodPost, "/track/mutate", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			if tc.headers {
				req.Header.Set("X-App-Id", "1")
				req.Header.Set("X-Tenancy-Id", "2")
			}
			resp := httptest.NewRecorder()
			engine.ServeHTTP(resp, req)
			assert.Equal(t, tc.status, resp.Code)
		})
	}
}
