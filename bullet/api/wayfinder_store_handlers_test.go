package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/vixac/bullet/model"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

func TestWayFinder(t *testing.T) {
	for name, client := range clients {
		testGetOneForClient(client, name, t)
	}
}

func testGetOneForClient(client store_interface.Store, name string, t *testing.T) {

	t.Run(name, func(t *testing.T) {

		// setup server
		engine := gin.Default()
		engine = SetupWayFinderRouter(client, "test-wayfinder", engine)
		server := httptest.NewServer(engine.Handler())
		defer server.Close()

		baseURL := server.URL + "/test-wayfinder"

		// Insert one item
		payload := "this-is-a-payload"
		prefixKey := "foo:123"

		var bucketId int32 = 43

		putReq := model.WayFinderPutRequest{
			BucketId: bucketId,
			Key:      prefixKey,
			Payload:  payload,
			Tag:      nil,
			Metric:   nil,
		}

		putBody, _ := json.Marshal(putReq)
		req, _ := http.NewRequest("POST", baseURL+"/insert-one", bytes.NewBuffer(putBody))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-App-Id", "123")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("WayFinderInsertOne request failed: %v", err)
		}
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var putResp struct {
			ItemId int64 `json:"itemId"`
		}
		json.NewDecoder(resp.Body).Decode(&putResp)
		assert.NotZero(t, putResp.ItemId)

		// Query by prefix (positive match)
		queryReq := model.WayFinderPrefixQueryRequest{
			BucketId:   bucketId,
			Prefix:     "foo:",
			Tags:       nil,
			Metric:     nil,
			MetricIsGt: true,
		}
		queryBody, _ := json.Marshal(queryReq)
		queryHttpReq, _ := http.NewRequest("POST", baseURL+"/query-by-prefix", bytes.NewBuffer(queryBody))
		queryHttpReq.Header.Set("Content-Type", "application/json")
		queryHttpReq.Header.Set("X-App-Id", "123")

		queryResp, err := http.DefaultClient.Do(queryHttpReq)
		if err != nil {
			t.Fatalf("WayFinderQueryByPrefix request failed: %v", err)
		}
		defer queryResp.Body.Close()

		assert.Equal(t, http.StatusOK, queryResp.StatusCode)

		var queryRespBody struct {
			Items []model.WayFinderQueryItem `json:"items"`
		}
		json.NewDecoder(queryResp.Body).Decode(&queryRespBody)

		assert.Equal(t, 1, len(queryRespBody.Items))

		item := queryRespBody.Items[0]
		assert.Equal(t, prefixKey, item.Key)
		assert.Equal(t, payload, item.Payload)
		assert.Equal(t, putResp.ItemId, item.ItemId)
		assert.Nil(t, item.Tag)
		assert.Nil(t, item.Metric)

		// Query by prefix (no match)
		queryReq.Prefix = "bar:"
		queryBody, _ = json.Marshal(queryReq)
		noMatchReq, _ := http.NewRequest("POST", baseURL+"/query-by-prefix", bytes.NewBuffer(queryBody))
		noMatchReq.Header.Set("Content-Type", "application/json")
		noMatchReq.Header.Set("X-App-Id", "123")

		noMatchResp, err := http.DefaultClient.Do(noMatchReq)
		if err != nil {
			t.Fatalf("WayFinderQueryByPrefix (no match) request failed: %v", err)
		}
		defer noMatchResp.Body.Close()

		assert.Equal(t, http.StatusOK, noMatchResp.StatusCode)

		var noMatchRespBody struct {
			Items []model.WayFinderQueryItem `json:"items"`
		}
		json.NewDecoder(noMatchResp.Body).Decode(&noMatchRespBody)

		assert.True(t, noMatchRespBody.Items == nil || len(noMatchRespBody.Items) == 0)
	})

}

// VX:TODO test getbyprefix
func TestWayFinderGetOne(t *testing.T) {
	for name, client := range clients {
		testWayFinderGetOne(client, name, t)
	}
}

func testWayFinderGetOne(client store_interface.Store, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {

		engine := gin.Default()
		engine = SetupWayFinderRouter(client, "test-wayfinder", engine)
		server := httptest.NewServer(engine.Handler())
		defer server.Close()

		baseURL := server.URL + "/test-wayfinder"

		// Insert an item first so we can retrieve it
		payload := "this-is-the-payload"
		prefixKey := "foo:999"

		var tag int64 = 32
		var metric float64 = 1.23
		putReq := model.WayFinderPutRequest{
			BucketId: 42,
			Key:      prefixKey,
			Payload:  payload,
			Tag:      &tag,
			Metric:   &metric,
		}
		putBody, _ := json.Marshal(putReq)

		putHttpReq, _ := http.NewRequest("POST", baseURL+"/insert-one", bytes.NewBuffer(putBody))
		putHttpReq.Header.Set("Content-Type", "application/json")
		putHttpReq.Header.Set("X-App-Id", "123")

		putResp, err := http.DefaultClient.Do(putHttpReq)
		if err != nil {
			t.Fatalf("WayFinderInsertOne failed: %v", err)
		}
		defer putResp.Body.Close()
		assert.Equal(t, http.StatusOK, putResp.StatusCode)

		var putRespBody struct {
			ItemId int64 `json:"itemId"`
		}
		if err := json.NewDecoder(putResp.Body).Decode(&putRespBody); err != nil {
			t.Fatalf("Failed to decode insert-one response: %v", err)
		}
		assert.NotZero(t, putRespBody.ItemId)

		// Now attempt to get the same item via /get-one
		getReq := model.WayFinderGetOneRequest{
			BucketId: 42,
			Key:      prefixKey,
		}
		getBody, _ := json.Marshal(getReq)

		getHttpReq, _ := http.NewRequest("POST", baseURL+"/get-one", bytes.NewBuffer(getBody))
		getHttpReq.Header.Set("Content-Type", "application/json")
		getHttpReq.Header.Set("X-App-Id", "123")

		getResp, err := http.DefaultClient.Do(getHttpReq)
		if err != nil {
			t.Fatalf("WayFinderGetOne failed: %v", err)
		}
		defer getResp.Body.Close()
		assert.Equal(t, http.StatusOK, getResp.StatusCode)

		var getRespBody struct {
			Item model.WayFinderGetResponse `json:"item"`
		}
		if err := json.NewDecoder(getResp.Body).Decode(&getRespBody); err != nil {
			t.Fatalf("Failed to decode get-one response: %v", err)
		}

		assert.Equal(t, "this-is-the-payload", getRespBody.Item.Payload)
		assert.Equal(t, *getRespBody.Item.Tag, int64(32))
		assert.Equal(t, *getRespBody.Item.Metric, 1.23)
		assert.NotNil(t, getRespBody.Item.ItemId)

		// Negative case: try to get a non-existent item
		missingGetReq := model.WayFinderGetOneRequest{
			BucketId: 42,
			Key:      "nonexistent-key",
		}
		missingBody, _ := json.Marshal(missingGetReq)

		missingHttpReq, _ := http.NewRequest("POST", baseURL+"/get-one", bytes.NewBuffer(missingBody))
		missingHttpReq.Header.Set("Content-Type", "application/json")
		missingHttpReq.Header.Set("X-App-Id", "123")

		missingResp, err := http.DefaultClient.Do(missingHttpReq)
		if err != nil {
			t.Fatalf("WayFinderGetOne (missing) failed: %v", err)
		}
		defer missingResp.Body.Close()
		assert.Equal(t, http.StatusNotFound, missingResp.StatusCode)
	})
}
