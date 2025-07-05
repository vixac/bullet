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
)

func TestWayFinder(t *testing.T) {
	println("Starting WayFinder test...")

	// setup server
	engine := gin.Default()
	engine = SetupWayFinderRouter(g_mongoStore, "test-wayfinder", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	baseURL := server.URL + "/test-wayfinder"

	// Insert one item
	payload := "this-is-a-payload"
	prefixKey := "foo:123"

	putReq := model.WayFinderPutRequest{
		BucketId: 42,
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
		BucketId:    42,
		Prefix:      "foo:",
		Tags:        nil,
		MetricValue: nil,
		MetricIsGt:  true,
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
}
