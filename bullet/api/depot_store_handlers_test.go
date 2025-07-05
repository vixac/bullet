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

func TestDepot(t *testing.T) {
	println("Starting Depot test...")

	engine := gin.Default()
	engine = SetupDepotRouter(g_mongoStore, "test-depot", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	baseURL := server.URL + "/test-depot"

	// Insert one item
	insertReq := model.DepotRequest{
		Key:   123,
		Value: "this-is-a-payload",
	}
	insertBody, _ := json.Marshal(insertReq)
	insertHttpReq, _ := http.NewRequest("POST", baseURL+"/insert-one", bytes.NewBuffer(insertBody))
	insertHttpReq.Header.Set("Content-Type", "application/json")
	insertHttpReq.Header.Set("X-App-Id", "123")

	insertResp, err := http.DefaultClient.Do(insertHttpReq)
	if err != nil {
		t.Fatalf("DepotInsertOne failed: %v", err)
	}
	defer insertResp.Body.Close()
	assert.Equal(t, http.StatusOK, insertResp.StatusCode)

	// Query for multiple keys
	keys := []string{"123", "234"}
	getReq := model.DepotGetManyRequest{
		Keys: keys,
	}
	getBody, _ := json.Marshal(getReq)
	getHttpReq, _ := http.NewRequest("POST", baseURL+"/get-many", bytes.NewBuffer(getBody))
	getHttpReq.Header.Set("Content-Type", "application/json")
	getHttpReq.Header.Set("X-App-Id", "123")

	getResp, err := http.DefaultClient.Do(getHttpReq)
	if err != nil {
		t.Fatalf("DepotGetMany failed: %v", err)
	}
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusOK, getResp.StatusCode)

	var getRespBody model.DepotGetManyResponse
	if err := json.NewDecoder(getResp.Body).Decode(&getRespBody); err != nil {
		t.Fatalf("Failed to decode DepotGetManyResponse: %v", err)
	}

	// Validate response
	assert.NotNil(t, getRespBody.Values)
	assert.Equal(t, 1, len(getRespBody.Values))

	found, ok := getRespBody.Values[123]
	assert.True(t, ok)
	assert.Equal(t, "this-is-a-payload", found)

	assert.Equal(t, 1, len(getRespBody.Missing))
	assert.Equal(t, int64(234), getRespBody.Missing[0])
}
