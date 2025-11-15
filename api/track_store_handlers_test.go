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

func TestTrackInsertOneAndGetOne(t *testing.T) {
	for name, client := range clients {
		testTrackInsertOneAndGetOne(client, name, t)
	}
}

//VX:Note this test suite doesnt test all of track,yet, but wayfinder tests will cover alot of the functionality.

func testTrackInsertOneAndGetOne(client store_interface.Store, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {

		engine := gin.Default()
		engine = SetupTrackRouter(client, "test-track", engine)
		server := httptest.NewServer(engine.Handler())
		defer server.Close()

		baseURL := server.URL + "/test-track"

		// Insert one item
		var tag int64 = 1
		var metric float64 = 42.5
		insertReq := model.TrackRequest{
			BucketID: 42,
			Key:      "foo:1",
			Value:    100,
			Tag:      &tag,
			Metric:   &metric,
		}
		insertBody, _ := json.Marshal(insertReq)

		insertHttpReq, _ := http.NewRequest("POST", baseURL+"/insert-one", bytes.NewBuffer(insertBody))
		insertHttpReq.Header.Set("Content-Type", "application/json")
		insertHttpReq.Header.Set("X-App-Id", "123")

		insertResp, err := http.DefaultClient.Do(insertHttpReq)
		if err != nil {
			t.Fatalf("TrackInsertOne failed: %v", err)
		}
		defer insertResp.Body.Close()
		assert.Equal(t, http.StatusOK, insertResp.StatusCode)

		// Get many
		getReq := model.TrackGetManyRequest{
			Buckets: []model.TrackGetKeys{
				{
					BucketID: 42,
					Keys:     []string{"foo:1", "foo:2"},
				},
			},
		}
		getBody, _ := json.Marshal(getReq)

		getHttpReq, _ := http.NewRequest("POST", baseURL+"/get-many", bytes.NewBuffer(getBody))
		getHttpReq.Header.Set("Content-Type", "application/json")
		getHttpReq.Header.Set("X-App-Id", "123")

		getResp, err := http.DefaultClient.Do(getHttpReq)
		if err != nil {
			t.Fatalf("TrackGetMany failed: %v", err)
		}
		defer getResp.Body.Close()
		assert.Equal(t, http.StatusOK, getResp.StatusCode)

		var getRespBody model.TrackGetManyResponse
		if err := json.NewDecoder(getResp.Body).Decode(&getRespBody); err != nil {
			t.Fatalf("Failed to decode TrackGetManyResponse: %v", err)
		}

		// Validate values
		assert.NotNil(t, getRespBody.Values)
		bucket42, ok := getRespBody.Values["42"]
		assert.True(t, ok)

		foo1, ok := bucket42["foo:1"]
		assert.True(t, ok)
		assert.Equal(t, int64(100), foo1.Value)
		assert.NotNil(t, foo1.Tag)
		assert.Equal(t, int64(1), *foo1.Tag)
		assert.NotNil(t, foo1.Metric)
		assert.Equal(t, 42.5, *foo1.Metric)

		// Validate missing
		assert.Equal(t, 1, len(getRespBody.Missing))
		missing42, ok := getRespBody.Missing["42"]
		assert.True(t, ok)
		assert.Equal(t, 1, len(missing42))
		assert.Equal(t, "foo:2", missing42[0])
	})
}
