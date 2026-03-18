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

// depotClients contains only stores with real DepotStore implementations.
// Mongo and BoltDB depot stores are stubs and are excluded.
var depotClientNames = []string{"ram_store", "sqlite"}

func TestDepot(t *testing.T) {
	for _, name := range depotClientNames {
		client, ok := clients[name]
		if !ok {
			t.Fatalf("client %q not found in clients map", name)
		}
		testDepot(client, name, t)
	}
}

func testDepot(client store_interface.Store, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		engine := gin.Default()
		engine = SetupDepotRouter(client, "test-depot", engine)
		server := httptest.NewServer(engine.Handler())
		defer server.Close()

		base := server.URL + "/test-depot"
		appHeader := func(r *http.Request) *http.Request {
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("X-App-Id", "123")
			return r
		}
		post := func(path string, body any) *http.Response {
			b, _ := json.Marshal(body)
			req, _ := http.NewRequest("POST", base+path, bytes.NewBuffer(b))
			appHeader(req)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("POST %s failed: %v", path, err)
			}
			return resp
		}

		const bucket = int32(42)

		// Create one item
		createResp := post("/create-one", model.DepotCreateRequest{BucketID: bucket, Value: "hello"})
		assert.Equal(t, http.StatusOK, createResp.StatusCode)
		var createBody model.DepotCreateResponse
		json.NewDecoder(createResp.Body).Decode(&createBody)
		createResp.Body.Close()
		id := createBody.ID
		assert.NotZero(t, id)

		// Get one by id
		getResp := post("/get-one", model.DepotGetRequest{ID: id})
		assert.Equal(t, http.StatusOK, getResp.StatusCode)
		var getBody model.DepotGetResponse
		json.NewDecoder(getResp.Body).Decode(&getBody)
		getResp.Body.Close()
		assert.Equal(t, "hello", getBody.Value)

		// Get one with non-existent id returns 404
		missingResp := post("/get-one", model.DepotGetRequest{ID: id + 99999})
		assert.Equal(t, http.StatusNotFound, missingResp.StatusCode)
		missingResp.Body.Close()

		// Update
		updateResp := post("/update", model.DepotUpdateRequest{ID: id, Value: "updated"})
		assert.Equal(t, http.StatusOK, updateResp.StatusCode)
		updateResp.Body.Close()

		getAfterUpdate := post("/get-one", model.DepotGetRequest{ID: id})
		var updatedBody model.DepotGetResponse
		json.NewDecoder(getAfterUpdate.Body).Decode(&updatedBody)
		getAfterUpdate.Body.Close()
		assert.Equal(t, "updated", updatedBody.Value)

		// Create many
		createManyResp := post("/create-many", model.DepotCreateManyRequest{
			BucketID: bucket,
			Values:   []string{"alpha", "beta", "gamma"},
		})
		assert.Equal(t, http.StatusOK, createManyResp.StatusCode)
		var createManyBody model.DepotCreateManyResponse
		json.NewDecoder(createManyResp.Body).Decode(&createManyBody)
		createManyResp.Body.Close()
		assert.Equal(t, 3, len(createManyBody.IDs))

		// Get many — mix of found and missing
		bogusID := createManyBody.IDs[0] + 99999
		getManyResp := post("/get-many", model.DepotGetManyRequest{
			IDs: []int64{createManyBody.IDs[0], bogusID},
		})
		assert.Equal(t, http.StatusOK, getManyResp.StatusCode)
		var getManyBody model.DepotGetManyResponse
		json.NewDecoder(getManyResp.Body).Decode(&getManyBody)
		getManyResp.Body.Close()
		assert.Equal(t, 1, len(getManyBody.Values))
		assert.Equal(t, 1, len(getManyBody.Missing))

		// Get all by bucket — should contain original + createMany items
		getAllResp := post("/get-all-by-bucket", model.DepotBucketRequest{BucketID: bucket})
		assert.Equal(t, http.StatusOK, getAllResp.StatusCode)
		var getAllBody model.DepotGetAllByBucketResponse
		json.NewDecoder(getAllResp.Body).Decode(&getAllBody)
		getAllResp.Body.Close()
		assert.Equal(t, 4, len(getAllBody.Values)) // 1 original + 3 from create-many

		// Delete one
		delResp := post("/delete-one", model.DepotDeleteRequest{ID: id})
		assert.Equal(t, http.StatusOK, delResp.StatusCode)
		delResp.Body.Close()

		afterDel := post("/get-one", model.DepotGetRequest{ID: id})
		assert.Equal(t, http.StatusNotFound, afterDel.StatusCode)
		afterDel.Body.Close()

		// Delete by bucket — removes the createMany items
		delBucketResp := post("/delete-by-bucket", model.DepotBucketRequest{BucketID: bucket})
		assert.Equal(t, http.StatusOK, delBucketResp.StatusCode)
		delBucketResp.Body.Close()

		getAllAfterDel := post("/get-all-by-bucket", model.DepotBucketRequest{BucketID: bucket})
		var emptyBody model.DepotGetAllByBucketResponse
		json.NewDecoder(getAllAfterDel.Body).Decode(&emptyBody)
		getAllAfterDel.Body.Close()
		assert.Equal(t, 0, len(emptyBody.Values))
	})
}
