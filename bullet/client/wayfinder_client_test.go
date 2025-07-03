package client

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/model"
)

func TestWayFinder(t *testing.T) {
	println("Starting WayFinder test...")

	// setup server
	engine := gin.Default()
	engine = api.SetupWayFinderRouter(g_mongoStore, "test-wayfinder", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	// setup client
	client := NewBulletClient(server.URL+"/test-wayfinder", 123)

	// Insert one item
	payload := "this-is-a-payload"
	prefixKey := "foo:123"

	req := model.WayFinderPutRequest{
		BucketId: 42,
		Key:      prefixKey,
		Payload:  payload,
		Tag:      nil,
		Metric:   nil,
	}

	itemId, err := client.WayFinderInsertOne(req)
	if err != nil {
		t.Fatalf("WayFinderInsertOne failed: %v", err)
	}
	assert.NotZero(t, itemId)

	// Query by prefix (positive match)
	queryReq := model.WayFinderPrefixQueryRequest{
		BucketId:    42,
		Prefix:      "foo:",
		Tags:        nil,
		MetricValue: nil,
		MetricIsGt:  true,
	}

	items, err := client.WayFinderQueryByPrefix(queryReq)
	if err != nil {
		t.Fatalf("WayFinderQueryByPrefix failed: %v", err)
	}

	assert.NotNil(t, items)
	assert.Equal(t, 1, len(items))

	item := items[0]
	assert.Equal(t, prefixKey, item.Key)
	assert.Equal(t, payload, item.Payload)
	assert.Equal(t, itemId, item.ItemId)
	assert.Nil(t, item.Tag)
	assert.Nil(t, item.Metric)

	// Query by prefix (no match)
	queryReq.Prefix = "bar:"
	items, err = client.WayFinderQueryByPrefix(queryReq)
	if err != nil {
		t.Fatalf("WayFinderQueryByPrefix with no matches failed: %v", err)
	}
	assert.Nil(t, items) //or len 0 but in our case its nil
}
