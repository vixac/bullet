package client

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/model"
)

// VX:Note This is the beginning of what will end up being the bullet spec.
func TestTrackInsertOneAndGetOne(t *testing.T) {

	println("Starting Track test...")
	engine := gin.Default()
	engine = api.SetupTrackRouter(g_mongoStore, "test-track", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	client := NewBulletClient(server.URL+"/test-track", 123)

	var tag int64 = 1
	var metric float64 = 42.5
	err := client.TrackInsertOne(42, "foo:1", 100, &tag, &metric)
	if err != nil {
		t.Fatal(err)
	}
	//get many
	req := model.TrackGetManyRequest{
		Buckets: []model.TrackGetKeys{
			{
				BucketID: 42,
				Keys:     []string{"foo:1", "foo:2"},
			},
		},
	}

	result, err := client.TrackGetMany(req)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotNil(t, result)
	bucket42 := result.Values["42"]
	assert.NotNil(t, bucket42)
	foo1 := bucket42["foo:1"]
	assert.NotNil(t, foo1)
	assert.Equal(t, foo1.Value, int64(100))
	assert.NotNil(t, foo1.Tag)
	assert.Equal(t, *foo1.Tag, int64(1))
	assert.NotNil(t, foo1.Metric)
	assert.Equal(t, *foo1.Metric, 42.5)

	assert.Equal(t, len(result.Missing), 1)
	missing42 := result.Missing["42"]
	assert.NotNil(t, missing42)
	assert.Equal(t, len(missing42), 1)
	foo2 := missing42[0]
	assert.Equal(t, foo2, "foo:2")
}
