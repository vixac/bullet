package client

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/model"
)

func TestDepot(t *testing.T) {
	println("Starting Depot test...")
	engine := gin.Default()
	engine = api.SetupDepotRouter(g_mongoStore, "test-depot", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	client := NewBulletClient(server.URL+"/test-depot", 123)

	err := client.DepotInsertOne(model.DepotRequest{Key: 123, Value: "this-is-a-payload"})
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"123", "234"}

	result, err := client.DepotGetMany(model.DepotGetManyRequest{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}

	assert.NotNil(t, result)
	assert.NotNil(t, result.Values)
	assert.Equal(t, len(result.Values), 1)
	found := result.Values[123]
	assert.NotNil(t, found)
	assert.Equal(t, "this-is-a-payload", found)
	assert.Equal(t, len(result.Missing), 1)
	missing := result.Missing[0]
	assert.Equal(t, missing, int64(234))

}
