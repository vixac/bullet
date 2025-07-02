package client

import (
	"context"
	"log"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/model"
	mongodb "github.com/vixac/bullet/store/mongo"
)

var g_mongoStore *mongodb.MongoStore
var g_container testcontainers.Container

func TestMain(m *testing.M) {
	println("Start main launched..")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mongo:7",
		ExposedPorts: []string{"27017/tcp"},

		WaitingFor: wait.ForListeningPort("27017/tcp").WithStartupTimeout(30 * time.Second),
	}

	mongoC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}
	g_container = mongoC

	uri, _ := g_container.Endpoint(ctx, "")
	mongoURI := "mongodb://" + uri
	println("Mongo url is " + mongoURI)

	mongoStore, err := mongodb.NewMongoStore(mongoURI)
	if err != nil {
		log.Fatal(err)
	}
	g_mongoStore = mongoStore // quickhandle global for all tests.

	println("Starting test suite")
	code := m.Run() //run entire test suite
	os.Exit(code)
	// Teardown
	//_ = mongoClient.Disconnect(ctx)
	println("Terminating.")
	_ = g_container.Terminate(ctx)

}

func TestInsertOneAndGetOne(t *testing.T) {

	println("Starting test...")
	engine := gin.Default()
	engine = api.SetupBucketRouter(g_mongoStore, "test-bucket", engine)
	server := httptest.NewServer(engine.Handler())
	defer server.Close()

	client := NewBulletClient(server.URL+"/test-bucket", 123)
	var tag int64 = 1
	var metric float64 = 42.5
	err := client.InsertOne(42, "foo:1", 100, &tag, &metric)
	if err != nil {
		t.Fatal(err)
	}
	//get many
	req := model.GetManyRequest{
		Buckets: []model.BucketGetKeys{
			{
				BucketID: 42,
				Keys:     []string{"foo:1", "foo:2"},
			},
		},
	}

	result, err := client.GetMany(req)
	if err != nil {
		print("VX: what caused it though...")
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
