package client

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
