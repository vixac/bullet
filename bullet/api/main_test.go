package api

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vixac/bullet/store"
	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
)

var (
	clients map[string]store.Store
)

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

	uri, _ := mongoC.Endpoint(ctx, "")
	mongoURI := "mongodb://" + uri
	println("Mongo url is " + mongoURI)

	mongoStore, err := mongodb.NewMongoStore(mongoURI)
	if err != nil {
		log.Fatal(err)
	}

	clients = make(map[string]store.Store)
	clients["mongo_store"] = mongoStore

	//making bolt now.

	boltStore, err := boltdb.NewBoltStore("test-boltdb")
	if err != nil {
		log.Fatal(err)
	}

	clients["bolt_store"] = boltStore
	println("Starting test suite")

	code := m.Run() //run entire test suite
	os.Exit(code)
	// Teardown
	//_ = mongoClient.Disconnect(ctx)
	println("Terminating.")
	_ = mongoC.Terminate(ctx)

}
