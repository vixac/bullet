package api

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

var (
	clients map[string]store_interface.Store
)

func TestMain(m *testing.M) {
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

	mongoStore, err := mongodb.NewMongoStore(mongoURI)
	if err != nil {
		log.Fatal(err)
	}

	clients = make(map[string]store_interface.Store)
	clients["mongo_store"] = mongoStore

	//making bolt now.

	boltStore, err := boltdb.NewBoltStore("test-boltdb")
	if err != nil {
		log.Fatal(err)
	}

	clients["bolt_store"] = boltStore

	code := m.Run() //run entire test suite
	os.Exit(code)
	// Teardown
	//_ = mongoClient.Disconnect(ctx)
	_ = mongoC.Terminate(ctx)

}
