package main

import (
	"fmt"
	"log"

	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/config"
	"github.com/vixac/bullet/store"
	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
)

func main() {
	cfg := config.Load()
	fmt.Printf("VX: config is %+v\n", cfg)
	var kvStore store.BucketStore
	var err error

	switch cfg.DBType {
	case config.Mongo:
		kvStore, err = mongodb.NewMongoStore(cfg.MongoURI)
	case config.Boltdb:
		kvStore, err = boltdb.NewBoltStore(cfg.BoltPath)
	default:
		log.Fatal("unsupported store type")
	}

	if err != nil {
		log.Fatal(err)
	}
	defer kvStore.BucketClose()

	router := api.SetupRouter(kvStore)
	log.Fatal(router.Run(":" + cfg.Port))
}
