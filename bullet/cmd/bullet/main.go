package main

import (
	"bullet/api"
	"bullet/config"
	"bullet/store"
	"bullet/store/boltdb"
	"bullet/store/mongodb"
	"log"
)

func main() {
	cfg := config.Load()

	var kvStore store.Store
	var err error

	switch cfg.DBType {
	case "mongodb":
		kvStore, err = mongodb.NewMongoStore(cfg.MongoURI)
	case "boltdb":
		kvStore, err = boltdb.NewBoltStore(cfg.BoltPath)
	default:
		log.Fatal("unsupported store type")
	}

	if err != nil {
		log.Fatal(err)
	}
	defer kvStore.Close()

	router := api.SetupRouter(kvStore)
	log.Fatal(router.Run(cfg.ListenAddr))
}
