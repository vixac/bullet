package main

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/config"
	"github.com/vixac/bullet/store"
	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
)

func main() {
	cfg := config.Load()
	fmt.Printf("VX: config is %+v\n", cfg)
	var kvStore store.Store
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
	defer kvStore.TrackClose()

	println("Creating gin routers..?")
	engine := gin.Default()
	engine = api.SetupTrackRouter(kvStore, "bucket/", engine)
	engine = api.SetupPigeonRouter(kvStore, "pigeon/", engine)
	log.Fatal(engine.Run(":" + cfg.Port))
}
