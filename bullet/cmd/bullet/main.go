package main

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/config"
	"github.com/vixac/bullet/store/boltdb"
	mongodb "github.com/vixac/bullet/store/mongo"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

func main() {
	cfg := config.Load()
	var kvStore store_interface.Store
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

	println("Creating gin routers.. on port: ", cfg.Port)
	engine := gin.Default()
	engine = api.SetupTrackRouter(kvStore, "track/", engine)
	engine = api.SetupDepotRouter(kvStore, "depot/", engine)
	engine = api.SetupWayFinderRouter(kvStore, "wayfinder/", engine)
	fmt.Println("Bullet is Healthy, on port " + cfg.Port)
	log.Fatal(engine.Run(":" + cfg.Port))
}
