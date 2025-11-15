package config

import (
	"flag"
	"log"
)

type Config struct {
	DBType   string
	MongoURI string
	BoltPath string
	Port     string
}

const (
	Mongo  = "mongodb"
	Boltdb = "boltdb"
)

func Load() *Config {
	var cfg Config

	port := flag.String("port", "", "port number for bullet HTTP")
	mongoStr := flag.String("mongo", "", "mongodb endpoint") //mongodb://localhost:27017
	boltStr := flag.String("bolt", "", "BoltDB file path")
	dbType := flag.String("db-type", "", "mongo or boldtb mode")
	flag.Parse()
	if *port == "" {
		log.Fatal("missing port number")
	}
	cfg.Port = *port

	if *dbType != Mongo && *dbType != Boltdb {
		log.Fatal("invalid db-type:" + *dbType + ". needs to be either " + Mongo + " or " + Boltdb)
	}
	if *dbType == Mongo && *mongoStr == "" {
		log.Fatal("you asked for mongo db type but didnt provide a mongodb con string")

	}
	if *dbType == Boltdb && *boltStr == "" {
		log.Fatal("you asked for boltdb but didnt provide a bolt path")
	}

	cfg.DBType = *dbType
	cfg.MongoURI = *mongoStr
	cfg.BoltPath = *boltStr
	return &cfg
}
