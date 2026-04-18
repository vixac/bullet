package config

import (
	"flag"
	"fmt"
	"log"
)

type Config struct {
	DBType      string
	MongoURI    string
	BoltPath    string
	SqlPath     string
	PostgresDSN string
	Port        string
}

const (
	Mongo      = "mongodb"
	Boltdb     = "boltdb"
	Sqlite     = "sqlite"
	Postgresql = "postgresql"
)

func Load() *Config {
	var cfg Config

	port := flag.String("port", "", "port number for bullet HTTP")
	mongoStr := flag.String("mongo", "", "mongodb endpoint") //mongodb://localhost:27017
	boltStr := flag.String("bolt", "", "BoltDB file path")
	sqlStr := flag.String("sqlite", "", "Sqlite file path")
	postgresStr := flag.String("postgres", "", "PostgreSQL DSN (postgres://user:pass@host/db)")
	dbType := flag.String("db-type", "", "mongo or boldtb mode")
	fmt.Printf("VX: Bullet fields are port: %s\n, mongo %s\n, bolt %s\n, sql %s\n, postgres %s\n, dbType %s\n", *port, *mongoStr, *boltStr, *sqlStr, *postgresStr, *dbType)
	flag.Parse()
	if *port == "" {
		log.Fatal("missing port number")
	}
	cfg.Port = *port

	if *dbType != Mongo && *dbType != Boltdb && *dbType != Sqlite && *dbType != Postgresql {
		log.Fatal("invalid db-type:" + *dbType + ". needs to be either " + Mongo + " or " + Boltdb + " or " + Sqlite + " or " + Postgresql)
	}
	if *dbType == Mongo && *mongoStr == "" {
		log.Fatal("you asked for mongo db type but didnt provide a mongodb con string")

	}
	if *dbType == Boltdb && *boltStr == "" {
		log.Fatal("you asked for boltdb but didnt provide a bolt path")
	}
	if *dbType == Sqlite && *sqlStr == "" {
		log.Fatal("you asked for sqlite but didnt provide a bolt path")
	}
	if *dbType == Postgresql && *postgresStr == "" {
		log.Fatal("you asked for postgresql but didnt provide a DSN")
	}

	cfg.DBType = *dbType
	cfg.MongoURI = *mongoStr
	cfg.BoltPath = *boltStr
	cfg.SqlPath = *sqlStr
	cfg.PostgresDSN = *postgresStr
	return &cfg
}
