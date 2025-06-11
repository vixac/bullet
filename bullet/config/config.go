package config

import (
    "flag"
)

type Config struct {
    DBType     string
    MongoURI   string
    BoltPath   string
    ListenAddr string
}

func Load() *Config {
    var cfg Config

    flag.StringVar(&cfg.DBType, "db-type", "mongodb", "Database type: mongodb or boltdb")
    flag.StringVar(&cfg.MongoURI, "mongo-uri", "mongodb://localhost:27017", "MongoDB URI")
    flag.StringVar(&cfg.BoltPath, "bolt-path", "data.db", "BoltDB file path")
    flag.StringVar(&cfg.ListenAddr, "listen", ":8080", "HTTP listen address")
    flag.Parse()

    return &cfg
}
