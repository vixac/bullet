package boltdb

import "go.etcd.io/bbolt"

type BoltStore struct {
	db *bbolt.DB
}

func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bbolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	return &BoltStore{db: db}, nil
}
