package boltdb

import (
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

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

func bucketName(appID, bucketID int32) string {
	return fmt.Sprintf("app_%d_bucket_%d", appID, bucketID)
}

func (b *BoltStore) Put(appID, bucketID int32, key string, value int64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(bucketName(appID, bucketID)))
		if err != nil {
			return err
		}
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, uint64(value))
		return bkt.Put([]byte(key), val)
	})
}

func (b *BoltStore) Get(appID, bucketID int32, key string) (int64, error) {
	var value int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(bucketName(appID, bucketID)))
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}
		val := bkt.Get([]byte(key))
		if val == nil {
			return fmt.Errorf("key not found")
		}
		value = int64(binary.BigEndian.Uint64(val))
		return nil
	})
	return value, err
}

func (b *BoltStore) Delete(appID, bucketID int32, key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(bucketName(appID, bucketID)))
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}
		return bkt.Delete([]byte(key))
	})
}

func (b *BoltStore) Close() error {
	return b.db.Close()
}
