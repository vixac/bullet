package boltdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/vixac/bullet/model"
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

func (b *BoltStore) BucketPut(appID, bucketID int32, key string, value int64) error {
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

func (b *BoltStore) BucketGet(appID, bucketID int32, key string) (int64, error) {
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

func (b *BoltStore) BucketDelete(appID, bucketID int32, key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(bucketName(appID, bucketID)))
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}
		return bkt.Delete([]byte(key))
	})
}

func (b *BoltStore) BucketClose() error {
	return b.db.Close()
}

func (b *BoltStore) BucketPutMany(appID int32, items map[int32][]model.BucketKeyValueItem) error {
	return errors.New("put many not implmemented on bolt store")
}
func (b *BoltStore) BucketGetMany(appID int32, keys map[int32][]string) (map[int32]map[string]int64, map[int32][]string, error) {

	return nil, nil, errors.New("get many not implmemented on bolt store")
}
