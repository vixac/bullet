package boltdb

import (
	"encoding/binary"
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
	"go.etcd.io/bbolt"
)

func oldBucketName(appId int32) []byte {
	return []byte(fmt.Sprintf("pigeon:app:%d", appId))
}

// VX:TODO use the newdepot after migration.
func newDepotBucket(appID int32, tenantId int64) []byte {
	return []byte(fmt.Sprintf("depot:v2:%d:tenant:%d", appID, tenantId))
}

func getBucketName(space store_interface.TenancySpace) []byte {
	return newDepotBucket(space.AppId, space.TenancyId)
}

func (b *BoltStore) DepotPut(space store_interface.TenancySpace, key int64, value string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucketName := getBucketName(space)
		bkt, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bkt.Put(encodeInt64(key), []byte(value))
	})
}

func (b *BoltStore) DepotGet(space store_interface.TenancySpace, key int64) (string, error) {
	var val []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		bucketName := getBucketName(space)
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			return fmt.Errorf("not found")
		}
		val = bkt.Get(encodeInt64(key))
		if val == nil {
			return fmt.Errorf("not found")
		}
		return nil
	})
	return string(val), err
}

func (b *BoltStore) DepotDelete(space store_interface.TenancySpace, key int64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucketName := getBucketName(space)
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			return nil
		}
		return bkt.Delete(encodeInt64(key))
	})
}

func (b *BoltStore) DepotPutMany(space store_interface.TenancySpace, items []model.DepotKeyValueItem) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucketName := getBucketName(space)
		bkt, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := bkt.Put(encodeInt64(item.Key), []byte(item.Value)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltStore) DepotGetMany(space store_interface.TenancySpace, keys []int64) (map[int64]string, []int64, error) {
	results := make(map[int64]string)
	var missing []int64

	err := b.db.View(func(tx *bbolt.Tx) error {
		bucketName := getBucketName(space)
		bkt := tx.Bucket(bucketName)
		if bkt == nil {
			missing = keys
			return nil
		}
		for _, k := range keys {
			val := bkt.Get(encodeInt64(k))
			if val == nil {
				missing = append(missing, k)
			} else {
				results[k] = string(val)
			}
		}
		return nil
	})

	return results, missing, err
}

func encodeInt64(i int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
