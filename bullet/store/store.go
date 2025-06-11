package store

import "github.com/vixac/bullet/model"

type BucketStore interface {
	Put(appID, bucketID int32, key string, value int64) error
	Get(appID, bucketID int32, key string) (int64, error)
	Delete(appID, bucketID int32, key string) error
	Close() error
	PutMany(appID int32, items map[int32][]model.KeyValueItem) error
	GetMany(appID int32, keys map[int32][]string) (map[int32]map[string]int64, map[int32][]string, error)
}
