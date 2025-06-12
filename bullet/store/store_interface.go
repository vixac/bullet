package store

import "github.com/vixac/bullet/model"

type BucketStore interface {
	BucketPut(appID int32, bucketID int32, key string, value int64) error
	BucketGet(appID int32, bucketID int32, key string) (int64, error)
	BucketDelete(appID int32, bucketID int32, key string) error
	BucketClose() error //VX:TODO whats this
	BucketPutMany(appID int32, items map[int32][]model.BucketKeyValueItem) error
	BucketGetMany(appID int32, keys map[int32][]string) (map[int32]map[string]int64, map[int32][]string, error)

	GetItemsByKeyPrefix(appID, bucketID int32, prefix string) ([]model.BucketKeyValueItem, error)
}

type PigeonStore interface {
	PigeonPut(appID int32, key int64, value string) error
	PigeonGet(appID int32, key int64) (string, error)
	PigeonDelete(appID int32, key int64) error
	PigeonPutMany(appID int32, items []model.PigeonKeyValueItem) error
	PigeonGetMany(appID int32, keys []int64) (map[int64]string, []int64, error)
}

type Store interface {
	BucketStore
	PigeonStore
}
