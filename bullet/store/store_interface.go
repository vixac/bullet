package store

import "github.com/vixac/bullet/model"

type TrackStore interface {
	TrackPut(appID int32, bucketID int32, key string, value int64, tag *int64, metric *float64) error
	TrackGet(appID int32, bucketID int32, key string) (int64, error)
	TrackDelete(appID int32, bucketID int32, key string) error
	TrackClose() error
	TrackPutMany(appID int32, items map[int32][]model.TrackKeyValueItem) error
	TrackGetMany(appID int32, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error)
	GetItemsByKeyPrefix(
		appID, bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.TrackKeyValueItem, error)
}
type PigeonStore interface {
	PigeonPut(appID int32, key int64, value string) error
	PigeonGet(appID int32, key int64) (string, error)
	PigeonDelete(appID int32, key int64) error
	PigeonPutMany(appID int32, items []model.PigeonKeyValueItem) error
	PigeonGetMany(appID int32, keys []int64) (map[int64]string, []int64, error)
}

type Store interface {
	TrackStore
	PigeonStore
}
