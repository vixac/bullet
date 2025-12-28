package store_interface

import (
	"github.com/vixac/bullet/model"
)

type TenancySpace struct {
	AppId     int32
	TenancyId int64
}
type TrackStore interface {
	TrackPut(space TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error
	TrackGet(space TenancySpace, bucketID int32, key string) (int64, error)

	TrackDeleteMany(space TenancySpace, items []model.TrackBucketKeyPair) error
	TrackClose() error
	TrackPutMany(space TenancySpace, items map[int32][]model.TrackKeyValueItem) error
	TrackGetMany(space TenancySpace, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error)
	GetItemsByKeyPrefix(
		space TenancySpace,
		bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.TrackKeyValueItem, error)

	//Slower. Advisable to keep the number of prefix strings < 30 as it is implemented via  $or clause
	GetItemsByKeyPrefixes(space TenancySpace,
		bucketID int32,
		prefixes []string,
		tags []int64,
		metricValue *float64,
		metricIsGt bool,
	) ([]model.TrackKeyValueItem, error)
}

type DepotStore interface {
	DepotPut(space TenancySpace, key int64, value string) error
	DepotGet(space TenancySpace, key int64) (string, error)
	DepotDelete(space TenancySpace, key int64) error
	DepotPutMany(space TenancySpace, items []model.DepotKeyValueItem) error
	DepotGetMany(space TenancySpace, keys []int64) (map[int64]string, []int64, error)
}

// using its own ids, wayfinder uses track and depot to provide a query to payload interface.
type WayFinderStore interface {
	WayFinderPut(space TenancySpace, bucketID int32, key string, payload string, tag *int64, metric *float64) (int64, error)
	WayFinderGetByPrefix(space TenancySpace, bucketID int32,
		prefix string,
		tags []int64, // optional slice of tags
		metricValue *float64, // optional metric value
		metricIsGt bool, // "gt" or "lt"
	) ([]model.WayFinderQueryItem, error)

	WayFinderGetOne(space TenancySpace, bucketID int32, key string) (*model.WayFinderGetResponse, error)
}

type Store interface {
	TrackStore
	DepotStore
	WayFinderStore
}
