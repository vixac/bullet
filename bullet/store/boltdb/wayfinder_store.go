package boltdb

import (
	"errors"

	"github.com/vixac/bullet/model"
)

func (b *BoltStore) WayFinderPut(appID int32, bucketID int32, key string, payload string, tag *int64, metric *float64) (int64, error) {
	return 0, errors.New("get many not implmemented on bolt store")
}
func (b *BoltStore) WayFinderGetByPrefix(appID int32, bucketID int32,
	prefix string,
	tags []int64, // optional slice of tags
	metricValue *float64, // optional metric value
	metricIsGt bool, // "gt" or "lt"
) ([]model.WayFinderQueryItem, error) {
	return nil, errors.New("get many not implmemented on bolt store")
}

func (s *BoltStore) WayFinderGetOne(
	appID int32,
	bucketID int32,
	key string,
) (*model.WayFinderGetResponse, error) {
	return nil, errors.New("get many not implmemented on bolt store")
}
