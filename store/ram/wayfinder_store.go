package ram

import (
	"errors"
	"strings"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (r *RamStore) WayFinderPut(space store_interface.TenancySpace, bucketID int32, key string, payload string, tag *int64, metric *float64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.wayfind[space] == nil {
		r.wayfind[space] = make(map[int32]map[string]model.WayFinderQueryItem)
	}
	if r.wayfind[space][bucketID] == nil {
		r.wayfind[space][bucketID] = make(map[string]model.WayFinderQueryItem)
	}

	itemID := int64(len(r.wayfind[space][bucketID]) + 1)
	r.wayfind[space][bucketID][key] = model.WayFinderQueryItem{
		Key:     key,
		ItemId:  itemID,
		Payload: payload,
		Tag:     tag,
		Metric:  metric,
	}

	return itemID, nil
}

func (r *RamStore) WayFinderGetByPrefix(space store_interface.TenancySpace, bucketID int32, prefix string, tags []int64, metricValue *float64, metricIsGt bool) ([]model.WayFinderQueryItem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []model.WayFinderQueryItem

	bucket := r.wayfind[space][bucketID]
	if bucket == nil {
		return result, nil
	}

	tagFilter := func(tag *int64) bool {
		if len(tags) == 0 {
			return true
		}
		if tag == nil {
			return false
		}
		for _, t := range tags {
			if *tag == t {
				return true
			}
		}
		return false
	}

	metricFilter := func(metric *float64) bool {
		if metricValue == nil {
			return true
		}
		if metric == nil {
			return false
		}
		if metricIsGt {
			return *metric > *metricValue
		}
		return *metric < *metricValue
	}

	for k, v := range bucket {
		if strings.HasPrefix(k, prefix) && tagFilter(v.Tag) && metricFilter(v.Metric) {
			result = append(result, v)
		}
	}

	return result, nil
}

func (r *RamStore) WayFinderGetOne(space store_interface.TenancySpace, bucketID int32, key string) (*model.WayFinderGetResponse, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bucket := r.wayfind[space][bucketID]
	if bucket == nil {
		return nil, errors.New("bucket not found in wayfinder get one ramstore")
	}

	item, ok := bucket[key]
	if !ok {
		return nil, nil
	}

	return &model.WayFinderGetResponse{
		ItemId:  item.ItemId,
		Payload: item.Payload,
		Tag:     item.Tag,
		Metric:  item.Metric,
	}, nil
}
