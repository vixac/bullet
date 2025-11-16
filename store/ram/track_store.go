package ram

import (
	"errors"
	"strings"

	"github.com/vixac/bullet/model"
)

func (r *RamStore) TrackDeleteMany(appID int32, items []model.TrackBucketKeyPair) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	appBuckets, ok := r.tracks[appID]
	if !ok {
		// No buckets at all — treat all deletes as no-ops, just like single delete
		return nil
	}

	for _, item := range items {
		bucket, ok := appBuckets[item.BucketID]
		if !ok {
			// Bucket missing — consistent with TrackDelete (silent no-op)
			continue
		}
		delete(bucket, item.Key)
	}

	return nil
}
func (r *RamStore) TrackPut(appID int32, bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tracks[appID] == nil {
		r.tracks[appID] = make(map[int32]map[string]model.TrackValue)
	}
	if r.tracks[appID][bucketID] == nil {
		r.tracks[appID][bucketID] = make(map[string]model.TrackValue)
	}

	r.tracks[appID][bucketID][key] = model.TrackValue{
		Value:  value,
		Tag:    tag,
		Metric: metric,
	}
	return nil
}

func (r *RamStore) TrackGet(appID int32, bucketID int32, key string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bucket, ok := r.tracks[appID][bucketID]
	if !ok {
		return 0, errors.New("bucket not found")
	}
	val, ok := bucket[key]
	if !ok {
		return 0, errors.New("key not found")
	}
	return val.Value, nil
}

func (r *RamStore) TrackDelete(appID int32, bucketID int32, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bucket, ok := r.tracks[appID][bucketID]; ok {
		delete(bucket, key)
	}
	return nil
}

func (r *RamStore) TrackClose() error {
	return nil // nothing to close in memory
}

func (r *RamStore) TrackPutMany(appID int32, items map[int32][]model.TrackKeyValueItem) error {
	for bucketID, kvList := range items {
		for _, kv := range kvList {
			if err := r.TrackPut(appID, bucketID, kv.Key, kv.Value.Value, kv.Value.Tag, kv.Value.Metric); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RamStore) TrackGetMany(appID int32, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	found := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	for bucketID, keyList := range keys {
		if r.tracks[appID] == nil {
			missing[bucketID] = keyList
			continue
		}
		bucket := r.tracks[appID][bucketID]
		if bucket == nil {
			missing[bucketID] = keyList
			continue
		}
		for _, k := range keyList {
			if val, ok := bucket[k]; ok {
				if found[bucketID] == nil {
					found[bucketID] = make(map[string]model.TrackValue)
				}
				found[bucketID][k] = val
			} else {
				missing[bucketID] = append(missing[bucketID], k)
			}
		}
	}

	return found, missing, nil
}

func (r *RamStore) GetItemsByKeyPrefix(appID, bucketID int32, prefix string, tags []int64, metricValue *float64, metricIsGt bool) ([]model.TrackKeyValueItem, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []model.TrackKeyValueItem
	bucket := r.tracks[appID][bucketID]
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
			result = append(result, model.TrackKeyValueItem{
				Key:   k,
				Value: v,
			})
		}
	}

	return result, nil
}
