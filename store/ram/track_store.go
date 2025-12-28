package ram

import (
	"errors"
	"fmt"
	"strings"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (r *RamStore) TrackDeleteMany(space store_interface.TenancySpace, items []model.TrackBucketKeyPair) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	appBuckets, ok := r.tracks[space.AppId]
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
func (r *RamStore) TrackPut(space store_interface.TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tracks[space.AppId] == nil {
		r.tracks[space.AppId] = make(map[int32]map[string]model.TrackValue)
	}
	if r.tracks[space.AppId][bucketID] == nil {
		r.tracks[space.AppId][bucketID] = make(map[string]model.TrackValue)
	}

	r.tracks[space.AppId][bucketID][key] = model.TrackValue{
		Value:  value,
		Tag:    tag,
		Metric: metric,
	}
	return nil
}

func (r *RamStore) TrackGet(space store_interface.TenancySpace, bucketID int32, key string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	bucket, ok := r.tracks[space.AppId][bucketID]
	if !ok {
		return 0, errors.New("bucket not found in ram Store.")
	}
	val, ok := bucket[key]
	if !ok {
		return 0, errors.New("key not found")
	}
	return val.Value, nil
}

func (r *RamStore) TrackDelete(space store_interface.TenancySpace, bucketID int32, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if bucket, ok := r.tracks[space.AppId][bucketID]; ok {
		delete(bucket, key)
	}
	return nil
}

func (r *RamStore) TrackClose() error {
	return nil // nothing to close in memory
}

func (r *RamStore) TrackPutMany(space store_interface.TenancySpace, items map[int32][]model.TrackKeyValueItem) error {
	for bucketID, kvList := range items {
		for _, kv := range kvList {
			if err := r.TrackPut(space, bucketID, kv.Key, kv.Value.Value, kv.Value.Tag, kv.Value.Metric); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RamStore) TrackGetMany(space store_interface.TenancySpace, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	found := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	for bucketID, keyList := range keys {
		if r.tracks[space.AppId] == nil {
			missing[bucketID] = keyList
			continue
		}
		bucket := r.tracks[space.AppId][bucketID]
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

func (b *RamStore) GetItemsByKeyPrefix(
	space store_interface.TenancySpace,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {
	return b.GetItemsByKeyPrefixes(space, bucketID, []string{prefix}, tags, metricValue, metricIsGt)
}
func (r *RamStore) GetItemsByKeyPrefixes(
	space store_interface.TenancySpace,
	bucketID int32,
	prefixes []string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []model.TrackKeyValueItem
	bucket := r.tracks[space.AppId][bucketID]
	if bucket == nil {
		return result, nil
	}

	if len(prefixes) == 0 {
		return result, fmt.Errorf("must provide at least one prefix")
	}

	// Convert prefixes to a more efficient lookup structure
	// (highly cache-friendly when scanning map keys)
	prefixList := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		if p != "" {
			prefixList = append(prefixList, p)
		}
	}
	if len(prefixList) == 0 {
		return result, fmt.Errorf("all prefixes were empty")
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

	matchesPrefix := func(k string) bool {
		for _, p := range prefixList {
			if strings.HasPrefix(k, p) {
				return true
			}
		}
		return false
	}

	for k, v := range bucket {
		if matchesPrefix(k) && tagFilter(v.Tag) && metricFilter(v.Metric) {
			result = append(result, model.TrackKeyValueItem{
				Key:   k,
				Value: v,
			})
		}
	}

	return result, nil
}
