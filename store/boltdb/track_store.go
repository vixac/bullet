package boltdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/vixac/bullet/model"
	"go.etcd.io/bbolt"
)

func bucketName(appID, bucketID int32) string {
	return fmt.Sprintf("app_%d_bucket_%d", appID, bucketID)
}

func encodeTrackValue(value int64, tag *int64, metric *float64) []byte {
	buf := &bytes.Buffer{}

	// Value
	binary.Write(buf, binary.BigEndian, uint64(value))

	// Tag
	if tag != nil {
		buf.WriteByte(1)
		binary.Write(buf, binary.BigEndian, uint64(*tag))
	} else {
		buf.WriteByte(0)
	}

	// Metric
	if metric != nil {
		buf.WriteByte(1)
		binary.Write(buf, binary.BigEndian, math.Float64bits(*metric))
	} else {
		buf.WriteByte(0)
	}

	return buf.Bytes()
}

func decodeTrackValue(b []byte) (value int64, tag *int64, metric *float64, err error) {
	buf := bytes.NewReader(b)

	var v uint64
	if err = binary.Read(buf, binary.BigEndian, &v); err != nil {
		return
	}
	value = int64(v)

	// Tag
	flag, err := buf.ReadByte()
	if err != nil {
		return
	}
	if flag == 1 {
		var tv uint64
		if err = binary.Read(buf, binary.BigEndian, &tv); err != nil {
			return
		}
		tv2 := int64(tv)
		tag = &tv2
	}

	// Metric
	flag, err = buf.ReadByte()
	if err != nil {
		return
	}
	if flag == 1 {
		var mv uint64
		if err = binary.Read(buf, binary.BigEndian, &mv); err != nil {
			return
		}
		mv2 := math.Float64frombits(mv)
		metric = &mv2
	}

	return
}

func (b *BoltStore) TrackPut(appID int32, bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists([]byte(bucketName(appID, bucketID)))
		if err != nil {
			return err
		}
		val := encodeTrackValue(value, tag, metric)
		return bkt.Put([]byte(key), val)
	})
}

// VX:Note should return int64 and nil onNotFound
func (b *BoltStore) TrackGet(appID, bucketID int32, key string) (int64, error) {
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

func (b *BoltStore) TrackDeleteMany(appID int32, items []model.TrackBucketKeyPair) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		// Group deletions by bucket to avoid repeated lookups
		buckets := make(map[int32]*bbolt.Bucket)

		for _, item := range items {
			bkt, ok := buckets[item.BucketID]
			if !ok {
				bkt = tx.Bucket([]byte(bucketName(appID, item.BucketID)))
				if bkt == nil {
					return bbolt.ErrBucketNotFound
				}
				buckets[item.BucketID] = bkt
			}

			if err := bkt.Delete([]byte(item.Key)); err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *BoltStore) TrackClose() error {
	return b.db.Close()
}

func (b *BoltStore) TrackPutMany(appID int32, items map[int32][]model.TrackKeyValueItem) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		for bucketID, arr := range items {
			bkt, err := tx.CreateBucketIfNotExists([]byte(bucketName(appID, bucketID)))
			if err != nil {
				return err
			}

			for _, it := range arr {
				val := encodeTrackValue(it.Value.Value, it.Value.Tag, it.Value.Metric)
				if err := bkt.Put([]byte(it.Key), val); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *BoltStore) TrackGetMany(appID int32, keys map[int32][]string) (
	map[int32]map[string]model.TrackValue,
	map[int32][]string,
	error,
) {

	found := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	err := b.db.View(func(tx *bbolt.Tx) error {
		for bucketID, keyList := range keys {
			bkt := tx.Bucket([]byte(bucketName(appID, bucketID)))
			if bkt == nil {
				// whole bucket missing, all keys missing
				missing[bucketID] = append(missing[bucketID], keyList...)
				continue
			}

			for _, key := range keyList {
				val := bkt.Get([]byte(key))
				if val == nil {
					missing[bucketID] = append(missing[bucketID], key)
					continue
				}

				v, tag, metric, err := decodeTrackValue(val)
				if err != nil {
					return err
				}

				if found[bucketID] == nil {
					found[bucketID] = make(map[string]model.TrackValue)
				}

				found[bucketID][key] = model.TrackValue{
					Value:  v,
					Tag:    tag,
					Metric: metric,
				}
			}
		}
		return nil
	})

	return found, missing, err
}

// /VX:Note this does not use any index. It fetches all items in the bucket and manually seeks on the parameters
func (b *BoltStore) GetItemsByKeyPrefix(
	appID, bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {

	var result []model.TrackKeyValueItem
	p := []byte(prefix)

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

	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket([]byte(bucketName(appID, bucketID)))
		if bkt == nil {
			return bbolt.ErrBucketNotFound
		}

		c := bkt.Cursor()

		for k, v := c.Seek(p); k != nil && bytes.HasPrefix(k, p); k, v = c.Next() {
			value, tag, metric, err := decodeTrackValue(v)
			if err != nil {
				return err
			}

			if !tagFilter(tag) {
				continue
			}

			if !metricFilter(metric) {
				continue
			}

			result = append(result, model.TrackKeyValueItem{
				Key: string(k),
				Value: model.TrackValue{
					Value:  value,
					Tag:    tag,
					Metric: metric,
				},
			})
		}
		return nil
	})

	return result, err
}
