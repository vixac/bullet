package boltdb

import (
	"fmt"

	"github.com/vixac/bullet/model"
	"go.etcd.io/bbolt"
)

func (b *BoltStore) WayFinderPut(
	appID int32,
	bucketID int32,
	key string,
	payload string,
	tag *int64,
	metric *float64,
) (int64, error) {

	var itemId int64

	err := b.db.Update(func(tx *bbolt.Tx) error {
		// 1. Use TrackPut to store (value=itemId, tag, metric)
		// Generate an itemId: simplest is using UnixNano or an increment key.
		// But Bolt does support a sequence per bucket.

		trackBucket, err := tx.CreateBucketIfNotExists([]byte(bucketName(appID, bucketID)))
		if err != nil {
			return err
		}

		// bolt built-in sequence ID generator (uint64)
		seq, err := trackBucket.NextSequence()
		if err != nil {
			return err
		}
		itemId = int64(seq)

		// encode TrackValue: value=itemId
		encoded := encodeTrackValue(itemId, tag, metric)

		if err := trackBucket.Put([]byte(key), encoded); err != nil {
			return err
		}

		// 2. Store payload in Depot under key=itemId
		depotBucketName := []byte(fmt.Sprintf("pigeon:app:%d", appID))
		depot, err := tx.CreateBucketIfNotExists(depotBucketName)
		if err != nil {
			return err
		}

		return depot.Put(encodeInt64(itemId), []byte(payload))
	})

	if err != nil {
		return 0, err
	}

	return itemId, nil
}

func (b *BoltStore) WayFinderGetByPrefix(
	appID int32,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.WayFinderQueryItem, error) {

	var results []model.WayFinderQueryItem

	trackItems, err := b.GetItemsByKeyPrefix(
		appID,
		bucketID,
		prefix,
		tags,
		metricValue,
		metricIsGt,
	)
	if err != nil {
		return nil, err
	}

	err = b.db.View(func(tx *bbolt.Tx) error {
		depot := tx.Bucket([]byte(fmt.Sprintf("pigeon:app:%d", appID)))
		if depot == nil {
			// Track existed but Depot missing → corruption
			return fmt.Errorf("depot bucket missing")
		}

		for _, t := range trackItems {
			itemId := t.Value.Value

			raw := depot.Get(encodeInt64(itemId))
			if raw == nil {
				// payload missing → still return but skip corrupted items
				continue
			}

			results = append(results, model.WayFinderQueryItem{
				Key:     t.Key,
				ItemId:  itemId,
				Tag:     t.Value.Tag,
				Metric:  t.Value.Metric,
				Payload: string(raw),
			})
		}

		return nil
	})

	return results, err
}

func (b *BoltStore) WayFinderGetOne(
	appID int32,
	bucketID int32,
	key string,
) (*model.WayFinderGetResponse, error) {

	var resp *model.WayFinderGetResponse

	var notFound = false
	err := b.db.View(func(tx *bbolt.Tx) error {
		track := tx.Bucket([]byte(bucketName(appID, bucketID)))
		if track == nil {
			return fmt.Errorf("track bucket not found")
		}

		val := track.Get([]byte(key))
		if val == nil {
			notFound = true
			return fmt.Errorf("key not found")
		}

		itemId, tag, metric, err := decodeTrackValue(val)
		if err != nil {
			return err
		}

		depot := tx.Bucket([]byte(fmt.Sprintf("pigeon:app:%d", appID)))
		if depot == nil {
			return fmt.Errorf("depot bucket not found")
		}

		payload := depot.Get(encodeInt64(itemId))
		if payload == nil {
			return fmt.Errorf("depot entry not found for itemId %d", itemId)
		}

		resp = &model.WayFinderGetResponse{
			ItemId:  itemId,
			Payload: string(payload),
			Tag:     tag,
			Metric:  metric,
		}

		return nil
	})
	//here we handle the case that we didnt find the result, but its not an error to have a missing field.
	if resp == nil && notFound == true {
		return nil, nil
	}
	return resp, err
}
