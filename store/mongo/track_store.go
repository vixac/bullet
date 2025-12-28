package mongodb

import (
	"context"
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *MongoStore) TrackDeleteMany(space store_interface.TenancySpace, items []model.TrackBucketKeyPair) error {
	if len(items) == 0 {
		return nil
	}

	// Build a massive OR filter in a single DeleteMany
	// (MongoDB handles this efficiently using index intersections).
	orFilters := make([]bson.M, 0, len(items))

	for _, item := range items {
		orFilters = append(orFilters, bson.M{
			"appId":     space.AppId,
			"tenancyId": space.TenancyId,
			"bucketId":  item.BucketID,
			"key":       item.Key,
		})
	}

	filter := bson.M{
		"$or": orFilters,
	}

	_, err := m.trackCollection.DeleteMany(context.TODO(), filter)
	return err
}

func (m *MongoStore) TrackPut(space store_interface.TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	filter := bson.M{
		"appId":     space.AppId,
		"tenancyId": space.TenancyId,
		"bucketId":  bucketID,
		"key":       key,
	}

	// Build the update document
	updateFields := bson.M{
		"value": value,
	}

	if tag != nil {
		updateFields["tag"] = *tag
	}

	if metric != nil {
		updateFields["metric"] = *metric
	}

	update := bson.M{
		"$set": updateFields,
	}

	_, err := m.trackCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
	return err
}

func (m *MongoStore) TrackGet(space store_interface.TenancySpace, bucketID int32, key string) (int64, error) {
	var result struct{ Value int64 }
	filter := bson.M{"appId": space.AppId, "tenancyId": space.TenancyId, "bucketId": bucketID, "key": key}
	err := m.trackCollection.FindOne(context.TODO(), filter).Decode(&result)
	return result.Value, err
}

func (m *MongoStore) TrackDelete(space store_interface.TenancySpace, bucketID int32, key string) error {
	filter := bson.M{"appId": space.AppId, "tenancyId": space.TenancyId, "bucketId": bucketID, "key": key}
	_, err := m.trackCollection.DeleteOne(context.TODO(), filter)
	return err
}

func (m *MongoStore) TrackClose() error {
	return m.client.Disconnect(context.TODO())
}

func (m *MongoStore) TrackPutMany(space store_interface.TenancySpace, items map[int32][]model.TrackKeyValueItem) error {
	var docs []interface{}

	for bucketID, kvItems := range items {
		for _, kv := range kvItems {
			doc := bson.M{
				"appId":     space.AppId,
				"tenancyId": space.TenancyId,
				"bucketId":  bucketID,
				"key":       kv.Key,
				"value":     kv.Value,
			}
			docs = append(docs, doc)
		}
	}

	if len(docs) == 0 {
		return nil
	}

	_, err := m.trackCollection.InsertMany(context.TODO(), docs, options.InsertMany().SetOrdered(false))
	return err
}

func (b *MongoStore) GetItemsByKeyPrefix(
	space store_interface.TenancySpace, bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.TrackKeyValueItem, error) {
	return b.GetItemsByKeyPrefixes(space, bucketID, []string{prefix}, tags, metricValue, metricIsGt)
}

func (m *MongoStore) TrackGetMany(space store_interface.TenancySpace, keys map[int32][]string) (map[int32]map[string]model.TrackValue, map[int32][]string, error) {
	values := make(map[int32]map[string]model.TrackValue)
	missing := make(map[int32][]string)

	var orFilters []bson.M
	for bucketID, keyList := range keys {
		for _, key := range keyList {
			orFilters = append(orFilters, bson.M{
				"appId":     space.AppId,
				"tenancyId": space.TenancyId,
				"bucketId":  bucketID,
				"key":       key,
			})
		}
	}

	if len(orFilters) == 0 {
		return values, missing, nil
	}

	cur, err := m.trackCollection.Find(context.TODO(), bson.M{"$or": orFilters})
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close(context.TODO())

	foundKeys := make(map[int32]map[string]bool)

	for cur.Next(context.TODO()) {
		var result struct {
			BucketID int32    `bson:"bucketId"`
			Key      string   `bson:"key"`
			Value    int64    `bson:"value"`
			Tag      *int64   `bson:"tag,omitempty"`
			Metric   *float64 `bson:"metric,omitempty"`
		}
		if err := cur.Decode(&result); err != nil {
			return nil, nil, err
		}

		if _, ok := values[result.BucketID]; !ok {
			values[result.BucketID] = make(map[string]model.TrackValue)
			foundKeys[result.BucketID] = make(map[string]bool)
		}

		values[result.BucketID][result.Key] = model.TrackValue{
			Value:  result.Value,
			Tag:    result.Tag,
			Metric: result.Metric,
		}
		foundKeys[result.BucketID][result.Key] = true
	}

	// identify missing keys
	for bucketID, keyList := range keys {
		for _, key := range keyList {
			if _, ok := foundKeys[bucketID]; !ok {
				missing[bucketID] = append(missing[bucketID], key)
			} else if !foundKeys[bucketID][key] {
				missing[bucketID] = append(missing[bucketID], key)
			}
		}
	}

	return values, missing, nil
}

func nextLexicographicString(s string) string {
	if len(s) == 0 {
		return ""
	}

	// Convert string to byte slice
	b := []byte(s)

	// Walk backwards, looking for a byte we can increment
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}

	// If all bytes were 0xFF, append 0x00 (or pick a safe suffix char)
	return s + "\x00"
}
func (m *MongoStore) GetItemsByKeyPrefixes(
	space store_interface.TenancySpace, bucketID int32,
	prefixes []string, // multiple prefixes allowed
	tags []int64, // optional
	metricValue *float64, // optional
	metricIsGt bool, // if metricValue != nil
) ([]model.TrackKeyValueItem, error) {

	if len(prefixes) == 0 {
		return nil, fmt.Errorf("must provide at least one prefix")
	}

	// Base filter for app and bucket
	filter := bson.M{
		"appId":     space.AppId,
		"tenancyId": space.TenancyId,
		"bucketId":  bucketID,
	}

	// Build the OR clause for prefix ranges
	orClauses := make([]bson.M, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix == "" {
			continue // ignore empty prefix entries
		}
		lower := prefix
		upper := nextLexicographicString(prefix)

		orClauses = append(orClauses, bson.M{
			"key": bson.M{
				"$gte": lower,
				"$lt":  upper,
			},
		})
	}

	if len(orClauses) == 0 {
		return nil, fmt.Errorf("all prefixes were empty")
	}

	// Attach OR conditions
	filter["$or"] = orClauses

	// Attach tags filter if provided
	if len(tags) > 0 {
		filter["tag"] = bson.M{"$in": tags}
	}

	// Attach metric filter if provided
	if metricValue != nil {
		op := "$lt"
		if metricIsGt {
			op = "$gt"
		}
		filter["metric"] = bson.M{op: *metricValue}
	}

	cursor, err := m.trackCollection.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	var results []model.TrackKeyValueItem
	if err := cursor.All(context.TODO(), &results); err != nil {
		return nil, err
	}

	return results, nil
}
