package mongodb

import (
	"context"
	"fmt"

	"github.com/vixac/bullet/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *MongoStore) BucketPut(appID, bucketID int32, key string, value int64) error {
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	update := bson.M{"$set": bson.M{"value": value}}
	_, err := m.bucketCollection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
	return err
}

func (m *MongoStore) BucketGet(appID, bucketID int32, key string) (int64, error) {
	var result struct{ Value int64 }
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	err := m.bucketCollection.FindOne(context.TODO(), filter).Decode(&result)
	return result.Value, err
}

func (m *MongoStore) BucketDelete(appID, bucketID int32, key string) error {
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	_, err := m.bucketCollection.DeleteOne(context.TODO(), filter)
	return err
}

func (m *MongoStore) BucketClose() error {
	return m.client.Disconnect(context.TODO())
}

func (m *MongoStore) BucketPutMany(appID int32, items map[int32][]model.BucketKeyValueItem) error {
	var docs []interface{}

	for bucketID, kvItems := range items {
		for _, kv := range kvItems {
			doc := bson.M{
				"appId":    appID,
				"bucketId": bucketID,
				"key":      kv.Key,
				"value":    kv.Value,
			}
			docs = append(docs, doc)
		}
	}

	if len(docs) == 0 {
		return nil
	}

	_, err := m.bucketCollection.InsertMany(context.TODO(), docs, options.InsertMany().SetOrdered(false))
	return err
}

func (m *MongoStore) BucketGetMany(appID int32, keys map[int32][]string) (map[int32]map[string]int64, map[int32][]string, error) {
	values := make(map[int32]map[string]int64)
	missing := make(map[int32][]string)

	var orFilters []bson.M
	for bucketID, keyList := range keys {
		for _, key := range keyList {
			orFilters = append(orFilters, bson.M{
				"appId":    appID,
				"bucketId": bucketID,
				"key":      key,
			})
		}
	}

	if len(orFilters) == 0 {
		return values, missing, nil
	}

	cur, err := m.bucketCollection.Find(context.TODO(), bson.M{"$or": orFilters})
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close(context.TODO())

	foundKeys := make(map[int32]map[string]bool)

	for cur.Next(context.TODO()) {
		var result struct {
			BucketID int32  `bson:"bucketId"`
			Key      string `bson:"key"`
			Value    int64  `bson:"value"`
		}
		if err := cur.Decode(&result); err != nil {
			return nil, nil, err
		}

		if _, ok := values[result.BucketID]; !ok {
			values[result.BucketID] = make(map[string]int64)
			foundKeys[result.BucketID] = make(map[string]bool)
		}

		values[result.BucketID][result.Key] = result.Value
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
func (m *MongoStore) GetItemsByKeyPrefix(appID, bucketID int32, prefix string) ([]model.BucketKeyValueItem, error) {
	lower := prefix
	upper := nextLexicographicString(prefix)

	filter := bson.M{
		"appId":    appID,
		"bucketId": bucketID,
		"key": bson.M{
			"$gte": lower,
			"$lt":  upper,
		},
	}
	fmt.Printf("VX: filter is %+v\n", filter)
	cursor, err := m.bucketCollection.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	var results []model.BucketKeyValueItem
	if err := cursor.All(context.TODO(), &results); err != nil {
		return nil, err
	}

	return results, nil
}
