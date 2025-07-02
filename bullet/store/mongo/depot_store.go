package mongodb

import (
	"context"
	"fmt"

	"github.com/vixac/bullet/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *MongoStore) DepotPut(appID int32, key int64, value string) error {
	filter := bson.M{"appId": appID, "key": key}
	update := bson.M{"$set": bson.M{"value": value}}
	opts := options.Update().SetUpsert(true)

	_, err := m.pigeonCollection.UpdateOne(context.TODO(), filter, update, opts)
	return err
}

func (m *MongoStore) DepotGet(appID int32, key int64) (string, error) {
	filter := bson.M{"appId": appID, "key": key}

	var result struct {
		Value string `bson:"value"`
	}

	err := m.pigeonCollection.FindOne(context.TODO(), filter).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return "", fmt.Errorf("not found")
	}
	return result.Value, err
}

func (m *MongoStore) DepotDelete(appID int32, key int64) error {
	filter := bson.M{"appId": appID, "key": key}
	_, err := m.pigeonCollection.DeleteOne(context.TODO(), filter)
	return err
}

func (m *MongoStore) DepotPutMany(appID int32, items []model.DepotKeyValueItem) error {
	var ops []mongo.WriteModel

	for _, item := range items {
		filter := bson.M{"appId": appID, "key": item.Key}
		update := bson.M{"$set": bson.M{"value": item.Value}}
		ops = append(ops, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	if len(ops) == 0 {
		return nil
	}

	_, err := m.pigeonCollection.BulkWrite(context.TODO(), ops, options.BulkWrite().SetOrdered(false))
	return err
}

func (m *MongoStore) DepotGetMany(appID int32, keys []int64) (map[int64]string, []int64, error) {
	filter := bson.M{
		"appId": appID,
		"key":   bson.M{"$in": keys},
	}

	cur, err := m.pigeonCollection.Find(context.TODO(), filter)
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close(context.TODO())

	results := make(map[int64]string)
	foundKeys := make(map[int64]bool)

	for cur.Next(context.TODO()) {
		var doc struct {
			Key   int64  `bson:"key"`
			Value string `bson:"value"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, nil, err
		}
		results[doc.Key] = doc.Value
		foundKeys[doc.Key] = true
	}

	var missing []int64
	for _, k := range keys {
		if !foundKeys[k] {
			missing = append(missing, k)
		}
	}

	return results, missing, nil
}
