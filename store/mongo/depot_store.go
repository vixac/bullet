package mongodb

import (
	"context"
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *MongoStore) DepotPut(space store_interface.TenancySpace, key int64, value string) error {
	filter := bson.M{"appId": space.AppId, "key": key}
	update := bson.M{"$set": bson.M{"value": value}}
	opts := options.Update().SetUpsert(true)

	_, err := m.depotCollection.UpdateOne(context.TODO(), filter, update, opts)
	return err
}

func (m *MongoStore) DepotGet(space store_interface.TenancySpace, key int64) (string, error) {
	filter := bson.M{"appId": space.AppId, "key": key}

	var result struct {
		Value string `bson:"value"`
	}

	err := m.depotCollection.FindOne(context.TODO(), filter).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return "", fmt.Errorf("not found")
	}
	return result.Value, err
}

func (m *MongoStore) DepotDelete(space store_interface.TenancySpace, key int64) error {
	filter := bson.M{"appId": space.AppId, "key": key}
	_, err := m.depotCollection.DeleteOne(context.TODO(), filter)
	return err
}

func (m *MongoStore) DepotPutMany(space store_interface.TenancySpace, items []model.DepotKeyValueItem) error {
	var ops []mongo.WriteModel

	for _, item := range items {
		filter := bson.M{"appId": space.AppId, "key": item.Key}
		update := bson.M{"$set": bson.M{"value": item.Value}}
		ops = append(ops, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	if len(ops) == 0 {
		return nil
	}

	_, err := m.depotCollection.BulkWrite(context.TODO(), ops, options.BulkWrite().SetOrdered(false))
	return err
}

func (m *MongoStore) DepotGetMany(space store_interface.TenancySpace, keys []int64) (map[int64]string, []int64, error) {
	filter := bson.M{
		"appId": space.AppId,
		"key":   bson.M{"$in": keys},
	}

	cur, err := m.depotCollection.Find(context.TODO(), filter)
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
