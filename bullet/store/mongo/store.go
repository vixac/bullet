package mongodb

import (
	"context"
	"time"

	"github.com/vixac/bullet/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func NewMongoStore(uri string) (*MongoStore, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	store := MongoStore{
		client:     client,
		collection: client.Database("bullet").Collection("kv"),
	}
	model := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "bucketId", Value: 1},
			{Key: "key", Value: 1},
		},
	}
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	_, err = store.collection.Indexes().CreateOne(context.TODO(), model, opts)
	if err != nil {
		return nil, err
	}
	return &store, nil
}

func (m *MongoStore) Put(appID, bucketID int32, key string, value int64) error {
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	update := bson.M{"$set": bson.M{"value": value}}
	_, err := m.collection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
	return err
}

func (m *MongoStore) Get(appID, bucketID int32, key string) (int64, error) {
	var result struct{ Value int64 }
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	err := m.collection.FindOne(context.TODO(), filter).Decode(&result)
	return result.Value, err
}

func (m *MongoStore) Delete(appID, bucketID int32, key string) error {
	filter := bson.M{"appId": appID, "bucketId": bucketID, "key": key}
	_, err := m.collection.DeleteOne(context.TODO(), filter)
	return err
}

func (m *MongoStore) Close() error {
	return m.client.Disconnect(context.TODO())
}

func (m *MongoStore) PutMany(appID int32, items map[int32][]model.KeyValueItem) error {
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

	_, err := m.collection.InsertMany(context.TODO(), docs, options.InsertMany().SetOrdered(false))
	return err
}

func (m *MongoStore) GetMany(appID int32, keys map[int32][]string) (map[int32]map[string]int64, map[int32][]string, error) {
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

	cur, err := m.collection.Find(context.TODO(), bson.M{"$or": orFilters})
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
