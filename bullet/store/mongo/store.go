package mongodb

import (
    "context"
    "time"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/bson"
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

    return &MongoStore{
        client:     client,
        collection: client.Database("bullet").Collection("kv"),
    }, nil
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
