package mongodb

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	client           *mongo.Client
	bucketCollection *mongo.Collection
	pigeonCollection *mongo.Collection
}

func NewMongoStore(uri string) (*MongoStore, error) {
	println("Attempting to create mongo client")
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		println("Mongo client failed.")
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	println("Attempting to connect...")
	if err := client.Connect(ctx); err != nil {
		println("Mongo connection failed.")
		return nil, err
	}

	database := client.Database("bullet")
	store := MongoStore{
		client:           client,
		bucketCollection: database.Collection("bucket"),
		pigeonCollection: database.Collection("pigeon"),
	}
	//bucket index
	model := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "bucketId", Value: 1},
			{Key: "key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	println("Attempting to create databses and indexes...")
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	_, err = store.bucketCollection.Indexes().CreateOne(context.TODO(), model, opts)
	if err != nil {
		print("Creating bucket indexes failed.")
		return nil, err
	}

	pigeonModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err = store.pigeonCollection.Indexes().CreateOne(context.TODO(), pigeonModel, opts)
	if err != nil {
		println("Creating pigeon indexes failed.")
		return nil, err
	}

	println("Mongo connection complete")
	return &store, nil
}
