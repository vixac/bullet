package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStore struct {
	client          *mongo.Client
	trackCollection *mongo.Collection
	depotCollection *mongo.Collection
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
		client:          client,
		trackCollection: database.Collection("bucket"),
		depotCollection: database.Collection("depot"),
	}

	//bucket index
	model := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "tenenacyId", Value: 1},
			{Key: "bucketId", Value: 1},
			{Key: "key", Value: 1},
			{Key: "tag", Value: 1},    // equality filters should come before range filters
			{Key: "metric", Value: 1}, // range queries at the end
		},
		Options: options.Index().SetUnique(true),
	}
	println("Attempting to create databses and indexes...")
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)
	_, err = store.trackCollection.Indexes().CreateOne(context.TODO(), model, opts)
	if err != nil {
		print("Creating bucket indexes failed.")
		return nil, err
	}
	uniqueIndex := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "tenancyId", Value: 1},
			{Key: "bucketId", Value: 1},
			{Key: "key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err = store.trackCollection.Indexes().CreateOne(context.TODO(), uniqueIndex, opts)
	if err != nil {
		log.Fatalf("Failed to create unique index: %v", err)
	}

	depotModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "appId", Value: 1},
			{Key: "tenancyId", Value: 1},
			{Key: "key", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}
	_, err = store.depotCollection.Indexes().CreateOne(context.TODO(), depotModel, opts)
	if err != nil {
		println("Creating depot indexes failed.")
		return nil, err
	}

	println("Mongo connection complete.")
	return &store, nil
}
