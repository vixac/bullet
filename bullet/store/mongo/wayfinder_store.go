package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/vixac/bullet/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func generateItemID() int64 {
	return time.Now().UnixNano()
}

func (s *MongoStore) WayFinderPut(appID int32, bucketID int32, key string, payload string, tag *int64, metric *float64) (int64, error) {
	id := generateItemID()

	// Insert into Track
	err := s.TrackPut(appID, bucketID, key, int64(id), tag, metric)
	if err != nil {
		return 0, err
	}

	// Insert into Depot
	err = s.DepotPut(appID, int64(id), payload)
	if err != nil {
		return 0, err
	}

	return int64(id), nil
}

//VX:TODO she wants a t shirt that says not today santa.

func (s *MongoStore) WayFinderGetByPrefix(
	appID int32,
	bucketID int32,
	prefix string,
	tags []int64,
	metricValue *float64,
	metricIsGt bool,
) ([]model.WayFinderQueryItem, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build $match stage dynamically
	matchStage := bson.D{
		{"appId", appID},
		{"bucketId", bucketID},
		{"key", bson.M{"$regex": "^" + prefix}},
	}

	if len(tags) > 0 {
		matchStage = append(matchStage, bson.E{"tag", bson.M{"$in": tags}})
	}

	if metricValue != nil {
		op := "$gt"
		if !metricIsGt {
			op = "$lt"
		}
		matchStage = append(matchStage, bson.E{"metric", bson.M{op: *metricValue}})
	}

	// Build aggregation pipeline
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchStage}},
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "depot"},
			{Key: "localField", Value: "value"},
			{Key: "foreignField", Value: "key"},
			{Key: "as", Value: "depotPayload"},
		}}},
		{{Key: "$unwind", Value: "$depotPayload"}},
		{{Key: "$project", Value: bson.D{
			{Key: "key", Value: 1},
			{Key: "value", Value: 1},
			{Key: "tag", Value: 1},
			{Key: "metric", Value: 1},
			{Key: "payload", Value: "$depotPayload.value"},
		}}},
	}

	cur, err := s.trackCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregation error: %w", err)
	}
	defer cur.Close(ctx)

	// Read results
	var result []model.WayFinderQueryItem
	for cur.Next(ctx) {
		var doc struct {
			Key     string   `bson:"key"`
			Value   int64    `bson:"value"`
			Tag     *int64   `bson:"tag,omitempty"`
			Metric  *float64 `bson:"metric,omitempty"`
			Payload string   `bson:"payload"`
		}
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		result = append(result, model.WayFinderQueryItem{
			Key:     doc.Key,
			ItemId:  doc.Value,
			Tag:     doc.Tag,
			Metric:  doc.Metric,
			Payload: doc.Payload,
		})
	}
	if err := cur.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return result, nil
}
