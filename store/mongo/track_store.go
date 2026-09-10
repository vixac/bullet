package mongodb

import (
	"context"
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func (m *MongoStore) TrackMutate(space store_interface.TenancySpace, req store_interface.TrackMutation) (store_interface.TrackMutationResult, error) {
	mutations := m.trackCollection.Database().Collection("track_mutations")
	var applied bool
	err := m.trackTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		// WithTransaction can retry the callback; never keep an earlier attempt's result.
		applied = false
		marker, err := mutations.UpdateOne(ctx, bson.M{"_id": string(req.MutationID)},
			bson.M{"$setOnInsert": bson.M{"applied": true}}, options.Update().SetUpsert(true))
		if err != nil {
			return nil, err
		}
		if marker.UpsertedCount == 0 {
			return nil, nil
		}
		writes := make([]mongo.WriteModel, 0, len(req.Puts)+len(req.Deletes))
		for _, put := range req.Puts {
			writes = append(writes, trackPutModel(space, put.BucketID, put.Key,
				model.TrackValue{Value: put.Value, Tag: put.Tag, Metric: put.Metric}))
		}
		for _, key := range req.Deletes {
			writes = append(writes, mongo.NewDeleteOneModel().SetFilter(trackKeyFilter(space, key.BucketID, key.Key)))
		}
		if len(writes) > 0 {
			if _, err := m.trackCollection.BulkWrite(ctx, writes, options.BulkWrite().SetOrdered(true)); err != nil {
				return nil, err
			}
		}
		applied = true
		return nil, nil
	})
	if err != nil {
		return store_interface.TrackMutationResult{}, err
	}
	return store_interface.TrackMutationResult{Applied: applied}, nil
}

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

	return m.trackTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		return m.trackCollection.DeleteMany(ctx, filter)
	})
}

func (m *MongoStore) TrackPut(space store_interface.TenancySpace, bucketID int32, key string, value int64, tag *int64, metric *float64) error {
	put := trackPutModel(space, bucketID, key, model.TrackValue{Value: value, Tag: tag, Metric: metric})
	_, err := m.trackCollection.ReplaceOne(context.TODO(), put.Filter, put.Replacement, options.Replace().SetUpsert(true))
	return err
}

func (m *MongoStore) TrackGet(space store_interface.TenancySpace, bucketID int32, key string) (int64, error) {
	var result struct{ Value int64 }
	filter := bson.M{"appId": space.AppId, "tenancyId": space.TenancyId, "bucketId": bucketID, "key": key}
	err := m.trackCollection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		return 0, err
	}
	return result.Value, nil
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
	var writes []mongo.WriteModel

	for bucketID, kvItems := range items {
		for _, kv := range kvItems {
			writes = append(writes, trackPutModel(space, bucketID, kv.Key, kv.Value))
		}
	}

	if len(writes) == 0 {
		return nil
	}

	return m.trackTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		return m.trackCollection.BulkWrite(ctx, writes, options.BulkWrite().SetOrdered(true))
	})
}

// trackTransaction provides one snapshot for reads and an all-or-nothing commit
// for writes. It requires a replica set or sharded cluster and never falls back
// to nontransactional operations on unsupported deployments.
func (m *MongoStore) trackTransaction(fn func(mongo.SessionContext) (interface{}, error)) error {
	ctx := context.Background()
	session, err := m.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	_, err = session.WithTransaction(ctx, fn, options.Transaction().
		SetReadConcern(readconcern.Snapshot()).
		SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.Majority()))
	return err
}

func trackKeyFilter(space store_interface.TenancySpace, bucketID int32, key string) bson.M {
	return bson.M{"appId": space.AppId, "tenancyId": space.TenancyId, "bucketId": bucketID, "key": key}
}

func trackPutModel(space store_interface.TenancySpace, bucketID int32, key string, value model.TrackValue) *mongo.ReplaceOneModel {
	doc := trackKeyFilter(space, bucketID, key)
	doc["value"] = value.Value
	if value.Tag != nil {
		doc["tag"] = *value.Tag
	}
	if value.Metric != nil {
		doc["metric"] = *value.Metric
	}
	return mongo.NewReplaceOneModel().SetFilter(trackKeyFilter(space, bucketID, key)).SetReplacement(doc).SetUpsert(true)
}

type trackDocument struct {
	BucketID int32    `bson:"bucketId"`
	Key      string   `bson:"key"`
	Value    int64    `bson:"value"`
	Tag      *int64   `bson:"tag,omitempty"`
	Metric   *float64 `bson:"metric,omitempty"`
}

// trackFind consumes every cursor batch in one snapshot transaction. Results
// from failed or retried attempts must not escape to the caller.
func (m *MongoStore) trackFind(filter bson.M) ([]trackDocument, error) {
	var documents []trackDocument
	err := m.trackTransaction(func(ctx mongo.SessionContext) (interface{}, error) {
		documents = nil
		cursor, err := m.trackCollection.Find(ctx, filter)
		if err != nil {
			return nil, err
		}
		defer cursor.Close(ctx)
		return nil, cursor.All(ctx, &documents)
	})
	if err != nil {
		return nil, err
	}
	return documents, nil
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

	documents, err := m.trackFind(bson.M{"$or": orFilters})
	if err != nil {
		return nil, nil, err
	}

	foundKeys := make(map[int32]map[string]bool)
	for _, result := range documents {
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

	documents, err := m.trackFind(filter)
	if err != nil {
		return nil, err
	}
	var results []model.TrackKeyValueItem
	for _, doc := range documents {
		results = append(results, model.TrackKeyValueItem{
			Key:   doc.Key,
			Value: model.TrackValue{Value: doc.Value, Tag: doc.Tag, Metric: doc.Metric},
		})
	}
	return results, nil
}
