package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vixac/bullet/model"
	si "github.com/vixac/bullet/store/store_interface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestTrackBatchTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("requires a MongoDB replica set container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "mongo:7", ExposedPorts: []string{"27017/tcp"},
			Cmd:        []string{"--replSet", "rs0", "--bind_ip_all", "--setParameter", "enableTestCommands=1"},
			WaitingFor: wait.ForListeningPort("27017/tcp"),
		}, Started: true,
	})
	require.NoError(t, err)
	defer container.Terminate(context.Background())
	code, _, err := container.Exec(ctx, []string{"mongosh", "--quiet", "--eval", `rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})`})
	require.NoError(t, err)
	require.Zero(t, code)
	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+endpoint+"/?directConnection=true"))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	require.Eventually(t, func() bool {
		var result struct {
			Primary bool `bson:"isWritablePrimary"`
		}
		return client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&result) == nil && result.Primary
	}, 30*time.Second, 100*time.Millisecond)
	db := client.Database("track_atomicity")
	require.NoError(t, db.CreateCollection(ctx, "track", options.CreateCollection().SetValidator(bson.M{"value": bson.M{"$gte": 0}})))
	s := &MongoStore{client: client, trackCollection: db.Collection("track")}
	space := si.TenancySpace{AppId: 1, TenancyId: 2}
	tag, metric := int64(7), 1.5
	require.NoError(t, s.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{
		1: {{Key: "first", Value: model.TrackValue{Value: 10, Tag: &tag, Metric: &metric}}},
		2: {{Key: "second", Value: model.TrackValue{Value: 20}}},
	}))
	// The last write fails validation after an update and an insert have executed.
	err = s.TrackPutMany(space, map[int32][]model.TrackKeyValueItem{1: {
		{Key: "first", Value: model.TrackValue{Value: 99}},
		{Key: "new", Value: model.TrackValue{Value: 30}},
		{Key: "fail", Value: model.TrackValue{Value: -1}},
	}})
	require.Error(t, err)
	found, missing, err := s.TrackGetMany(space, map[int32][]string{1: {"first", "new", "fail"}, 2: {"second"}})
	require.NoError(t, err)
	require.Equal(t, model.TrackValue{Value: 10, Tag: &tag, Metric: &metric}, found[1]["first"])
	require.ElementsMatch(t, []string{"new", "fail"}, missing[1])
	require.Equal(t, int64(20), found[2]["second"].Value)

	deletes := []model.TrackBucketKeyPair{{BucketID: 1, Key: "first"}, {BucketID: 2, Key: "second"}}
	// Reject commit after DeleteMany executes. No deleted document may become visible.
	require.NoError(t, client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.M{"times": 1}},
		{Key: "data", Value: bson.M{"failCommands": []string{"commitTransaction"}, "errorCode": 2}},
	}).Err())
	require.Error(t, s.TrackDeleteMany(space, deletes))
	for bucket, key := range map[int32]string{1: "first", 2: "second"} {
		got, err := s.TrackGet(space, bucket, key)
		require.NoError(t, err)
		require.Equal(t, int64(bucket*10), got)
	}
	require.NoError(t, s.TrackDeleteMany(space, deletes))
	for bucket, key := range map[int32]string{1: "first", 2: "second"} {
		_, err := s.TrackGet(space, bucket, key)
		require.ErrorIs(t, err, mongo.ErrNoDocuments)
	}
}
