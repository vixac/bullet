package protocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
)

func TestExistingWireRepresentations(t *testing.T) {
	fixtures := []struct {
		name  string
		value any
		json  string
	}{
		{"single track value is a string", protocol.TrackRequest{BucketID: 1, Key: "k", Value: 9007199254740993}, `{"bucketId":1,"key":"k","value":"9007199254740993"}`},
		{"nested track value keeps capitalized keys and nulls", protocol.TrackPutManyRequest{Buckets: []protocol.TrackPutItems{{BucketID: 1, Items: []protocol.TrackKeyValueItem{{Key: "k", Value: protocol.TrackValue{Value: 42}}}}}}, `{"buckets":[{"bucketId":1,"items":[{"key":"k","value":{"Value":42,"Tag":null,"Metric":null}}]}]}`},
		{"ledger position remains a string", protocol.LedgerRecordResponse{LedgerID: "l", Position: "9007199254740993", AppendID: "a", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), Payload: "p"}, `{"ledger_id":"l","position":"9007199254740993","append_id":"a","created_at":"2026-01-02T03:04:05Z","payload":"p"}`},
		{"warehouse bytes remain base64", protocol.PutBlobRequest{PutID: "p", ContentType: "binary", Value: []byte{0, 255}, Checksum: "c"}, `{"put_id":"p","content_type":"binary","value":"AP8=","checksum":"c"}`},
		{"grove field names", protocol.GroveCreateNodeRequest{NodeID: "n"}, `{"node_id":"n"}`},
		{"depot fields", protocol.DepotCreateRequest{BucketID: 1, Value: "v"}, `{"bucket_id":1,"value":"v"}`},
		{"error body", protocol.ErrorResponse{Error: "missing"}, `{"error":"missing"}`},
		{"mutation has no tenancy fields", protocol.TrackMutateRequest{MutationID: "m", Puts: []protocol.TrackRequest{{BucketID: 1, Key: "k", Value: 42, IfAbsent: true}}, Deletes: []protocol.TrackBucketKeyPair{{BucketID: 2, Key: "d"}}}, `{"mutationId":"m","puts":[{"bucketId":1,"key":"k","value":"42","ifAbsent":true}],"deletes":[{"bucketId":2,"key":"d"}]}`},
	}
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			encoded, err := json.Marshal(f.value)
			require.NoError(t, err)
			require.JSONEq(t, f.json, string(encoded))
		})
	}
}

func TestTrackPutManyCombinesRepeatedBuckets(t *testing.T) {
	var request protocol.TrackPutManyRequest
	require.NoError(t, json.Unmarshal([]byte(`{"buckets":[{"bucketId":1,"items":[{"key":"a","value":{"Value":1}}]},{"bucketId":1,"items":[{"key":"b","value":{"Value":2}}]}]}`), &request))
	require.Equal(t, map[int32][]model.TrackKeyValueItem{1: {{Key: "a", Value: model.TrackValue{Value: 1}}, {Key: "b", Value: model.TrackValue{Value: 2}}}}, request.Model())
}

func TestWarehouseModelRoundTrip(t *testing.T) {
	original := model.Blob{ID: "b", PutID: "p", ContentType: "binary", Value: []byte{0, 255}, Checksum: "c", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	encoded, err := json.Marshal(protocol.BlobFromModel(original))
	require.NoError(t, err)
	var decoded protocol.Blob
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, original, decoded.Model())
}
