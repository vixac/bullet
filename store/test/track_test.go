package store_test

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func TestTrackPayloadContract(t *testing.T) {
	for name, store := range trackStores {
		t.Run(name, func(t *testing.T) {
			space := model.TenancySpace{AppId: 109, TenancyId: 1}
			bucketID := int32(81)
			payload := []byte("payload")

			if err := store.TrackPut(space, bucketID, "payload:key", model.TrackValue{Value: 1, Payload: payload}); err != nil {
				t.Fatalf("put payload: %v", err)
			}
			payload[0] = 'X'

			without, err := store.TrackGet(space, bucketID, "payload:key", model.TrackReadOptions{})
			if err != nil {
				t.Fatalf("get without payload: %v", err)
			}
			if without.Payload != nil {
				t.Fatalf("default point read returned payload: %q", without.Payload)
			}

			with, err := store.TrackGet(space, bucketID, "payload:key", model.TrackReadOptions{IncludePayload: true})
			if err != nil {
				t.Fatalf("get with payload: %v", err)
			}
			if !bytes.Equal(with.Payload, []byte("payload")) {
				t.Fatalf("payload mismatch: %q", with.Payload)
			}
			with.Payload[0] = 'Y'
			again, err := store.TrackGet(space, bucketID, "payload:key", model.TrackReadOptions{IncludePayload: true})
			if err != nil || !bytes.Equal(again.Payload, []byte("payload")) {
				t.Fatalf("returned payload was not caller-owned: value=%q err=%v", again.Payload, err)
			}

			values, _, err := store.TrackGetMany(space, map[int32][]string{bucketID: {"payload:key"}}, model.TrackReadOptions{})
			if err != nil || values[bucketID]["payload:key"].Payload != nil {
				t.Fatalf("default explicit-key read returned payload: value=%q err=%v", values[bucketID]["payload:key"].Payload, err)
			}
			values, _, err = store.TrackGetMany(space, map[int32][]string{bucketID: {"payload:key"}}, model.TrackReadOptions{IncludePayload: true})
			if err != nil || !bytes.Equal(values[bucketID]["payload:key"].Payload, []byte("payload")) {
				t.Fatalf("explicit-key payload mismatch: value=%q err=%v", values[bucketID]["payload:key"].Payload, err)
			}

			items, err := store.GetItemsByKeyPrefix(space, bucketID, "payload:", nil, nil, false)
			if err != nil || len(items) != 1 {
				t.Fatalf("prefix read: items=%v err=%v", items, err)
			}
			if items[0].Value.Payload != nil {
				t.Fatalf("prefix read returned payload: %q", items[0].Value.Payload)
			}

			if err := store.TrackPut(space, bucketID, "empty", model.TrackValue{Value: 2, Payload: []byte{}}); err != nil {
				t.Fatalf("put empty payload: %v", err)
			}
			empty, err := store.TrackGet(space, bucketID, "empty", model.TrackReadOptions{IncludePayload: true})
			if err != nil || empty.Payload == nil || len(empty.Payload) != 0 {
				t.Fatalf("empty payload was not preserved: value=%#v err=%v", empty.Payload, err)
			}

			if err := store.TrackPut(space, bucketID, "payload:key", model.TrackValue{Value: 3}); err != nil {
				t.Fatalf("full replacement without payload: %v", err)
			}
			cleared, err := store.TrackGet(space, bucketID, "payload:key", model.TrackReadOptions{IncludePayload: true})
			if err != nil || cleared.Payload != nil {
				t.Fatalf("nil payload did not clear stored payload: value=%q err=%v", cleared.Payload, err)
			}

			maximum := bytes.Repeat([]byte{'m'}, model.TrackMaxPayloadBytes)
			if err := store.TrackPut(space, bucketID, "maximum", model.TrackValue{Value: 4, Payload: maximum}); err != nil {
				t.Fatalf("maximum payload rejected: %v", err)
			}
			tooLarge := bytes.Repeat([]byte{'x'}, model.TrackMaxPayloadBytes+1)
			if err := store.TrackPut(space, bucketID, "maximum", model.TrackValue{Value: 5, Payload: tooLarge}); !errors.Is(err, model.ErrTrackPayloadTooLarge) {
				t.Fatalf("oversized payload error = %v", err)
			}
			unchanged, err := store.TrackGet(space, bucketID, "maximum", model.TrackReadOptions{IncludePayload: true})
			if err != nil || unchanged.Value != 4 || len(unchanged.Payload) != model.TrackMaxPayloadBytes {
				t.Fatalf("rejected replacement changed value: value=%+v err=%v", unchanged, err)
			}

			batch := map[int32][]model.TrackKeyValueItem{bucketID: {
				{Key: "batch-valid", Value: model.TrackValue{Value: 6}},
				{Key: "batch-large", Value: model.TrackValue{Value: 7, Payload: tooLarge}},
			}}
			if err := store.TrackPutMany(space, batch); !errors.Is(err, model.ErrTrackPayloadTooLarge) {
				t.Fatalf("oversized batch error = %v", err)
			}
			if _, err := store.TrackGet(space, bucketID, "batch-valid", model.TrackReadOptions{}); err == nil {
				t.Fatal("oversized batch partially committed")
			}

			mutation := model.TrackMutation{MutationID: "payload-limit-109", Puts: []model.TrackPut{{
				BucketID: bucketID, Key: "mutation", Value: model.TrackValue{Value: 8, Payload: tooLarge},
			}}}
			if _, err := store.TrackMutate(space, mutation); !errors.Is(err, model.ErrTrackPayloadTooLarge) {
				t.Fatalf("oversized mutation error = %v", err)
			}
			mutation.Puts[0].Value.Payload = []byte("valid")
			result, err := store.TrackMutate(space, mutation)
			if err != nil || !result.Applied {
				t.Fatalf("oversized mutation consumed its ID: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestTrackMutateIsAtomicAndIdempotent(t *testing.T) {
	for name, trackStore := range trackStores {
		t.Run(name, func(t *testing.T) {
			space := model.TenancySpace{AppId: 901, TenancyId: 902}
			if err := trackStore.TrackPut(space, 1, "delete-me", model.TrackValue{Value: 1}); err != nil {
				t.Fatal(err)
			}

			req := model.TrackMutation{
				MutationID: model.MutationID("track-mutation-test-901-902"),
				Puts: []model.TrackPut{
					{BucketID: 1, Key: "put-me", Value: model.TrackValue{Value: 42}},
				},
				Deletes: []model.TrackKey{{BucketID: 1, Key: "delete-me"}},
			}
			result, err := trackStore.TrackMutate(space, req)
			if errors.Is(err, model.ErrTrackMutationUnsupported) {
				t.Skip("track mutations are intentionally unsupported")
			}
			if err != nil {
				t.Fatalf("first mutation: %v", err)
			}
			if !result.Applied {
				t.Fatal("first mutation was not applied")
			}
			if got, err := trackStore.TrackGet(space, 1, "put-me", model.TrackReadOptions{}); err != nil || got.Value != 42 {
				t.Fatalf("put result: got %d, err %v", got.Value, err)
			}
			if _, err := trackStore.TrackGet(space, 1, "delete-me", model.TrackReadOptions{}); err == nil {
				t.Fatal("delete was not applied")
			}

			// Changing the replay proves the mutation body is not executed twice.
			req.Puts[0].Value = model.TrackValue{Value: 99}
			result, err = trackStore.TrackMutate(space, req)
			if err != nil {
				t.Fatalf("replayed mutation: %v", err)
			}
			if result.Applied {
				t.Fatal("replayed mutation reported Applied")
			}
			if got, err := trackStore.TrackGet(space, 1, "put-me", model.TrackReadOptions{}); err != nil || got.Value != 42 {
				t.Fatalf("replay changed value: got %d, err %v", got.Value, err)
			}
		})
	}
}

func TestTrackMutateIfAbsentIsAtomicAndRetryableAfterConflict(t *testing.T) {
	for name, trackStore := range trackStores {
		t.Run(name, func(t *testing.T) {
			space := model.TenancySpace{AppId: 903, TenancyId: 904}
			if err := trackStore.TrackPut(space, 1, "existing", model.TrackValue{Value: 1}); err != nil {
				t.Fatal(err)
			}

			req := model.TrackMutation{
				MutationID: "track-if-absent-conflict-903-904",
				Puts: []model.TrackPut{
					{BucketID: 1, Key: "new", Value: model.TrackValue{Value: 2}, IfAbsent: true},
					{BucketID: 1, Key: "existing", Value: model.TrackValue{Value: 3}, IfAbsent: true},
				},
			}
			_, err := trackStore.TrackMutate(space, req)
			if errors.Is(err, model.ErrTrackMutationUnsupported) {
				t.Skip("track mutations are intentionally unsupported")
			}
			if !errors.Is(err, model.ErrTrackKeyAlreadyExists) {
				t.Fatalf("expected create-only conflict, got %v", err)
			}
			if _, err := trackStore.TrackGet(space, 1, "new", model.TrackReadOptions{}); err == nil {
				t.Fatal("conflicting mutation partially inserted new")
			}
			if got, err := trackStore.TrackGet(space, 1, "existing", model.TrackReadOptions{}); err != nil || got.Value != 1 {
				t.Fatalf("conflicting mutation changed existing: got %d, err %v", got.Value, err)
			}

			// A failed condition must not consume the ID: once the key is removed,
			// retrying the same request can commit.
			if err := trackStore.TrackDeleteMany(space, []model.TrackKey{{BucketID: 1, Key: "existing"}}); err != nil {
				t.Fatal(err)
			}
			result, err := trackStore.TrackMutate(space, req)
			if err != nil || !result.Applied {
				t.Fatalf("retry after resolving conflict: result=%+v, err=%v", result, err)
			}
			for key, want := range map[string]int64{"new": 2, "existing": 3} {
				if got, err := trackStore.TrackGet(space, 1, key, model.TrackReadOptions{}); err != nil || got.Value != want {
					t.Fatalf("created %q: got %d, err %v", key, got.Value, err)
				}
			}
		})
	}
}

// trackStores is defined and populated in stores.go

func TestTrackBasicOperations(t *testing.T) {
	for name, store := range trackStores {
		testTrackBasicOperations(store, name, t)
	}
}

func testTrackBasicOperations(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 100, TenancyId: 1}
		bucketID := int32(1)

		// Test basic put and get
		key := "test_key_1"
		value := int64(42)
		err := store.TrackPut(space, bucketID, key, model.TrackValue{Value: value})
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		got, err := store.TrackGet(space, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}
		if got.Value != value {
			t.Errorf("Expected value %d, got %d", value, got.Value)
		}

		// Test put with tag and metric
		key2 := "test_key_2"
		value2 := int64(100)
		tag := int64(5)
		metric := 3.14
		err = store.TrackPut(space, bucketID, key2, model.TrackValue{Value: value2, Tag: &tag, Metric: &metric})
		if err != nil {
			t.Fatalf("Failed to put with tag/metric: %v", err)
		}

		got2, err := store.TrackGet(space, bucketID, key2, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Failed to get key2: %v", err)
		}
		if got2.Value != value2 {
			t.Errorf("Expected value %d, got %d", value2, got2.Value)
		}

		// Test overwrite
		newValue := int64(999)
		err = store.TrackPut(space, bucketID, key, model.TrackValue{Value: newValue})
		if err != nil {
			t.Fatalf("Failed to overwrite: %v", err)
		}

		got3, err := store.TrackGet(space, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Failed to get after overwrite: %v", err)
		}
		if got3.Value != newValue {
			t.Errorf("Expected overwritten value %d, got %d", newValue, got3.Value)
		}

		// Test get non-existent key
		_, err = store.TrackGet(space, bucketID, "non_existent_key", model.TrackReadOptions{})
		if err == nil {
			t.Error("Expected error for non-existent key, got nil")
		}

		// Test get from non-existent bucket
		_, err = store.TrackGet(space, int32(9999), "any_key", model.TrackReadOptions{})
		if err == nil {
			t.Error("Expected error for non-existent bucket, got nil")
		}
	})
}

func TestTrackPutManyGetMany(t *testing.T) {
	for name, store := range trackStores {
		testTrackPutManyGetMany(store, name, t)
	}
}

func testTrackPutManyGetMany(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 101, TenancyId: 1}
		bucketID1 := int32(10)
		bucketID2 := int32(20)

		// Prepare items for put many
		tag1, tag2 := int64(1), int64(2)
		metric1, metric2 := 1.5, 2.5

		items := map[int32][]model.TrackKeyValueItem{
			bucketID1: {
				{Key: "a", Value: model.TrackValue{Value: 100, Tag: &tag1, Metric: &metric1}},
				{Key: "b", Value: model.TrackValue{Value: 200, Tag: &tag2, Metric: &metric2}},
			},
			bucketID2: {
				{Key: "c", Value: model.TrackValue{Value: 300, Tag: nil, Metric: nil}},
			},
		}

		err := store.TrackPutMany(space, items)
		if err != nil {
			t.Fatalf("TrackPutMany failed: %v", err)
		}

		// Test get many - all found
		keys := map[int32][]string{
			bucketID1: {"a", "b"},
			bucketID2: {"c"},
		}

		found, missing, err := store.TrackGetMany(space, keys, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("TrackGetMany failed: %v", err)
		}

		// Verify found items
		if len(found[bucketID1]) != 2 {
			t.Errorf("Expected 2 items in bucket1, got %d", len(found[bucketID1]))
		}
		if found[bucketID1]["a"].Value != 100 {
			t.Errorf("Expected a=100, got %d", found[bucketID1]["a"].Value)
		}
		if found[bucketID1]["b"].Value != 200 {
			t.Errorf("Expected b=200, got %d", found[bucketID1]["b"].Value)
		}
		if len(found[bucketID2]) != 1 {
			t.Errorf("Expected 1 item in bucket2, got %d", len(found[bucketID2]))
		}

		// Verify tag and metric preserved
		if found[bucketID1]["a"].Tag == nil || *found[bucketID1]["a"].Tag != tag1 {
			t.Error("Tag not preserved for key 'a'")
		}
		if found[bucketID1]["a"].Metric == nil || *found[bucketID1]["a"].Metric != metric1 {
			t.Error("Metric not preserved for key 'a'")
		}

		// Verify missing is empty
		if len(missing[bucketID1]) != 0 || len(missing[bucketID2]) != 0 {
			t.Errorf("Expected no missing items, got bucket1=%v, bucket2=%v", missing[bucketID1], missing[bucketID2])
		}

		// Test get many with some missing keys
		keysWithMissing := map[int32][]string{
			bucketID1: {"a", "missing1", "b", "missing2"},
			bucketID2: {"c", "missing3"},
		}

		found2, missing2, err := store.TrackGetMany(space, keysWithMissing, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("TrackGetMany with missing failed: %v", err)
		}

		if len(found2[bucketID1]) != 2 {
			t.Errorf("Expected 2 found in bucket1, got %d", len(found2[bucketID1]))
		}
		if len(missing2[bucketID1]) != 2 {
			t.Errorf("Expected 2 missing in bucket1, got %d", len(missing2[bucketID1]))
		}
		if len(missing2[bucketID2]) != 1 {
			t.Errorf("Expected 1 missing in bucket2, got %d", len(missing2[bucketID2]))
		}

		// Test get many from non-existent bucket
		keysNonExistent := map[int32][]string{
			int32(9999): {"x", "y"},
		}
		found3, missing3, err := store.TrackGetMany(space, keysNonExistent, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("TrackGetMany from non-existent bucket failed: %v", err)
		}
		if len(found3[9999]) != 0 {
			t.Errorf("Expected 0 found from non-existent bucket, got %d", len(found3[9999]))
		}
		if len(missing3[9999]) != 2 {
			t.Errorf("Expected 2 missing from non-existent bucket, got %d", len(missing3[9999]))
		}
	})
}

func TestTrackDeleteMany(t *testing.T) {
	for name, store := range trackStores {
		testTrackDeleteMany(store, name, t)
	}
}

func testTrackDeleteMany(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 102, TenancyId: 1}
		bucketID := int32(30)

		// Put some items first
		items := map[int32][]model.TrackKeyValueItem{
			bucketID: {
				{Key: "del1", Value: model.TrackValue{Value: 1}},
				{Key: "del2", Value: model.TrackValue{Value: 2}},
				{Key: "keep", Value: model.TrackValue{Value: 3}},
			},
		}
		err := store.TrackPutMany(space, items)
		if err != nil {
			t.Fatalf("Setup TrackPutMany failed: %v", err)
		}

		// Delete some items
		deleteItems := []model.TrackKey{
			{BucketID: bucketID, Key: "del1"},
			{BucketID: bucketID, Key: "del2"},
		}
		err = store.TrackDeleteMany(space, deleteItems)
		if err != nil {
			t.Fatalf("TrackDeleteMany failed: %v", err)
		}

		// Verify deleted items are gone
		_, err = store.TrackGet(space, bucketID, "del1", model.TrackReadOptions{})
		if err == nil {
			t.Error("del1 should be deleted")
		}
		_, err = store.TrackGet(space, bucketID, "del2", model.TrackReadOptions{})
		if err == nil {
			t.Error("del2 should be deleted")
		}

		// Verify kept item still exists
		val, err := store.TrackGet(space, bucketID, "keep", model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("keep should still exist: %v", err)
		}
		if val.Value != 3 {
			t.Errorf("Expected keep=3, got %d", val.Value)
		}

		// Test deleting non-existent keys (should not error - idempotent)
		deleteNonExistent := []model.TrackKey{
			{BucketID: bucketID, Key: "non_existent"},
		}
		err = store.TrackDeleteMany(space, deleteNonExistent)
		if err != nil {
			t.Errorf("Deleting non-existent key should not error: %v", err)
		}

		// Test deleting from non-existent bucket (behavior may vary)
		deleteFromNonExistent := []model.TrackKey{
			{BucketID: int32(9999), Key: "any"},
		}
		err = store.TrackDeleteMany(space, deleteFromNonExistent)
		// Note: This may reveal inconsistencies between implementations
		// RAM and SQLite don't error, BoltDB may error
		if err != nil {
			t.Logf("Note: %s returns error when deleting from non-existent bucket: %v", name, err)
		}
	})
}

func TestTrackGetItemsByKeyPrefix(t *testing.T) {
	for name, store := range trackStores {
		testTrackGetItemsByKeyPrefix(store, name, t)
	}
}

func testTrackGetItemsByKeyPrefix(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 103, TenancyId: 1}
		bucketID := int32(40)

		// Setup data with various prefixes, tags, and metrics
		tag1, tag2, tag3 := int64(1), int64(2), int64(3)
		metric1, metric2, metric3 := 10.0, 20.0, 30.0

		items := map[int32][]model.TrackKeyValueItem{
			bucketID: {
				{Key: "user:1:name", Value: model.TrackValue{Value: 100, Tag: &tag1, Metric: &metric1}},
				{Key: "user:1:email", Value: model.TrackValue{Value: 101, Tag: &tag1, Metric: &metric2}},
				{Key: "user:2:name", Value: model.TrackValue{Value: 200, Tag: &tag2, Metric: &metric3}},
				{Key: "user:2:email", Value: model.TrackValue{Value: 201, Tag: &tag2, Metric: &metric1}},
				{Key: "order:1:total", Value: model.TrackValue{Value: 300, Tag: &tag3, Metric: &metric2}},
				{Key: "order:2:total", Value: model.TrackValue{Value: 400, Tag: nil, Metric: nil}},
			},
		}
		err := store.TrackPutMany(space, items)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Test basic prefix query
		results, err := store.GetItemsByKeyPrefix(space, bucketID, "user:", nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix failed: %v", err)
		}
		if len(results) != 4 {
			t.Errorf("Expected 4 user items, got %d", len(results))
		}

		// Test more specific prefix
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:1:", nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix user:1: failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 user:1 items, got %d", len(results))
		}

		// Test prefix with tag filter
		filterTags := []int64{tag1}
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:", filterTags, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix with tag filter failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 items with tag1, got %d", len(results))
		}
		for _, r := range results {
			if r.Value.Tag == nil || *r.Value.Tag != tag1 {
				t.Errorf("Expected tag1, got %v", r.Value.Tag)
			}
		}

		// Test prefix with multiple tags filter
		filterMultiTags := []int64{tag1, tag2}
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:", filterMultiTags, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix with multi-tag filter failed: %v", err)
		}
		if len(results) != 4 {
			t.Errorf("Expected 4 items with tag1 or tag2, got %d", len(results))
		}

		// Test prefix with metric filter (greater than)
		metricThreshold := 15.0
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:", nil, &metricThreshold, true)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix with metric > filter failed: %v", err)
		}
		// Should match items with metric > 15.0: metric2=20.0, metric3=30.0
		for _, r := range results {
			if r.Value.Metric == nil || *r.Value.Metric <= metricThreshold {
				t.Errorf("Expected metric > %.1f, got %v for key %s", metricThreshold, r.Value.Metric, r.Key)
			}
		}

		// Test prefix with metric filter (less than)
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:", nil, &metricThreshold, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix with metric < filter failed: %v", err)
		}
		// Should match items with metric < 15.0: metric1=10.0
		for _, r := range results {
			if r.Value.Metric == nil || *r.Value.Metric >= metricThreshold {
				t.Errorf("Expected metric < %.1f, got %v for key %s", metricThreshold, r.Value.Metric, r.Key)
			}
		}

		// Test combined tag and metric filter
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "user:", filterTags, &metricThreshold, true)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix with combined filter failed: %v", err)
		}
		// tag1 items with metric > 15.0
		for _, r := range results {
			if r.Value.Tag == nil || *r.Value.Tag != tag1 {
				t.Errorf("Expected tag1, got %v", r.Value.Tag)
			}
			if r.Value.Metric == nil || *r.Value.Metric <= metricThreshold {
				t.Errorf("Expected metric > %.1f, got %v", metricThreshold, r.Value.Metric)
			}
		}

		// Test non-existent prefix
		results, err = store.GetItemsByKeyPrefix(space, bucketID, "nonexistent:", nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix non-existent failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 items for non-existent prefix, got %d", len(results))
		}

		// Test from non-existent bucket
		results, err = store.GetItemsByKeyPrefix(space, int32(9999), "user:", nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefix from non-existent bucket failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 items from non-existent bucket, got %d", len(results))
		}
	})
}

func TestTrackGetItemsByKeyPrefixes(t *testing.T) {
	for name, store := range trackStores {
		testTrackGetItemsByKeyPrefixes(store, name, t)
	}
}

func testTrackGetItemsByKeyPrefixes(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 104, TenancyId: 1}
		bucketID := int32(50)

		// Setup data
		tag1 := int64(1)
		items := map[int32][]model.TrackKeyValueItem{
			bucketID: {
				{Key: "cat:1", Value: model.TrackValue{Value: 10, Tag: &tag1}},
				{Key: "cat:2", Value: model.TrackValue{Value: 20, Tag: &tag1}},
				{Key: "dog:1", Value: model.TrackValue{Value: 30, Tag: nil}},
				{Key: "dog:2", Value: model.TrackValue{Value: 40, Tag: nil}},
				{Key: "bird:1", Value: model.TrackValue{Value: 50, Tag: nil}},
			},
		}
		err := store.TrackPutMany(space, items)
		if err != nil {
			t.Fatalf("Setup failed: %v", err)
		}

		// Test multiple prefixes
		prefixes := []string{"cat:", "dog:"}
		results, err := store.GetItemsByKeyPrefixes(space, bucketID, prefixes, nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefixes failed: %v", err)
		}
		if len(results) != 4 {
			t.Errorf("Expected 4 items (2 cats + 2 dogs), got %d", len(results))
		}

		// Verify we got the right keys
		keys := make([]string, len(results))
		for i, r := range results {
			keys[i] = r.Key
		}
		sort.Strings(keys)
		expectedKeys := []string{"cat:1", "cat:2", "dog:1", "dog:2"}
		sort.Strings(expectedKeys)
		for i, k := range keys {
			if k != expectedKeys[i] {
				t.Errorf("Expected key %s, got %s", expectedKeys[i], k)
			}
		}

		// Test single prefix (should work same as GetItemsByKeyPrefix)
		results, err = store.GetItemsByKeyPrefixes(space, bucketID, []string{"bird:"}, nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefixes single prefix failed: %v", err)
		}
		if len(results) != 1 {
			t.Errorf("Expected 1 bird item, got %d", len(results))
		}

		// Test empty prefix list
		results, err = store.GetItemsByKeyPrefixes(space, bucketID, []string{}, nil, nil, false)
		// Behavior varies: some return error, some return empty
		if err == nil && len(results) != 0 {
			t.Logf("Note: %s returns %d items for empty prefix list", name, len(results))
		}

		// Test empty string prefix (matches all)
		results, err = store.GetItemsByKeyPrefixes(space, bucketID, []string{""}, nil, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefixes empty string prefix failed: %v", err)
		}
		if len(results) != 5 {
			t.Errorf("Expected 5 items for empty prefix (all), got %d", len(results))
		}

		// Test with tag filter
		results, err = store.GetItemsByKeyPrefixes(space, bucketID, []string{"cat:", "dog:"}, []int64{tag1}, nil, false)
		if err != nil {
			t.Fatalf("GetItemsByKeyPrefixes with tag failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 items with tag1, got %d", len(results))
		}
	})
}

func TestTrackMultiTenancy(t *testing.T) {
	for name, store := range trackStores {
		testTrackMultiTenancy(store, name, t)
	}
}

func testTrackMultiTenancy(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space1 := model.TenancySpace{AppId: 105, TenancyId: 1}
		space2 := model.TenancySpace{AppId: 105, TenancyId: 2}
		space3 := model.TenancySpace{AppId: 106, TenancyId: 1}
		bucketID := int32(60)

		// Put same key in different tenancy spaces
		key := "shared_key"

		err := store.TrackPut(space1, bucketID, key, model.TrackValue{Value: 100})
		if err != nil {
			t.Fatalf("Put to space1 failed: %v", err)
		}
		err = store.TrackPut(space2, bucketID, key, model.TrackValue{Value: 200})
		if err != nil {
			t.Fatalf("Put to space2 failed: %v", err)
		}
		err = store.TrackPut(space3, bucketID, key, model.TrackValue{Value: 300})
		if err != nil {
			t.Fatalf("Put to space3 failed: %v", err)
		}

		// Verify isolation
		val1, err := store.TrackGet(space1, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from space1 failed: %v", err)
		}
		if val1.Value != 100 {
			t.Errorf("Expected space1 value 100, got %d", val1.Value)
		}

		val2, err := store.TrackGet(space2, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from space2 failed: %v", err)
		}
		if val2.Value != 200 {
			t.Errorf("Expected space2 value 200, got %d", val2.Value)
		}

		val3, err := store.TrackGet(space3, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from space3 failed: %v", err)
		}
		if val3.Value != 300 {
			t.Errorf("Expected space3 value 300, got %d", val3.Value)
		}

		// Delete from space1 shouldn't affect others
		err = store.TrackDeleteMany(space1, []model.TrackKey{{BucketID: bucketID, Key: key}})
		if err != nil {
			t.Fatalf("Delete from space1 failed: %v", err)
		}

		_, err = store.TrackGet(space1, bucketID, key, model.TrackReadOptions{})
		if err == nil {
			t.Error("Key should be deleted from space1")
		}

		val2After, err := store.TrackGet(space2, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from space2 after space1 delete failed: %v", err)
		}
		if val2After.Value != 200 {
			t.Errorf("Space2 value should still be 200, got %d", val2After.Value)
		}

		val3After, err := store.TrackGet(space3, bucketID, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from space3 after space1 delete failed: %v", err)
		}
		if val3After.Value != 300 {
			t.Errorf("Space3 value should still be 300, got %d", val3After.Value)
		}
	})
}

func TestTrackBucketIsolation(t *testing.T) {
	for name, store := range trackStores {
		testTrackBucketIsolation(store, name, t)
	}
}

func testTrackBucketIsolation(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 107, TenancyId: 1}
		bucket1 := int32(70)
		bucket2 := int32(71)

		// Put same key in different buckets
		key := "same_key"

		err := store.TrackPut(space, bucket1, key, model.TrackValue{Value: 111})
		if err != nil {
			t.Fatalf("Put to bucket1 failed: %v", err)
		}
		err = store.TrackPut(space, bucket2, key, model.TrackValue{Value: 222})
		if err != nil {
			t.Fatalf("Put to bucket2 failed: %v", err)
		}

		// Verify isolation
		val1, err := store.TrackGet(space, bucket1, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from bucket1 failed: %v", err)
		}
		if val1.Value != 111 {
			t.Errorf("Expected bucket1 value 111, got %d", val1.Value)
		}

		val2, err := store.TrackGet(space, bucket2, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from bucket2 failed: %v", err)
		}
		if val2.Value != 222 {
			t.Errorf("Expected bucket2 value 222, got %d", val2.Value)
		}

		// Delete from bucket1 shouldn't affect bucket2
		err = store.TrackDeleteMany(space, []model.TrackKey{{BucketID: bucket1, Key: key}})
		if err != nil {
			t.Fatalf("Delete from bucket1 failed: %v", err)
		}

		_, err = store.TrackGet(space, bucket1, key, model.TrackReadOptions{})
		if err == nil {
			t.Error("Key should be deleted from bucket1")
		}

		val2After, err := store.TrackGet(space, bucket2, key, model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get from bucket2 after bucket1 delete failed: %v", err)
		}
		if val2After.Value != 222 {
			t.Errorf("Bucket2 value should still be 222, got %d", val2After.Value)
		}
	})
}

func TestTrackLargeValues(t *testing.T) {
	for name, store := range trackStores {
		testTrackLargeValues(store, name, t)
	}
}

func testTrackLargeValues(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := model.TenancySpace{AppId: 108, TenancyId: 1}
		bucketID := int32(80)

		// Test with max int64
		maxVal := int64(9223372036854775807)
		err := store.TrackPut(space, bucketID, "max", model.TrackValue{Value: maxVal})
		if err != nil {
			t.Fatalf("Put max value failed: %v", err)
		}

		got, err := store.TrackGet(space, bucketID, "max", model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get max value failed: %v", err)
		}
		if got.Value != maxVal {
			t.Errorf("Expected max value %d, got %d", maxVal, got.Value)
		}

		// Test with min int64
		minVal := int64(-9223372036854775808)
		err = store.TrackPut(space, bucketID, "min", model.TrackValue{Value: minVal})
		if err != nil {
			t.Fatalf("Put min value failed: %v", err)
		}

		got, err = store.TrackGet(space, bucketID, "min", model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get min value failed: %v", err)
		}
		if got.Value != minVal {
			t.Errorf("Expected min value %d, got %d", minVal, got.Value)
		}

		// Test with zero
		err = store.TrackPut(space, bucketID, "zero", model.TrackValue{Value: 0})
		if err != nil {
			t.Fatalf("Put zero failed: %v", err)
		}

		got, err = store.TrackGet(space, bucketID, "zero", model.TrackReadOptions{})
		if err != nil {
			t.Fatalf("Get zero failed: %v", err)
		}
		if got.Value != 0 {
			t.Errorf("Expected 0, got %d", got.Value)
		}
	})
}
