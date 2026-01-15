package store_test

import (
	"sort"
	"testing"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/boltdb"
	"github.com/vixac/bullet/store/ram"
	sqlite_store "github.com/vixac/bullet/store/sqlite"
	"github.com/vixac/bullet/store/store_interface"
)

// trackStores contains all TrackStore implementations to test
var trackStores = map[string]store_interface.TrackStore{
	"ram": ram.NewRamStore(),
}

func init() {
	// Create SQLite store for testing
	sqliteStore, err := sqlite_store.NewSQLiteStore(":memory:")
	if err != nil {
		panic(err)
	}
	trackStores["sqlite"] = sqliteStore

	// Create BoltDB store for testing
	boltStore, err := boltdb.NewBoltStore("test-track.db")
	if err != nil {
		panic(err)
	}
	trackStores["boltdb"] = boltStore
}

// Note: TestMain is defined in grove_test.go
// Cleanup for track test db happens in that TestMain via cleanup of test-grove.db
// We use a separate db file for track tests

func TestTrackBasicOperations(t *testing.T) {
	for name, store := range trackStores {
		testTrackBasicOperations(store, name, t)
	}
}

func testTrackBasicOperations(store store_interface.TrackStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 100, TenancyId: 1}
		bucketID := int32(1)

		// Test basic put and get
		key := "test_key_1"
		value := int64(42)
		err := store.TrackPut(space, bucketID, key, value, nil, nil)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		got, err := store.TrackGet(space, bucketID, key)
		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}
		if got != value {
			t.Errorf("Expected value %d, got %d", value, got)
		}

		// Test put with tag and metric
		key2 := "test_key_2"
		value2 := int64(100)
		tag := int64(5)
		metric := 3.14
		err = store.TrackPut(space, bucketID, key2, value2, &tag, &metric)
		if err != nil {
			t.Fatalf("Failed to put with tag/metric: %v", err)
		}

		got2, err := store.TrackGet(space, bucketID, key2)
		if err != nil {
			t.Fatalf("Failed to get key2: %v", err)
		}
		if got2 != value2 {
			t.Errorf("Expected value %d, got %d", value2, got2)
		}

		// Test overwrite
		newValue := int64(999)
		err = store.TrackPut(space, bucketID, key, newValue, nil, nil)
		if err != nil {
			t.Fatalf("Failed to overwrite: %v", err)
		}

		got3, err := store.TrackGet(space, bucketID, key)
		if err != nil {
			t.Fatalf("Failed to get after overwrite: %v", err)
		}
		if got3 != newValue {
			t.Errorf("Expected overwritten value %d, got %d", newValue, got3)
		}

		// Test get non-existent key
		_, err = store.TrackGet(space, bucketID, "non_existent_key")
		if err == nil {
			t.Error("Expected error for non-existent key, got nil")
		}

		// Test get from non-existent bucket
		_, err = store.TrackGet(space, int32(9999), "any_key")
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
		space := store_interface.TenancySpace{AppId: 101, TenancyId: 1}
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

		found, missing, err := store.TrackGetMany(space, keys)
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

		found2, missing2, err := store.TrackGetMany(space, keysWithMissing)
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
		found3, missing3, err := store.TrackGetMany(space, keysNonExistent)
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
		space := store_interface.TenancySpace{AppId: 102, TenancyId: 1}
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
		deleteItems := []model.TrackBucketKeyPair{
			{BucketID: bucketID, Key: "del1"},
			{BucketID: bucketID, Key: "del2"},
		}
		err = store.TrackDeleteMany(space, deleteItems)
		if err != nil {
			t.Fatalf("TrackDeleteMany failed: %v", err)
		}

		// Verify deleted items are gone
		_, err = store.TrackGet(space, bucketID, "del1")
		if err == nil {
			t.Error("del1 should be deleted")
		}
		_, err = store.TrackGet(space, bucketID, "del2")
		if err == nil {
			t.Error("del2 should be deleted")
		}

		// Verify kept item still exists
		val, err := store.TrackGet(space, bucketID, "keep")
		if err != nil {
			t.Fatalf("keep should still exist: %v", err)
		}
		if val != 3 {
			t.Errorf("Expected keep=3, got %d", val)
		}

		// Test deleting non-existent keys (should not error - idempotent)
		deleteNonExistent := []model.TrackBucketKeyPair{
			{BucketID: bucketID, Key: "non_existent"},
		}
		err = store.TrackDeleteMany(space, deleteNonExistent)
		if err != nil {
			t.Errorf("Deleting non-existent key should not error: %v", err)
		}

		// Test deleting from non-existent bucket (behavior may vary)
		deleteFromNonExistent := []model.TrackBucketKeyPair{
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
		space := store_interface.TenancySpace{AppId: 103, TenancyId: 1}
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
		space := store_interface.TenancySpace{AppId: 104, TenancyId: 1}
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
		space1 := store_interface.TenancySpace{AppId: 105, TenancyId: 1}
		space2 := store_interface.TenancySpace{AppId: 105, TenancyId: 2}
		space3 := store_interface.TenancySpace{AppId: 106, TenancyId: 1}
		bucketID := int32(60)

		// Put same key in different tenancy spaces
		key := "shared_key"

		err := store.TrackPut(space1, bucketID, key, 100, nil, nil)
		if err != nil {
			t.Fatalf("Put to space1 failed: %v", err)
		}
		err = store.TrackPut(space2, bucketID, key, 200, nil, nil)
		if err != nil {
			t.Fatalf("Put to space2 failed: %v", err)
		}
		err = store.TrackPut(space3, bucketID, key, 300, nil, nil)
		if err != nil {
			t.Fatalf("Put to space3 failed: %v", err)
		}

		// Verify isolation
		val1, err := store.TrackGet(space1, bucketID, key)
		if err != nil {
			t.Fatalf("Get from space1 failed: %v", err)
		}
		if val1 != 100 {
			t.Errorf("Expected space1 value 100, got %d", val1)
		}

		val2, err := store.TrackGet(space2, bucketID, key)
		if err != nil {
			t.Fatalf("Get from space2 failed: %v", err)
		}
		if val2 != 200 {
			t.Errorf("Expected space2 value 200, got %d", val2)
		}

		val3, err := store.TrackGet(space3, bucketID, key)
		if err != nil {
			t.Fatalf("Get from space3 failed: %v", err)
		}
		if val3 != 300 {
			t.Errorf("Expected space3 value 300, got %d", val3)
		}

		// Delete from space1 shouldn't affect others
		err = store.TrackDeleteMany(space1, []model.TrackBucketKeyPair{{BucketID: bucketID, Key: key}})
		if err != nil {
			t.Fatalf("Delete from space1 failed: %v", err)
		}

		_, err = store.TrackGet(space1, bucketID, key)
		if err == nil {
			t.Error("Key should be deleted from space1")
		}

		val2After, err := store.TrackGet(space2, bucketID, key)
		if err != nil {
			t.Fatalf("Get from space2 after space1 delete failed: %v", err)
		}
		if val2After != 200 {
			t.Errorf("Space2 value should still be 200, got %d", val2After)
		}

		val3After, err := store.TrackGet(space3, bucketID, key)
		if err != nil {
			t.Fatalf("Get from space3 after space1 delete failed: %v", err)
		}
		if val3After != 300 {
			t.Errorf("Space3 value should still be 300, got %d", val3After)
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
		space := store_interface.TenancySpace{AppId: 107, TenancyId: 1}
		bucket1 := int32(70)
		bucket2 := int32(71)

		// Put same key in different buckets
		key := "same_key"

		err := store.TrackPut(space, bucket1, key, 111, nil, nil)
		if err != nil {
			t.Fatalf("Put to bucket1 failed: %v", err)
		}
		err = store.TrackPut(space, bucket2, key, 222, nil, nil)
		if err != nil {
			t.Fatalf("Put to bucket2 failed: %v", err)
		}

		// Verify isolation
		val1, err := store.TrackGet(space, bucket1, key)
		if err != nil {
			t.Fatalf("Get from bucket1 failed: %v", err)
		}
		if val1 != 111 {
			t.Errorf("Expected bucket1 value 111, got %d", val1)
		}

		val2, err := store.TrackGet(space, bucket2, key)
		if err != nil {
			t.Fatalf("Get from bucket2 failed: %v", err)
		}
		if val2 != 222 {
			t.Errorf("Expected bucket2 value 222, got %d", val2)
		}

		// Delete from bucket1 shouldn't affect bucket2
		err = store.TrackDeleteMany(space, []model.TrackBucketKeyPair{{BucketID: bucket1, Key: key}})
		if err != nil {
			t.Fatalf("Delete from bucket1 failed: %v", err)
		}

		_, err = store.TrackGet(space, bucket1, key)
		if err == nil {
			t.Error("Key should be deleted from bucket1")
		}

		val2After, err := store.TrackGet(space, bucket2, key)
		if err != nil {
			t.Fatalf("Get from bucket2 after bucket1 delete failed: %v", err)
		}
		if val2After != 222 {
			t.Errorf("Bucket2 value should still be 222, got %d", val2After)
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
		space := store_interface.TenancySpace{AppId: 108, TenancyId: 1}
		bucketID := int32(80)

		// Test with max int64
		maxVal := int64(9223372036854775807)
		err := store.TrackPut(space, bucketID, "max", maxVal, nil, nil)
		if err != nil {
			t.Fatalf("Put max value failed: %v", err)
		}

		got, err := store.TrackGet(space, bucketID, "max")
		if err != nil {
			t.Fatalf("Get max value failed: %v", err)
		}
		if got != maxVal {
			t.Errorf("Expected max value %d, got %d", maxVal, got)
		}

		// Test with min int64
		minVal := int64(-9223372036854775808)
		err = store.TrackPut(space, bucketID, "min", minVal, nil, nil)
		if err != nil {
			t.Fatalf("Put min value failed: %v", err)
		}

		got, err = store.TrackGet(space, bucketID, "min")
		if err != nil {
			t.Fatalf("Get min value failed: %v", err)
		}
		if got != minVal {
			t.Errorf("Expected min value %d, got %d", minVal, got)
		}

		// Test with zero
		err = store.TrackPut(space, bucketID, "zero", 0, nil, nil)
		if err != nil {
			t.Fatalf("Put zero failed: %v", err)
		}

		got, err = store.TrackGet(space, bucketID, "zero")
		if err != nil {
			t.Fatalf("Get zero failed: %v", err)
		}
		if got != 0 {
			t.Errorf("Expected 0, got %d", got)
		}
	})
}
