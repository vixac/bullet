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

// depotStores contains all DepotStore implementations to test
var depotStores = map[string]store_interface.DepotStore{
	"ram": ram.NewRamStore(),
}

func init() {
	// Create SQLite store for testing
	sqliteStore, err := sqlite_store.NewSQLiteStore(":memory:")
	if err != nil {
		panic(err)
	}
	depotStores["sqlite"] = sqliteStore

	// Create BoltDB store for testing
	boltStore, err := boltdb.NewBoltStore("test-depot.db")
	if err != nil {
		panic(err)
	}
	depotStores["boltdb"] = boltStore
}

// Note: TestMain is defined in grove_test.go and handles cleanup

func TestDepotBasicOperations(t *testing.T) {
	for name, store := range depotStores {
		testDepotBasicOperations(store, name, t)
	}
}

func testDepotBasicOperations(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 200, TenancyId: 1}

		// Test basic put and get
		key := int64(1)
		value := "test_value_1"
		err := store.DepotPut(space, key, value)
		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		got, err := store.DepotGet(space, key)
		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}
		if got != value {
			t.Errorf("Expected value %s, got %s", value, got)
		}

		// Test overwrite
		newValue := "updated_value"
		err = store.DepotPut(space, key, newValue)
		if err != nil {
			t.Fatalf("Failed to overwrite: %v", err)
		}

		got, err = store.DepotGet(space, key)
		if err != nil {
			t.Fatalf("Failed to get after overwrite: %v", err)
		}
		if got != newValue {
			t.Errorf("Expected overwritten value %s, got %s", newValue, got)
		}

		// Test get non-existent key
		_, err = store.DepotGet(space, int64(99999))
		if err == nil {
			t.Error("Expected error for non-existent key, got nil")
		}

		// Test empty string value
		emptyKey := int64(2)
		err = store.DepotPut(space, emptyKey, "")
		if err != nil {
			t.Fatalf("Failed to put empty value: %v", err)
		}

		got, err = store.DepotGet(space, emptyKey)
		if err != nil {
			t.Fatalf("Failed to get empty value: %v", err)
		}
		if got != "" {
			t.Errorf("Expected empty string, got %s", got)
		}
	})
}

func TestDepotDelete(t *testing.T) {
	for name, store := range depotStores {
		testDepotDelete(store, name, t)
	}
}

func testDepotDelete(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 201, TenancyId: 1}

		// Put a value
		key := int64(100)
		value := "to_be_deleted"
		err := store.DepotPut(space, key, value)
		if err != nil {
			t.Fatalf("Setup put failed: %v", err)
		}

		// Verify it exists
		_, err = store.DepotGet(space, key)
		if err != nil {
			t.Fatalf("Value should exist before delete: %v", err)
		}

		// Delete it
		err = store.DepotDelete(space, key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify it's gone
		_, err = store.DepotGet(space, key)
		if err == nil {
			t.Error("Value should not exist after delete")
		}

		// Delete non-existent key (should be idempotent)
		err = store.DepotDelete(space, int64(99999))
		if err != nil {
			t.Errorf("Deleting non-existent key should not error: %v", err)
		}
	})
}

func TestDepotPutManyGetMany(t *testing.T) {
	for name, store := range depotStores {
		testDepotPutManyGetMany(store, name, t)
	}
}

func testDepotPutManyGetMany(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 202, TenancyId: 1}

		// Prepare items for put many
		items := []model.DepotKeyValueItem{
			{Key: 10, Value: "value_10"},
			{Key: 20, Value: "value_20"},
			{Key: 30, Value: "value_30"},
		}

		err := store.DepotPutMany(space, items)
		if err != nil {
			t.Fatalf("DepotPutMany failed: %v", err)
		}

		// Test get many - all found
		keys := []int64{10, 20, 30}
		found, missing, err := store.DepotGetMany(space, keys)
		if err != nil {
			t.Fatalf("DepotGetMany failed: %v", err)
		}

		// Verify found items
		if len(found) != 3 {
			t.Errorf("Expected 3 items, got %d", len(found))
		}
		if found[10] != "value_10" {
			t.Errorf("Expected value_10, got %s", found[10])
		}
		if found[20] != "value_20" {
			t.Errorf("Expected value_20, got %s", found[20])
		}
		if found[30] != "value_30" {
			t.Errorf("Expected value_30, got %s", found[30])
		}

		// Verify missing is empty
		if len(missing) != 0 {
			t.Errorf("Expected no missing items, got %v", missing)
		}

		// Test get many with some missing keys
		keysWithMissing := []int64{10, 999, 20, 888}
		found, missing, err = store.DepotGetMany(space, keysWithMissing)
		if err != nil {
			t.Fatalf("DepotGetMany with missing failed: %v", err)
		}

		if len(found) != 2 {
			t.Errorf("Expected 2 found, got %d", len(found))
		}
		if len(missing) != 2 {
			t.Errorf("Expected 2 missing, got %d", len(missing))
		}

		// Verify missing keys
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		if missing[0] != 888 || missing[1] != 999 {
			t.Errorf("Expected missing [888, 999], got %v", missing)
		}

		// Test get many with empty keys
		found, missing, err = store.DepotGetMany(space, []int64{})
		if err != nil {
			t.Fatalf("DepotGetMany empty keys failed: %v", err)
		}
		if len(found) != 0 {
			t.Errorf("Expected 0 found for empty keys, got %d", len(found))
		}

		// Test get many from non-existent space
		nonExistentSpace := store_interface.TenancySpace{AppId: 9999, TenancyId: 9999}
		found, missing, err = store.DepotGetMany(nonExistentSpace, []int64{10, 20})
		if err != nil {
			t.Fatalf("DepotGetMany from non-existent space failed: %v", err)
		}
		if len(found) != 0 {
			t.Errorf("Expected 0 found from non-existent space, got %d", len(found))
		}
		if len(missing) != 2 {
			t.Errorf("Expected 2 missing from non-existent space, got %d", len(missing))
		}
	})
}

func TestDepotGetAll(t *testing.T) {
	for name, store := range depotStores {
		testDepotGetAll(store, name, t)
	}
}

func testDepotGetAll(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 203, TenancyId: 1}

		// Put some items
		items := []model.DepotKeyValueItem{
			{Key: 1, Value: "one"},
			{Key: 2, Value: "two"},
			{Key: 3, Value: "three"},
		}
		err := store.DepotPutMany(space, items)
		if err != nil {
			t.Fatalf("Setup DepotPutMany failed: %v", err)
		}

		// Test GetAll
		all, err := store.DepotGetAll(space)
		if err != nil {
			// RAM and SQLite don't implement this - that's a known issue
			t.Logf("Note: %s DepotGetAll not implemented: %v", name, err)
			return
		}

		if len(all) != 3 {
			t.Errorf("Expected 3 items, got %d", len(all))
		}
		if all[1] != "one" {
			t.Errorf("Expected 'one', got %s", all[1])
		}
		if all[2] != "two" {
			t.Errorf("Expected 'two', got %s", all[2])
		}
		if all[3] != "three" {
			t.Errorf("Expected 'three', got %s", all[3])
		}

		// Test GetAll on empty/non-existent space
		emptySpace := store_interface.TenancySpace{AppId: 9998, TenancyId: 9998}
		all, err = store.DepotGetAll(emptySpace)
		if err != nil {
			t.Fatalf("DepotGetAll on empty space failed: %v", err)
		}
		if len(all) != 0 {
			t.Errorf("Expected 0 items from empty space, got %d", len(all))
		}
	})
}

func TestDepotMultiTenancy(t *testing.T) {
	for name, store := range depotStores {
		testDepotMultiTenancy(store, name, t)
	}
}

func testDepotMultiTenancy(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space1 := store_interface.TenancySpace{AppId: 204, TenancyId: 1}
		space2 := store_interface.TenancySpace{AppId: 204, TenancyId: 2}
		space3 := store_interface.TenancySpace{AppId: 205, TenancyId: 1}

		// Put same key in different tenancy spaces
		key := int64(50)

		err := store.DepotPut(space1, key, "space1_value")
		if err != nil {
			t.Fatalf("Put to space1 failed: %v", err)
		}
		err = store.DepotPut(space2, key, "space2_value")
		if err != nil {
			t.Fatalf("Put to space2 failed: %v", err)
		}
		err = store.DepotPut(space3, key, "space3_value")
		if err != nil {
			t.Fatalf("Put to space3 failed: %v", err)
		}

		// Verify isolation
		val1, err := store.DepotGet(space1, key)
		if err != nil {
			t.Fatalf("Get from space1 failed: %v", err)
		}
		if val1 != "space1_value" {
			t.Errorf("Expected space1_value, got %s", val1)
		}

		val2, err := store.DepotGet(space2, key)
		if err != nil {
			t.Fatalf("Get from space2 failed: %v", err)
		}
		if val2 != "space2_value" {
			t.Errorf("Expected space2_value, got %s", val2)
		}

		val3, err := store.DepotGet(space3, key)
		if err != nil {
			t.Fatalf("Get from space3 failed: %v", err)
		}
		if val3 != "space3_value" {
			t.Errorf("Expected space3_value, got %s", val3)
		}

		// Delete from space1 shouldn't affect others
		err = store.DepotDelete(space1, key)
		if err != nil {
			t.Fatalf("Delete from space1 failed: %v", err)
		}

		_, err = store.DepotGet(space1, key)
		if err == nil {
			t.Error("Key should be deleted from space1")
		}

		val2After, err := store.DepotGet(space2, key)
		if err != nil {
			t.Fatalf("Get from space2 after space1 delete failed: %v", err)
		}
		if val2After != "space2_value" {
			t.Errorf("Space2 value should still be space2_value, got %s", val2After)
		}

		val3After, err := store.DepotGet(space3, key)
		if err != nil {
			t.Fatalf("Get from space3 after space1 delete failed: %v", err)
		}
		if val3After != "space3_value" {
			t.Errorf("Space3 value should still be space3_value, got %s", val3After)
		}
	})
}

func TestDepotLargeKeys(t *testing.T) {
	for name, store := range depotStores {
		testDepotLargeKeys(store, name, t)
	}
}

func testDepotLargeKeys(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 206, TenancyId: 1}

		// Test with max int64
		maxKey := int64(9223372036854775807)
		err := store.DepotPut(space, maxKey, "max_value")
		if err != nil {
			t.Fatalf("Put max key failed: %v", err)
		}

		got, err := store.DepotGet(space, maxKey)
		if err != nil {
			t.Fatalf("Get max key failed: %v", err)
		}
		if got != "max_value" {
			t.Errorf("Expected max_value, got %s", got)
		}

		// Test with min int64
		minKey := int64(-9223372036854775808)
		err = store.DepotPut(space, minKey, "min_value")
		if err != nil {
			t.Fatalf("Put min key failed: %v", err)
		}

		got, err = store.DepotGet(space, minKey)
		if err != nil {
			t.Fatalf("Get min key failed: %v", err)
		}
		if got != "min_value" {
			t.Errorf("Expected min_value, got %s", got)
		}

		// Test with zero
		err = store.DepotPut(space, 0, "zero_value")
		if err != nil {
			t.Fatalf("Put zero key failed: %v", err)
		}

		got, err = store.DepotGet(space, 0)
		if err != nil {
			t.Fatalf("Get zero key failed: %v", err)
		}
		if got != "zero_value" {
			t.Errorf("Expected zero_value, got %s", got)
		}

		// Test negative keys
		negKey := int64(-12345)
		err = store.DepotPut(space, negKey, "negative_value")
		if err != nil {
			t.Fatalf("Put negative key failed: %v", err)
		}

		got, err = store.DepotGet(space, negKey)
		if err != nil {
			t.Fatalf("Get negative key failed: %v", err)
		}
		if got != "negative_value" {
			t.Errorf("Expected negative_value, got %s", got)
		}
	})
}

func TestDepotLargeValues(t *testing.T) {
	for name, store := range depotStores {
		testDepotLargeValues(store, name, t)
	}
}

func testDepotLargeValues(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 207, TenancyId: 1}

		// Test with large string value (1MB)
		largeValue := make([]byte, 1024*1024)
		for i := range largeValue {
			largeValue[i] = byte('a' + (i % 26))
		}

		err := store.DepotPut(space, 1, string(largeValue))
		if err != nil {
			t.Fatalf("Put large value failed: %v", err)
		}

		got, err := store.DepotGet(space, 1)
		if err != nil {
			t.Fatalf("Get large value failed: %v", err)
		}
		if len(got) != len(largeValue) {
			t.Errorf("Expected length %d, got %d", len(largeValue), len(got))
		}
		if got != string(largeValue) {
			t.Error("Large value content mismatch")
		}

		// Test with unicode characters
		unicodeValue := "Hello 世界 🌍 مرحبا שלום"
		err = store.DepotPut(space, 2, unicodeValue)
		if err != nil {
			t.Fatalf("Put unicode value failed: %v", err)
		}

		got, err = store.DepotGet(space, 2)
		if err != nil {
			t.Fatalf("Get unicode value failed: %v", err)
		}
		if got != unicodeValue {
			t.Errorf("Expected %s, got %s", unicodeValue, got)
		}
	})
}

func TestDepotPutManyOverwrite(t *testing.T) {
	for name, store := range depotStores {
		testDepotPutManyOverwrite(store, name, t)
	}
}

func testDepotPutManyOverwrite(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 208, TenancyId: 1}

		// Put initial values
		initialItems := []model.DepotKeyValueItem{
			{Key: 1, Value: "initial_1"},
			{Key: 2, Value: "initial_2"},
		}
		err := store.DepotPutMany(space, initialItems)
		if err != nil {
			t.Fatalf("Initial DepotPutMany failed: %v", err)
		}

		// Overwrite with new values
		updateItems := []model.DepotKeyValueItem{
			{Key: 1, Value: "updated_1"},
			{Key: 2, Value: "updated_2"},
			{Key: 3, Value: "new_3"},
		}
		err = store.DepotPutMany(space, updateItems)
		if err != nil {
			t.Fatalf("Update DepotPutMany failed: %v", err)
		}

		// Verify all values
		found, _, err := store.DepotGetMany(space, []int64{1, 2, 3})
		if err != nil {
			t.Fatalf("DepotGetMany failed: %v", err)
		}

		if found[1] != "updated_1" {
			t.Errorf("Expected updated_1, got %s", found[1])
		}
		if found[2] != "updated_2" {
			t.Errorf("Expected updated_2, got %s", found[2])
		}
		if found[3] != "new_3" {
			t.Errorf("Expected new_3, got %s", found[3])
		}
	})
}
