package store_test

import (
	"sort"
	"testing"

	sqlite_store "github.com/vixac/bullet/store/sqlite"
	"github.com/vixac/bullet/store/ram"
	"github.com/vixac/bullet/store/store_interface"
)

// depotStores contains all DepotStore implementations to test
var depotStores = map[string]store_interface.DepotStore{
	"ram": ram.NewRamStore(),
}

func init() {
	sqliteStore, err := sqlite_store.NewSQLiteStore(":memory:")
	if err != nil {
		panic(err)
	}
	depotStores["sqlite"] = sqliteStore
}

// Note: TestMain is defined in grove_test.go and handles cleanup

func TestDepotCreateAndGet(t *testing.T) {
	for name, store := range depotStores {
		testDepotCreateAndGet(store, name, t)
	}
}

func testDepotCreateAndGet(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 200, TenancyId: 1}
		const bucket = int32(1)

		id, err := store.DepotCreate(space, bucket, "hello")
		if err != nil {
			t.Fatalf("DepotCreate failed: %v", err)
		}
		if id == 0 {
			t.Fatal("expected non-zero id from DepotCreate")
		}

		got, err := store.DepotGet(space, id)
		if err != nil {
			t.Fatalf("DepotGet failed: %v", err)
		}
		if got != "hello" {
			t.Errorf("expected 'hello', got %q", got)
		}

		// IDs should be unique across sequential creates
		id2, err := store.DepotCreate(space, bucket, "world")
		if err != nil {
			t.Fatalf("second DepotCreate failed: %v", err)
		}
		if id2 == id {
			t.Error("expected different id for second create")
		}

		// Get non-existent id
		_, err = store.DepotGet(space, id+99999)
		if err == nil {
			t.Error("expected error for non-existent id")
		}
	})
}

func TestDepotCreateMany(t *testing.T) {
	for name, store := range depotStores {
		testDepotCreateMany(store, name, t)
	}
}

func testDepotCreateMany(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 201, TenancyId: 1}
		const bucket = int32(10)

		values := []string{"a", "b", "c"}
		ids, err := store.DepotCreateMany(space, bucket, values)
		if err != nil {
			t.Fatalf("DepotCreateMany failed: %v", err)
		}
		if len(ids) != len(values) {
			t.Fatalf("expected %d ids, got %d", len(values), len(ids))
		}

		// All ids must be unique
		seen := make(map[int64]bool)
		for _, id := range ids {
			if seen[id] {
				t.Errorf("duplicate id %d", id)
			}
			seen[id] = true
		}

		// Verify values
		for i, id := range ids {
			got, err := store.DepotGet(space, id)
			if err != nil {
				t.Fatalf("DepotGet(%d) failed: %v", id, err)
			}
			if got != values[i] {
				t.Errorf("id %d: expected %q, got %q", id, values[i], got)
			}
		}
	})
}

func TestDepotUpdate(t *testing.T) {
	for name, store := range depotStores {
		testDepotUpdate(store, name, t)
	}
}

func testDepotUpdate(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 202, TenancyId: 1}
		const bucket = int32(1)

		id, err := store.DepotCreate(space, bucket, "original")
		if err != nil {
			t.Fatalf("DepotCreate failed: %v", err)
		}

		err = store.DepotUpdate(space, id, "updated")
		if err != nil {
			t.Fatalf("DepotUpdate failed: %v", err)
		}

		got, err := store.DepotGet(space, id)
		if err != nil {
			t.Fatalf("DepotGet after update failed: %v", err)
		}
		if got != "updated" {
			t.Errorf("expected 'updated', got %q", got)
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
		space := store_interface.TenancySpace{AppId: 203, TenancyId: 1}
		const bucket = int32(1)

		id, err := store.DepotCreate(space, bucket, "to_delete")
		if err != nil {
			t.Fatalf("DepotCreate failed: %v", err)
		}

		err = store.DepotDelete(space, id)
		if err != nil {
			t.Fatalf("DepotDelete failed: %v", err)
		}

		_, err = store.DepotGet(space, id)
		if err == nil {
			t.Error("expected error after delete, got nil")
		}

		// Deleting non-existent id should be idempotent
		err = store.DepotDelete(space, id)
		if err != nil {
			t.Errorf("deleting non-existent id should not error: %v", err)
		}
	})
}

func TestDepotDeleteByBucket(t *testing.T) {
	for name, store := range depotStores {
		testDepotDeleteByBucket(store, name, t)
	}
}

func testDepotDeleteByBucket(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 204, TenancyId: 1}
		const bucketA = int32(100)
		const bucketB = int32(200)

		idA1, _ := store.DepotCreate(space, bucketA, "a1")
		idA2, _ := store.DepotCreate(space, bucketA, "a2")
		idB1, _ := store.DepotCreate(space, bucketB, "b1")

		err := store.DepotDeleteByBucket(space, bucketA)
		if err != nil {
			t.Fatalf("DepotDeleteByBucket failed: %v", err)
		}

		// Bucket A items should be gone
		_, err = store.DepotGet(space, idA1)
		if err == nil {
			t.Error("idA1 should be deleted")
		}
		_, err = store.DepotGet(space, idA2)
		if err == nil {
			t.Error("idA2 should be deleted")
		}

		// Bucket B item should still exist
		got, err := store.DepotGet(space, idB1)
		if err != nil {
			t.Fatalf("idB1 should still exist: %v", err)
		}
		if got != "b1" {
			t.Errorf("expected 'b1', got %q", got)
		}

		// Deleting an already-empty bucket should be idempotent
		err = store.DepotDeleteByBucket(space, bucketA)
		if err != nil {
			t.Errorf("deleting empty bucket should not error: %v", err)
		}
	})
}

func TestDepotGetAllByBucket(t *testing.T) {
	for name, store := range depotStores {
		testDepotGetAllByBucket(store, name, t)
	}
}

func testDepotGetAllByBucket(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 205, TenancyId: 1}
		const bucketA = int32(300)
		const bucketB = int32(400)

		idA1, _ := store.DepotCreate(space, bucketA, "alpha")
		idA2, _ := store.DepotCreate(space, bucketA, "beta")
		_, _ = store.DepotCreate(space, bucketB, "gamma")

		all, err := store.DepotGetAllByBucket(space, bucketA)
		if err != nil {
			t.Fatalf("DepotGetAllByBucket failed: %v", err)
		}
		if len(all) != 2 {
			t.Fatalf("expected 2 items in bucketA, got %d", len(all))
		}
		if all[idA1] != "alpha" {
			t.Errorf("expected 'alpha' for idA1, got %q", all[idA1])
		}
		if all[idA2] != "beta" {
			t.Errorf("expected 'beta' for idA2, got %q", all[idA2])
		}

		// Empty bucket
		emptyResult, err := store.DepotGetAllByBucket(space, int32(9999))
		if err != nil {
			t.Fatalf("DepotGetAllByBucket on empty bucket failed: %v", err)
		}
		if len(emptyResult) != 0 {
			t.Errorf("expected 0 items for empty bucket, got %d", len(emptyResult))
		}
	})
}

func TestDepotGetMany(t *testing.T) {
	for name, store := range depotStores {
		testDepotGetMany(store, name, t)
	}
}

func testDepotGetMany(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 206, TenancyId: 1}
		const bucket = int32(1)

		ids, err := store.DepotCreateMany(space, bucket, []string{"x", "y", "z"})
		if err != nil {
			t.Fatalf("DepotCreateMany failed: %v", err)
		}

		// All found
		found, missing, err := store.DepotGetMany(space, ids)
		if err != nil {
			t.Fatalf("DepotGetMany failed: %v", err)
		}
		if len(found) != 3 {
			t.Errorf("expected 3 found, got %d", len(found))
		}
		if len(missing) != 0 {
			t.Errorf("expected 0 missing, got %v", missing)
		}

		// Mix of found and missing
		bogusID := ids[len(ids)-1] + 99999
		queryIDs := []int64{ids[0], bogusID}
		found, missing, err = store.DepotGetMany(space, queryIDs)
		if err != nil {
			t.Fatalf("DepotGetMany with missing failed: %v", err)
		}
		if len(found) != 1 {
			t.Errorf("expected 1 found, got %d", len(found))
		}
		if len(missing) != 1 || missing[0] != bogusID {
			t.Errorf("expected missing=[%d], got %v", bogusID, missing)
		}

		// Empty query
		found, missing, err = store.DepotGetMany(space, []int64{})
		if err != nil {
			t.Fatalf("DepotGetMany empty failed: %v", err)
		}
		if len(found) != 0 {
			t.Errorf("expected 0 found for empty query, got %d", len(found))
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
		space1 := store_interface.TenancySpace{AppId: 207, TenancyId: 1}
		space2 := store_interface.TenancySpace{AppId: 207, TenancyId: 2}
		space3 := store_interface.TenancySpace{AppId: 208, TenancyId: 1}
		const bucket = int32(1)

		id1, _ := store.DepotCreate(space1, bucket, "space1_value")
		// Create an extra item in space1 so its highest id is beyond space2's range
		id1Extra, _ := store.DepotCreate(space1, bucket, "space1_extra")
		id2, _ := store.DepotCreate(space2, bucket, "space2_value")
		id3, _ := store.DepotCreate(space3, bucket, "space3_value")

		val1, err := store.DepotGet(space1, id1)
		if err != nil || val1 != "space1_value" {
			t.Errorf("space1: expected 'space1_value', got %q (err=%v)", val1, err)
		}
		val2, err := store.DepotGet(space2, id2)
		if err != nil || val2 != "space2_value" {
			t.Errorf("space2: expected 'space2_value', got %q (err=%v)", val2, err)
		}
		val3, err := store.DepotGet(space3, id3)
		if err != nil || val3 != "space3_value" {
			t.Errorf("space3: expected 'space3_value', got %q (err=%v)", val3, err)
		}

		// id1Extra is space1's second item; space2 only has one item so this id is not in space2
		_, err = store.DepotGet(space2, id1Extra)
		if err == nil {
			t.Error("space2 should not be able to access space1's extra item")
		}

		// Delete from space1 shouldn't affect space2 or space3
		_ = store.DepotDelete(space1, id1)
		_, err = store.DepotGet(space1, id1)
		if err == nil {
			t.Error("id1 should be gone from space1")
		}
		val2After, err := store.DepotGet(space2, id2)
		if err != nil || val2After != "space2_value" {
			t.Errorf("space2 value should be unaffected: got %q (err=%v)", val2After, err)
		}
		val3After, err := store.DepotGet(space3, id3)
		if err != nil || val3After != "space3_value" {
			t.Errorf("space3 value should be unaffected: got %q (err=%v)", val3After, err)
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
		space := store_interface.TenancySpace{AppId: 209, TenancyId: 1}
		const bucket = int32(1)

		largeValue := make([]byte, 1024*1024)
		for i := range largeValue {
			largeValue[i] = byte('a' + (i % 26))
		}

		id, err := store.DepotCreate(space, bucket, string(largeValue))
		if err != nil {
			t.Fatalf("DepotCreate large value failed: %v", err)
		}

		got, err := store.DepotGet(space, id)
		if err != nil {
			t.Fatalf("DepotGet large value failed: %v", err)
		}
		if got != string(largeValue) {
			t.Error("large value content mismatch")
		}

		unicodeValue := "Hello 世界 🌍 مرحبا שלום"
		id2, err := store.DepotCreate(space, bucket, unicodeValue)
		if err != nil {
			t.Fatalf("DepotCreate unicode failed: %v", err)
		}
		got2, err := store.DepotGet(space, id2)
		if err != nil {
			t.Fatalf("DepotGet unicode failed: %v", err)
		}
		if got2 != unicodeValue {
			t.Errorf("expected %q, got %q", unicodeValue, got2)
		}
	})
}

func TestDepotBucketIsolationInGetAll(t *testing.T) {
	for name, store := range depotStores {
		testDepotBucketIsolationInGetAll(store, name, t)
	}
}

func testDepotBucketIsolationInGetAll(store store_interface.DepotStore, name string, t *testing.T) {
	t.Run(name, func(t *testing.T) {
		space := store_interface.TenancySpace{AppId: 210, TenancyId: 1}
		const bucketA = int32(500)
		const bucketB = int32(501)

		valuesA := []string{"a1", "a2", "a3"}
		valuesB := []string{"b1", "b2"}

		idsA, _ := store.DepotCreateMany(space, bucketA, valuesA)
		_, _ = store.DepotCreateMany(space, bucketB, valuesB)

		allA, err := store.DepotGetAllByBucket(space, bucketA)
		if err != nil {
			t.Fatalf("DepotGetAllByBucket bucketA failed: %v", err)
		}
		if len(allA) != 3 {
			t.Errorf("expected 3 in bucketA, got %d", len(allA))
		}

		allB, err := store.DepotGetAllByBucket(space, bucketB)
		if err != nil {
			t.Fatalf("DepotGetAllByBucket bucketB failed: %v", err)
		}
		if len(allB) != 2 {
			t.Errorf("expected 2 in bucketB, got %d", len(allB))
		}

		// Verify idsA are present in allA
		sort.Slice(idsA, func(i, j int) bool { return idsA[i] < idsA[j] })
		for _, id := range idsA {
			if _, ok := allA[id]; !ok {
				t.Errorf("expected id %d in bucketA results", id)
			}
		}
	})
}
