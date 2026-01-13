package migrator

import (
	"testing"

	"github.com/vixac/bullet/store/ram"
	"github.com/vixac/bullet/store/store_interface"
)

var testTenancy = store_interface.TenancySpace{
	AppId:     1,
	TenancyId: 100,
}

func TestTrackMigrator(t *testing.T) {
	source := ram.NewRamStore()
	target := ram.NewRamStore()

	// Populate source with test data
	bucketID := int32(1)
	tag1 := int64(10)
	tag2 := int64(20)
	metric1 := float64(1.5)
	metric2 := float64(2.5)

	// Add some test items
	err := source.TrackPut(testTenancy, bucketID, "key1", 100, &tag1, &metric1)
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}
	err = source.TrackPut(testTenancy, bucketID, "key2", 200, &tag2, &metric2)
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}
	err = source.TrackPut(testTenancy, bucketID, "key3", 300, nil, nil)
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}

	// Create migrator
	migrator := &TrackMigrator{
		SourceTrack: source,
		TargetTrack: target,
		Tenancy:     testTenancy,
	}

	// Migrate the bucket
	err = migrator.Migrate(bucketID)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify data in target
	val1, err := target.TrackGet(testTenancy, bucketID, "key1")
	if err != nil {
		t.Fatalf("Failed to get key1 from target: %v", err)
	}
	if val1 != 100 {
		t.Errorf("Expected value 100, got %d", val1)
	}

	val2, err := target.TrackGet(testTenancy, bucketID, "key2")
	if err != nil {
		t.Fatalf("Failed to get key2 from target: %v", err)
	}
	if val2 != 200 {
		t.Errorf("Expected value 200, got %d", val2)
	}

	val3, err := target.TrackGet(testTenancy, bucketID, "key3")
	if err != nil {
		t.Fatalf("Failed to get key3 from target: %v", err)
	}
	if val3 != 300 {
		t.Errorf("Expected value 300, got %d", val3)
	}

	// Verify with GetItemsByKeyPrefix to check tags and metrics
	items, err := target.GetItemsByKeyPrefix(testTenancy, bucketID, "", []int64{}, nil, true)
	if err != nil {
		t.Fatalf("Failed to get items by prefix: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(items))
	}

	// Check that tags and metrics are preserved
	for _, item := range items {
		if item.Key == "key1" {
			if item.Value.Tag == nil || *item.Value.Tag != tag1 {
				t.Errorf("Tag not preserved for key1")
			}
			if item.Value.Metric == nil || *item.Value.Metric != metric1 {
				t.Errorf("Metric not preserved for key1")
			}
		}
	}
}

func TestDepotMigrator(t *testing.T) {
	source := ram.NewRamStore()
	target := ram.NewRamStore()

	// Populate source with test data
	keys := []int64{1, 2, 3}
	err := source.DepotPut(testTenancy, keys[0], "value1")
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}
	err = source.DepotPut(testTenancy, keys[1], "value2")
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}
	err = source.DepotPut(testTenancy, keys[2], "value3")
	if err != nil {
		t.Fatalf("Failed to put test data: %v", err)
	}

	// Create migrator
	migrator := &DepotMigrator{
		SourceDepot: source,
		TargetDepot: target,
		Tenancy:     testTenancy,
	}

	// Migrate the keys
	err = migrator.Migrate(keys)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify data in target
	val1, err := target.DepotGet(testTenancy, keys[0])
	if err != nil {
		t.Fatalf("Failed to get key 1 from target: %v", err)
	}
	if val1 != "value1" {
		t.Errorf("Expected value1, got %s", val1)
	}

	val2, err := target.DepotGet(testTenancy, keys[1])
	if err != nil {
		t.Fatalf("Failed to get key 2 from target: %v", err)
	}
	if val2 != "value2" {
		t.Errorf("Expected value2, got %s", val2)
	}

	val3, err := target.DepotGet(testTenancy, keys[2])
	if err != nil {
		t.Fatalf("Failed to get key 3 from target: %v", err)
	}
	if val3 != "value3" {
		t.Errorf("Expected value3, got %s", val3)
	}
}

func TestGroveMigrator(t *testing.T) {
	source := ram.NewRamStore()
	target := ram.NewRamStore()

	treeID := store_interface.TreeID("test-tree")
	root := store_interface.NodeID("root")
	child1 := store_interface.NodeID("child1")
	child2 := store_interface.NodeID("child2")
	grandchild := store_interface.NodeID("grandchild")

	pos1 := store_interface.ChildPosition(1.0)
	pos2 := store_interface.ChildPosition(2.0)
	pos3 := store_interface.ChildPosition(1.0)

	metadata1 := store_interface.NodeMetadata{"name": "root node"}
	metadata2 := store_interface.NodeMetadata{"name": "child 1"}

	// Build a tree structure in source
	// root
	//   - child1
	//     - grandchild
	//   - child2
	err := source.CreateNode(testTenancy, treeID, root, nil, nil, &metadata1)
	if err != nil {
		t.Fatalf("Failed to create root: %v", err)
	}

	err = source.CreateNode(testTenancy, treeID, child1, &root, &pos1, &metadata2)
	if err != nil {
		t.Fatalf("Failed to create child1: %v", err)
	}

	err = source.CreateNode(testTenancy, treeID, child2, &root, &pos2, nil)
	if err != nil {
		t.Fatalf("Failed to create child2: %v", err)
	}

	err = source.CreateNode(testTenancy, treeID, grandchild, &child1, &pos3, nil)
	if err != nil {
		t.Fatalf("Failed to create grandchild: %v", err)
	}

	// Create migrator
	migrator := &GroveMigrator{
		SourceGrove: source,
		TargetGrove: target,
		Tenancy:     testTenancy,
	}

	// Migrate the tree
	err = migrator.MigrateTree(treeID, root)
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Verify tree structure in target
	// Check root exists
	exists, err := target.Exists(testTenancy, treeID, root)
	if err != nil || !exists {
		t.Fatalf("Root node not found in target")
	}

	// Check root info
	rootInfo, err := target.GetNodeInfo(testTenancy, treeID, root)
	if err != nil {
		t.Fatalf("Failed to get root info: %v", err)
	}
	if rootInfo.Parent != nil {
		t.Errorf("Root should have no parent")
	}
	if rootInfo.Metadata == nil || (*rootInfo.Metadata)["name"] != "root node" {
		t.Errorf("Root metadata not preserved")
	}

	// Check children of root
	children, _, err := target.GetChildren(testTenancy, treeID, root, nil)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}
	if len(children) != 2 {
		t.Errorf("Expected 2 children of root, got %d", len(children))
	}

	// Check child1 exists and has correct parent
	child1Info, err := target.GetNodeInfo(testTenancy, treeID, child1)
	if err != nil {
		t.Fatalf("Failed to get child1 info: %v", err)
	}
	if child1Info.Parent == nil || *child1Info.Parent != root {
		t.Errorf("Child1 parent not correct")
	}
	if child1Info.Position == nil || *child1Info.Position != pos1 {
		t.Errorf("Child1 position not preserved")
	}
	if child1Info.Metadata == nil || (*child1Info.Metadata)["name"] != "child 1" {
		t.Errorf("Child1 metadata not preserved")
	}

	// Check grandchild exists and has correct parent
	grandchildInfo, err := target.GetNodeInfo(testTenancy, treeID, grandchild)
	if err != nil {
		t.Fatalf("Failed to get grandchild info: %v", err)
	}
	if grandchildInfo.Parent == nil || *grandchildInfo.Parent != child1 {
		t.Errorf("Grandchild parent not correct")
	}

	// Check depth is correct (grandchild should be at depth 2)
	if grandchildInfo.Depth != 2 {
		t.Errorf("Expected grandchild depth 2, got %d", grandchildInfo.Depth)
	}
}

func TestTrackMigratorEmptyBucket(t *testing.T) {
	source := ram.NewRamStore()
	target := ram.NewRamStore()

	migrator := &TrackMigrator{
		SourceTrack: source,
		TargetTrack: target,
		Tenancy:     testTenancy,
	}

	// Migrate empty bucket should not error
	err := migrator.Migrate(999)
	if err != nil {
		t.Fatalf("Migration of empty bucket should not error: %v", err)
	}
}

func TestDepotMigratorEmptyKeys(t *testing.T) {
	source := ram.NewRamStore()
	target := ram.NewRamStore()

	migrator := &DepotMigrator{
		SourceDepot: source,
		TargetDepot: target,
		Tenancy:     testTenancy,
	}

	// Migrate empty keys should not error
	err := migrator.Migrate([]int64{})
	if err != nil {
		t.Fatalf("Migration of empty keys should not error: %v", err)
	}
}
