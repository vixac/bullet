package migrator

import (
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

//the idea here is that you take a read track and you write the equivalent to the the writet rack

type TrackMigrator struct {
	SourceTrack store_interface.TrackStore
	TargetTrack store_interface.TrackStore
	Tenancy     store_interface.TenancySpace
}

func (t *TrackMigrator) Migrate(bucketId int32) error {
	// Fetch all items from the source bucket (empty prefix gets everything)
	items, err := t.SourceTrack.GetItemsByKeyPrefix(t.Tenancy, bucketId, "", []int64{}, nil, true)
	if err != nil {
		return fmt.Errorf("failed to fetch items from source bucket %d: %w", bucketId, err)
	}

	// Nothing to migrate
	if len(items) == 0 {
		fmt.Printf("Bucket %d: no items to migrate\n", bucketId)
		return nil
	}

	// Package items for TrackPutMany
	itemsMap := map[int32][]model.TrackKeyValueItem{
		bucketId: items,
	}

	// Write to target store
	err = t.TargetTrack.TrackPutMany(t.Tenancy, itemsMap)
	if err != nil {
		return fmt.Errorf("failed to write items to target bucket %d: %w", bucketId, err)
	}

	fmt.Printf("Bucket %d: successfully migrated %d items\n", bucketId, len(items))
	return nil
}
