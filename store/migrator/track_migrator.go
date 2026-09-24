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
	Tenancy     model.TenancySpace
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
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i] = item.Key
	}
	values, missing, err := t.SourceTrack.TrackGetMany(
		t.Tenancy,
		map[int32][]string{bucketId: keys},
		model.TrackReadOptions{IncludePayload: true},
	)
	if err != nil {
		return fmt.Errorf("failed to fetch complete items from source bucket %d: %w", bucketId, err)
	}
	if len(missing[bucketId]) > 0 {
		return fmt.Errorf("source bucket %d changed during migration; %d enumerated keys disappeared", bucketId, len(missing[bucketId]))
	}
	items = items[:0]
	for _, key := range keys {
		items = append(items, model.TrackKeyValueItem{Key: key, Value: values[bucketId][key]})
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
