package migrator

import (
	"fmt"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

type DepotMigrator struct {
	SourceDepot store_interface.DepotStore
	TargetDepot store_interface.DepotStore
	Tenancy     store_interface.TenancySpace
}

// Migrate migrates the specified keys from source to target depot
func (d *DepotMigrator) Migrate(keys []int64) error {
	if len(keys) == 0 {
		fmt.Printf("Depot: no keys to migrate\n")
		return nil
	}

	// Fetch all values from source
	values, missingKeys, err := d.SourceDepot.DepotGetMany(d.Tenancy, keys)
	if err != nil {
		return fmt.Errorf("failed to fetch depot items: %w", err)
	}

	if len(missingKeys) > 0 {
		fmt.Printf("Depot: warning - %d keys not found in source\n", len(missingKeys))
	}

	if len(values) == 0 {
		fmt.Printf("Depot: no items found to migrate\n")
		return nil
	}

	// Package items for DepotPutMany
	items := make([]model.DepotKeyValueItem, 0, len(values))
	for key, value := range values {
		items = append(items, model.DepotKeyValueItem{
			Key:   key,
			Value: value,
		})
	}

	// Write to target store
	err = d.TargetDepot.DepotPutMany(d.Tenancy, items)
	if err != nil {
		return fmt.Errorf("failed to write depot items to target: %w", err)
	}

	fmt.Printf("Depot: successfully migrated %d items\n", len(items))
	return nil
}
