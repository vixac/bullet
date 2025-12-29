package boltdb

import (
	"fmt"

	"github.com/vixac/bullet/store/store_interface"
	"go.etcd.io/bbolt"
)

var schemaBucket = []byte("__schema")
var schemaVersionKey = []byte("version")

const currentSchemaVersion = "4"

func (s *BoltStore) MigrateToTenantBuckets(space store_interface.TenancySpace, bucketId int32) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		// schema bucket
		sb, err := tx.CreateBucketIfNotExists(schemaBucket)
		if err != nil {
			return err
		}

		//VX:Note when migrating multiple things, this schema check gets in the way.
		/*
			v := sb.Get(schemaVersionKey)
			if v != nil && string(v) == currentSchemaVersion {
				fmt.Printf("VX: ALREADY")
				return nil // already migrated
			}*/

		oldBkt := tx.Bucket(oldTrackBucketName(space, bucketId))
		if oldBkt == nil {
			fmt.Printf("VX: no bucket \n")
			// nothing to migrate
			sb.Put(schemaVersionKey, []byte(currentSchemaVersion))
			return nil
		}

		fmt.Printf("VX: new bucket \n")
		newBkt, err := tx.CreateBucketIfNotExists(
			newTrackBucketName(space, bucketId),
		)
		if err != nil {
			return err
		}

		// copy all kv pairs
		err = oldBkt.ForEach(func(k, v []byte) error {
			return newBkt.Put(k, v)
		})
		if err != nil {
			return err
		}

		// optional: delete old bucket
		if err := tx.DeleteBucket(oldTrackBucketName(space, bucketId)); err != nil {
			return err
		}

		sb.Put(schemaVersionKey, []byte(currentSchemaVersion))
		return nil
	})
}
