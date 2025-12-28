package boltdb

import "go.etcd.io/bbolt"

var schemaBucket = []byte("__schema")
var schemaVersionKey = []byte("version")

const currentSchemaVersion = 2
const DefaultTenant = 0

func (s *BoltStore) MigrateToTenantBuckets(appID int32) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		// schema bucket
		sb, err := tx.CreateBucketIfNotExists(schemaBucket)
		if err != nil {
			return err
		}

		v := sb.Get(schemaVersionKey)
		if v != nil && string(v) == "2" {
			return nil // already migrated
		}

		oldBkt := tx.Bucket(oldBucketName(appID))
		if oldBkt == nil {
			// nothing to migrate
			sb.Put(schemaVersionKey, []byte("2"))
			return nil
		}

		newBkt, err := tx.CreateBucketIfNotExists(
			newDepotBucket(appID, DefaultTenant),
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
		if err := tx.DeleteBucket(oldBucketName(appID)); err != nil {
			return err
		}

		sb.Put(schemaVersionKey, []byte("2"))
		return nil
	})
}
