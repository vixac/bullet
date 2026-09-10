package boltdb

import (
	"fmt"

	"github.com/vixac/bullet/model"
)

func newDepotBucket(appID int32, tenantId int64) []byte {
	return []byte(fmt.Sprintf("depot:v2:%d:tenant:%d", appID, tenantId))
}

func getBucketName(space model.TenancySpace) []byte {
	return newDepotBucket(space.AppId, space.TenancyId)
}
func (m *BoltStore) DepotCreate(space model.TenancySpace, bucketID int32, value string) (int64, error) {
	return 0, nil
}
func (m *BoltStore) DepotCreateMany(space model.TenancySpace, bucketID int32, values []string) ([]int64, error) {
	return []int64{}, nil
}

func (m *BoltStore) DepotUpdate(space model.TenancySpace, id int64, value string) error {
	return nil

}

func (m *BoltStore) DepotGet(space model.TenancySpace, id int64) (string, error) {
	return "", nil
}
func (m *BoltStore) DepotGetMany(space model.TenancySpace, ids []int64) (map[int64]string, []int64, error) {
	return map[int64]string{}, []int64{}, nil
}

func (m *BoltStore) DepotDelete(space model.TenancySpace, id int64) error {
	return nil
}
func (m *BoltStore) DepotDeleteByBucket(space model.TenancySpace, bucketID int32) error {
	return nil

}
func (m *BoltStore) DepotGetAllByBucket(space model.TenancySpace, bucketID int32) (map[int64]string, error) {

	return map[int64]string{}, nil
}
