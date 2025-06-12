package boltdb

import (
	"errors"

	"github.com/vixac/bullet/model"
)

func (b *BoltStore) PigeonPut(appID int32, key int64, value string) error {
	return errors.New("not implemented")
}
func (b *BoltStore) PigeonGet(appID int32, key int64) (string, error) {
	return "", errors.New("not implemented")
}
func (b *BoltStore) PigeonDelete(appID int32, key int64) error {
	return errors.New("not implemented")
}
func (b *BoltStore) PigeonPutMany(appID int32, items []model.PigeonKeyValueItem) error {
	return errors.New("not implemented")
}
func (b *BoltStore) PigeonGetMany(appID int32, keys []int64) (map[int64]string, []int64, error) {
	return nil, nil, errors.New("not implemented")
}
