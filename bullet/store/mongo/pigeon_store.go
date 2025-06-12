package mongodb

import (
	"errors"

	"github.com/vixac/bullet/model"
)

func (m *MongoStore) PigeonPut(appID int32, key int64, value string) error {
	return errors.New("not implemented")
}
func (m *MongoStore) PigeonGet(appID int32, key int64) (string, error) {
	return "", errors.New("not implemented")
}
func (m *MongoStore) PigeonDelete(appID int32, key int64) error {
	return errors.New("not implemented")
}
func (m *MongoStore) PigeonPutMany(appID int32, items []model.PigeonKeyValueItem) error {
	return errors.New("not implemented")
}
func (m *MongoStore) PigeonGetMany(appID int32, keys []int64) (map[int64]string, []int64, error) {
	return nil, nil, errors.New("not implemented")
}
