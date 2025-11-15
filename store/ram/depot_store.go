package ram

import (
	"errors"

	"github.com/vixac/bullet/model"
)

func (r *RamStore) DepotPut(appID int32, key int64, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.depots[appID] == nil {
		r.depots[appID] = make(map[int64]string)
	}
	r.depots[appID][key] = value
	return nil
}

func (r *RamStore) DepotGet(appID int32, key int64) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	val, ok := r.depots[appID][key]
	if !ok {
		return "", errors.New("key not found")
	}
	return val, nil
}

func (r *RamStore) DepotDelete(appID int32, key int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.depots[appID], key)
	return nil
}

func (r *RamStore) DepotPutMany(appID int32, items []model.DepotKeyValueItem) error {
	for _, item := range items {
		if err := r.DepotPut(appID, item.Key, item.Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *RamStore) DepotGetMany(appID int32, keys []int64) (map[int64]string, []int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	found := make(map[int64]string)
	var missing []int64
	for _, k := range keys {
		if v, ok := r.depots[appID][k]; ok {
			found[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	return found, missing, nil
}
