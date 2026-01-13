package ram

import (
	"errors"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func (r *RamStore) DepotPut(space store_interface.TenancySpace, key int64, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.depots[space] == nil {
		r.depots[space] = make(map[int64]string)
	}
	r.depots[space][key] = value
	return nil
}

func (r *RamStore) DepotGetAll(space store_interface.TenancySpace) (map[int64]string, error) {
	x := make(map[int64]string)
	return x, errors.New("Not implmented")
}

func (r *RamStore) DepotGet(space store_interface.TenancySpace, key int64) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	val, ok := r.depots[space][key]
	if !ok {
		return "", errors.New("key not found")
	}
	return val, nil
}

func (r *RamStore) DepotDelete(space store_interface.TenancySpace, key int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.depots[space], key)
	return nil
}

func (r *RamStore) DepotPutMany(space store_interface.TenancySpace, items []model.DepotKeyValueItem) error {
	for _, item := range items {
		if err := r.DepotPut(space, item.Key, item.Value); err != nil {
			return err
		}
	}
	return nil
}

func (r *RamStore) DepotGetMany(space store_interface.TenancySpace, keys []int64) (map[int64]string, []int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	found := make(map[int64]string)
	var missing []int64
	for _, k := range keys {
		if v, ok := r.depots[space][k]; ok {
			found[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	return found, missing, nil
}
