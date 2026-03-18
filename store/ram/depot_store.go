package ram

import (
	"fmt"

	"github.com/vixac/bullet/store/store_interface"
)

func (m *RamStore) depotEnsureSpace(space store_interface.TenancySpace) {
	if _, ok := m.depots[space]; !ok {
		m.depots[space] = make(map[int64]depotEntry)
	}
}

func (m *RamStore) depotGenID(space store_interface.TenancySpace) int64 {
	m.depotNextIDs[space]++
	return m.depotNextIDs[space]
}

func (m *RamStore) DepotCreate(space store_interface.TenancySpace, bucketID int32, value string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.depotEnsureSpace(space)
	id := m.depotGenID(space)
	m.depots[space][id] = depotEntry{value: value, bucketID: bucketID}
	return id, nil
}

func (m *RamStore) DepotCreateMany(space store_interface.TenancySpace, bucketID int32, values []string) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.depotEnsureSpace(space)
	ids := make([]int64, len(values))
	for i, v := range values {
		id := m.depotGenID(space)
		m.depots[space][id] = depotEntry{value: v, bucketID: bucketID}
		ids[i] = id
	}
	return ids, nil
}

func (m *RamStore) DepotUpdate(space store_interface.TenancySpace, id int64, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if spaceMap, ok := m.depots[space]; ok {
		if entry, ok := spaceMap[id]; ok {
			entry.value = value
			spaceMap[id] = entry
			return nil
		}
	}
	return fmt.Errorf("depot item %d not found", id)
}

func (m *RamStore) DepotGet(space store_interface.TenancySpace, id int64) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if spaceMap, ok := m.depots[space]; ok {
		if entry, ok := spaceMap[id]; ok {
			return entry.value, nil
		}
	}
	return "", fmt.Errorf("depot item %d not found", id)
}

func (m *RamStore) DepotGetMany(space store_interface.TenancySpace, ids []int64) (map[int64]string, []int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	found := make(map[int64]string)
	var missing []int64

	spaceMap := m.depots[space]
	for _, id := range ids {
		if spaceMap != nil {
			if entry, ok := spaceMap[id]; ok {
				found[id] = entry.value
				continue
			}
		}
		missing = append(missing, id)
	}
	return found, missing, nil
}

func (m *RamStore) DepotDelete(space store_interface.TenancySpace, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if spaceMap, ok := m.depots[space]; ok {
		delete(spaceMap, id)
	}
	return nil
}

func (m *RamStore) DepotDeleteByBucket(space store_interface.TenancySpace, bucketID int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if spaceMap, ok := m.depots[space]; ok {
		for id, entry := range spaceMap {
			if entry.bucketID == bucketID {
				delete(spaceMap, id)
			}
		}
	}
	return nil
}

func (m *RamStore) DepotGetAllByBucket(space store_interface.TenancySpace, bucketID int32) (map[int64]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[int64]string)
	if spaceMap, ok := m.depots[space]; ok {
		for id, entry := range spaceMap {
			if entry.bucketID == bucketID {
				result[id] = entry.value
			}
		}
	}
	return result, nil
}
