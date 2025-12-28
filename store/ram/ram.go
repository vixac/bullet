package ram

import (
	"sync"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

type RamStore struct {
	mu sync.RWMutex

	tracks  map[store_interface.TenancySpace]map[int32]map[string]model.TrackValue // appID -> bucketID -> key -> value
	depots  map[store_interface.TenancySpace]map[int64]string                      // appID -> key -> value
	wayfind map[store_interface.TenancySpace]map[int32]map[string]model.WayFinderQueryItem
}

// NewRamStore returns a new empty in-memory store
func NewRamStore() *RamStore {
	return &RamStore{
		tracks:  make(map[store_interface.TenancySpace]map[int32]map[string]model.TrackValue),
		depots:  make(map[store_interface.TenancySpace]map[int64]string),
		wayfind: make(map[store_interface.TenancySpace]map[int32]map[string]model.WayFinderQueryItem),
	}
}
