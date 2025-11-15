package ram

import (
	"sync"

	"github.com/vixac/bullet/model"
)

type RamStore struct {
	mu sync.RWMutex

	tracks  map[int32]map[int32]map[string]model.TrackValue // appID -> bucketID -> key -> value
	depots  map[int32]map[int64]string                      // appID -> key -> value
	wayfind map[int32]map[int32]map[string]model.WayFinderQueryItem
}

// NewRamStore returns a new empty in-memory store
func NewRamStore() *RamStore {
	return &RamStore{
		tracks:  make(map[int32]map[int32]map[string]model.TrackValue),
		depots:  make(map[int32]map[int64]string),
		wayfind: make(map[int32]map[int32]map[string]model.WayFinderQueryItem),
	}
}
