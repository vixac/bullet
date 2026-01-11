package ram

import (
	"sync"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

// Internal node data structure for Grove
type nodeData struct {
	id       store_interface.NodeID
	parent   *store_interface.NodeID
	position *store_interface.ChildPosition
	metadata *store_interface.NodeMetadata
	depth    int // absolute depth from tree root
}

type RamStore struct {
	mu sync.RWMutex

	tracks  map[store_interface.TenancySpace]map[int32]map[string]model.TrackValue // appID -> bucketID -> key -> value
	depots  map[store_interface.TenancySpace]map[int64]string                      // appID -> key -> value
	wayfind map[store_interface.TenancySpace]map[int32]map[string]model.WayFinderQueryItem

	// Grove data structures (with TreeID for logical tree separation)
	groveNodes        map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]*nodeData
	groveClosure      map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.NodeID]int // ancestor -> descendant -> relative_depth
	groveChildren     map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID][]store_interface.NodeID       // parent -> ordered children
	groveDeletedNodes map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]*nodeData
	groveMutations    map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.MutationID]bool
	groveAggregates   map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.AggregateKey]store_interface.AggregateValue
}

// NewRamStore returns a new empty in-memory store
func NewRamStore() *RamStore {
	return &RamStore{
		tracks:  make(map[store_interface.TenancySpace]map[int32]map[string]model.TrackValue),
		depots:  make(map[store_interface.TenancySpace]map[int64]string),
		wayfind: make(map[store_interface.TenancySpace]map[int32]map[string]model.WayFinderQueryItem),
	}
}
