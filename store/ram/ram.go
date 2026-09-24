package ram

import (
	"sync"

	"github.com/vixac/bullet/model"
)

// Internal node data structure for Grove
type nodeData struct {
	id       model.NodeID
	parent   *model.NodeID
	position *model.ChildPosition
	metadata *model.NodeMetadata
	depth    int // absolute depth from tree root
}

type depotEntry struct {
	value    string
	bucketID int32
}

type RamStore struct {
	mu        sync.RWMutex
	warehouse map[model.TenancySpace]*warehouseSpace

	tracks         map[model.TenancySpace]map[int32]map[string]model.TrackValue // appID -> bucketID -> key -> value
	trackPayloads  map[model.TenancySpace]map[int32]map[string][]byte
	trackMutations map[model.MutationID]struct{}
	depots         map[model.TenancySpace]map[int64]depotEntry // space -> id -> entry
	depotNextIDs   map[model.TenancySpace]int64                // space -> next auto-increment id
	ledgers        map[model.TenancySpace]*ledgerSpaceData

	// Grove data structures (with TreeID for logical tree separation)
	groveNodes        map[model.TenancySpace]map[model.TreeID]map[model.NodeID]*nodeData
	groveClosure      map[model.TenancySpace]map[model.TreeID]map[model.NodeID]map[model.NodeID]int // ancestor -> descendant -> relative_depth
	groveChildren     map[model.TenancySpace]map[model.TreeID]map[model.NodeID][]model.NodeID       // parent -> ordered children
	groveDeletedNodes map[model.TenancySpace]map[model.TreeID]map[model.NodeID]*nodeData
	groveMutations    map[model.TenancySpace]map[model.TreeID]map[model.NodeID]map[model.MutationID]bool
	groveAggregates   map[model.TenancySpace]map[model.TreeID]map[model.NodeID]map[model.AggregateKey]model.AggregateValue
}

// NewRamStore returns a new empty in-memory store
func NewRamStore() *RamStore {
	return &RamStore{
		tracks:         make(map[model.TenancySpace]map[int32]map[string]model.TrackValue),
		trackPayloads:  make(map[model.TenancySpace]map[int32]map[string][]byte),
		trackMutations: make(map[model.MutationID]struct{}),
		depots:         make(map[model.TenancySpace]map[int64]depotEntry),
		depotNextIDs:   make(map[model.TenancySpace]int64),
		ledgers:        make(map[model.TenancySpace]*ledgerSpaceData),
	}
}
