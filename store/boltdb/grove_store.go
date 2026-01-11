package boltdb

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/vixac/bullet/store/store_interface"
	"go.etcd.io/bbolt"
)

// Bucket naming helpers
func groveNodesBucket(space store_interface.TenancySpace) []byte {
	return []byte(fmt.Sprintf("grove:nodes:%d:%d", space.AppId, space.TenancyId))
}

func groveClosureBucket(space store_interface.TenancySpace) []byte {
	return []byte(fmt.Sprintf("grove:closure:%d:%d", space.AppId, space.TenancyId))
}

func groveMutationsBucket(space store_interface.TenancySpace) []byte {
	return []byte(fmt.Sprintf("grove:mutations:%d:%d", space.AppId, space.TenancyId))
}

func groveAggregatesBucket(space store_interface.TenancySpace) []byte {
	return []byte(fmt.Sprintf("grove:aggregates:%d:%d", space.AppId, space.TenancyId))
}

func groveDeletedBucket(space store_interface.TenancySpace) []byte {
	return []byte(fmt.Sprintf("grove:deleted:%d:%d", space.AppId, space.TenancyId))
}

// Node data structure
type nodeData struct {
	ID       string                          `json:"id"`
	Parent   *string                         `json:"parent,omitempty"`
	Position *float64                        `json:"position,omitempty"`
	Depth    int                             `json:"depth"`
	Metadata *store_interface.NodeMetadata   `json:"metadata,omitempty"`
}

// Closure entry structure
type closureEntry struct {
	AncestorID   string `json:"ancestor_id"`
	DescendantID string `json:"descendant_id"`
	Depth        int    `json:"depth"`
}

// CreateNode creates a new node in the tree
func (b *BoltStore) CreateNode(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
	parent *store_interface.NodeID,
	position *store_interface.ChildPosition,
	metadata *store_interface.NodeMetadata,
) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		nodesBkt, err := tx.CreateBucketIfNotExists(groveNodesBucket(space))
		if err != nil {
			return err
		}
		closureBkt, err := tx.CreateBucketIfNotExists(groveClosureBucket(space))
		if err != nil {
			return err
		}

		// Check if node already exists
		nodeKey := []byte(node)
		if nodesBkt.Get(nodeKey) != nil {
			return store_interface.ErrNodeAlreadyExists
		}

		// Calculate depth
		var depth int
		if parent != nil {
			parentKey := []byte(*parent)
			parentData := nodesBkt.Get(parentKey)
			if parentData == nil {
				return store_interface.ErrNodeNotFound
			}
			var parentNode nodeData
			if err := json.Unmarshal(parentData, &parentNode); err != nil {
				return err
			}
			depth = parentNode.Depth + 1
		}

		// Create node data
		var parentStr *string
		if parent != nil {
			p := string(*parent)
			parentStr = &p
		}
		var positionVal *float64
		if position != nil {
			p := float64(*position)
			positionVal = &p
		}

		nodeObj := nodeData{
			ID:       string(node),
			Parent:   parentStr,
			Position: positionVal,
			Depth:    depth,
			Metadata: metadata,
		}

		// Save node
		nodeBytes, err := json.Marshal(nodeObj)
		if err != nil {
			return err
		}
		if err := nodesBkt.Put(nodeKey, nodeBytes); err != nil {
			return err
		}

		// Insert self-reference in closure table
		selfClosure := closureEntry{
			AncestorID:   string(node),
			DescendantID: string(node),
			Depth:        0,
		}
		selfKey := []byte(fmt.Sprintf("%s:%s", node, node))
		selfBytes, err := json.Marshal(selfClosure)
		if err != nil {
			return err
		}
		if err := closureBkt.Put(selfKey, selfBytes); err != nil {
			return err
		}

		// If has parent, add relationships to all ancestors
		if parent != nil {
			c := closureBkt.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var entry closureEntry
				if err := json.Unmarshal(v, &entry); err != nil {
					continue
				}
				// If this entry has parent as descendant, then ancestor is also ancestor of node
				if entry.DescendantID == string(*parent) {
					newEntry := closureEntry{
						AncestorID:   entry.AncestorID,
						DescendantID: string(node),
						Depth:        entry.Depth + 1,
					}
					newKey := []byte(fmt.Sprintf("%s:%s", newEntry.AncestorID, newEntry.DescendantID))
					newBytes, err := json.Marshal(newEntry)
					if err != nil {
						return err
					}
					if err := closureBkt.Put(newKey, newBytes); err != nil {
						return err
					}
				}
			}
		}

		return nil
	})
}

// DeleteNode deletes a node (soft or hard delete)
func (b *BoltStore) DeleteNode(space store_interface.TenancySpace, node store_interface.NodeID, soft bool) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		closureBkt := tx.Bucket(groveClosureBucket(space))

		nodeKey := []byte(node)
		nodeBytes := nodesBkt.Get(nodeKey)
		if nodeBytes == nil {
			return store_interface.ErrNodeNotFound
		}

		// Check if node has children
		c := nodesBkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var n nodeData
			if err := json.Unmarshal(v, &n); err != nil {
				continue
			}
			if n.Parent != nil && *n.Parent == string(node) {
				return fmt.Errorf("cannot delete node with children")
			}
		}

		if soft {
			// Soft delete: move to deleted bucket
			deletedBkt, err := tx.CreateBucketIfNotExists(groveDeletedBucket(space))
			if err != nil {
				return err
			}
			if err := deletedBkt.Put(nodeKey, nodeBytes); err != nil {
				return err
			}
		}

		// Remove from nodes bucket
		if err := nodesBkt.Delete(nodeKey); err != nil {
			return err
		}

		// Remove from closure table
		if closureBkt != nil {
			c := closureBkt.Cursor()
			var keysToDelete [][]byte
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var entry closureEntry
				if err := json.Unmarshal(v, &entry); err != nil {
					continue
				}
				if entry.AncestorID == string(node) || entry.DescendantID == string(node) {
					keysToDelete = append(keysToDelete, append([]byte(nil), k...))
				}
			}
			for _, k := range keysToDelete {
				if err := closureBkt.Delete(k); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

// MoveNode moves a node to a new parent
func (b *BoltStore) MoveNode(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
	newParent *store_interface.NodeID,
	newPosition *store_interface.ChildPosition,
) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		closureBkt := tx.Bucket(groveClosureBucket(space))
		if closureBkt == nil {
			return store_interface.ErrNodeNotFound
		}

		nodeKey := []byte(node)
		nodeBytes := nodesBkt.Get(nodeKey)
		if nodeBytes == nil {
			return store_interface.ErrNodeNotFound
		}

		var nodeObj nodeData
		if err := json.Unmarshal(nodeBytes, &nodeObj); err != nil {
			return err
		}
		currentDepth := nodeObj.Depth

		// Calculate new depth
		var newDepth int
		if newParent != nil {
			parentKey := []byte(*newParent)
			parentBytes := nodesBkt.Get(parentKey)
			if parentBytes == nil {
				return store_interface.ErrNodeNotFound
			}
			var parentNode nodeData
			if err := json.Unmarshal(parentBytes, &parentNode); err != nil {
				return err
			}

			// Check for cycles: newParent cannot be a descendant of node
			cycleKey := []byte(fmt.Sprintf("%s:%s", node, *newParent))
			if closureBkt.Get(cycleKey) != nil {
				return store_interface.ErrCycleDetected
			}

			newDepth = parentNode.Depth + 1
		}

		depthDelta := newDepth - currentDepth

		// Get all descendants (including node itself)
		type descendantInfo struct {
			id    string
			depth int
		}
		var descendants []descendantInfo

		c := closureBkt.Cursor()
		prefix := []byte(fmt.Sprintf("%s:", node))
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var entry closureEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				continue
			}
			if entry.AncestorID == string(node) {
				descendants = append(descendants, descendantInfo{
					id:    entry.DescendantID,
					depth: entry.Depth,
				})
			}
		}

		// Remove old ancestor relationships (except self-references)
		var keysToDelete [][]byte
		c = closureBkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry closureEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				continue
			}
			// Delete if descendant is in our list and ancestor != descendant
			for _, desc := range descendants {
				if entry.DescendantID == desc.id && entry.AncestorID != entry.DescendantID {
					keysToDelete = append(keysToDelete, append([]byte(nil), k...))
					break
				}
			}
		}
		for _, k := range keysToDelete {
			if err := closureBkt.Delete(k); err != nil {
				return err
			}
		}

		// Update node's parent, position, and depth
		var newParentStr *string
		if newParent != nil {
			p := string(*newParent)
			newParentStr = &p
		}
		var newPositionVal *float64
		if newPosition != nil {
			p := float64(*newPosition)
			newPositionVal = &p
		}

		nodeObj.Parent = newParentStr
		nodeObj.Position = newPositionVal
		nodeObj.Depth = newDepth

		// Update depths for all descendants
		c = nodesBkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var n nodeData
			if err := json.Unmarshal(v, &n); err != nil {
				continue
			}
			for _, desc := range descendants {
				if n.ID == desc.id && n.ID != string(node) {
					n.Depth += depthDelta
					updatedBytes, err := json.Marshal(n)
					if err != nil {
						return err
					}
					if err := nodesBkt.Put(k, updatedBytes); err != nil {
						return err
					}
					break
				}
			}
		}

		// Save updated node
		nodeBytes, err := json.Marshal(nodeObj)
		if err != nil {
			return err
		}
		if err := nodesBkt.Put(nodeKey, nodeBytes); err != nil {
			return err
		}

		// Rebuild closure table for node and descendants
		if newParent != nil {
			// For each descendant, add relationships to all new ancestors
			c := closureBkt.Cursor()
			for _, desc := range descendants {
				relativeDepth := desc.depth
				// Find all ancestors of newParent
				for k, v := c.First(); k != nil; k, v = c.Next() {
					var entry closureEntry
					if err := json.Unmarshal(v, &entry); err != nil {
						continue
					}
					// If this entry has newParent as descendant
					if entry.DescendantID == string(*newParent) {
						newEntry := closureEntry{
							AncestorID:   entry.AncestorID,
							DescendantID: desc.id,
							Depth:        entry.Depth + 1 + relativeDepth,
						}
						newKey := []byte(fmt.Sprintf("%s:%s", newEntry.AncestorID, newEntry.DescendantID))
						newBytes, err := json.Marshal(newEntry)
						if err != nil {
							return err
						}
						if err := closureBkt.Put(newKey, newBytes); err != nil {
							return err
						}
					}
				}
			}
		}

		return nil
	})
}

// Exists checks if a node exists
func (b *BoltStore) Exists(space store_interface.TenancySpace, node store_interface.NodeID) (bool, error) {
	var exists bool
	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return nil
		}
		exists = nodesBkt.Get([]byte(node)) != nil
		return nil
	})
	return exists, err
}

// GetNodeInfo gets complete node information
func (b *BoltStore) GetNodeInfo(space store_interface.TenancySpace, node store_interface.NodeID) (*store_interface.NodeInfo, error) {
	var info *store_interface.NodeInfo
	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}

		nodeBytes := nodesBkt.Get([]byte(node))
		if nodeBytes == nil {
			return store_interface.ErrNodeNotFound
		}

		var nodeObj nodeData
		if err := json.Unmarshal(nodeBytes, &nodeObj); err != nil {
			return err
		}

		var parent *store_interface.NodeID
		if nodeObj.Parent != nil {
			p := store_interface.NodeID(*nodeObj.Parent)
			parent = &p
		}

		var position *store_interface.ChildPosition
		if nodeObj.Position != nil {
			p := store_interface.ChildPosition(*nodeObj.Position)
			position = &p
		}

		info = &store_interface.NodeInfo{
			ID:       node,
			Parent:   parent,
			Position: position,
			Depth:    nodeObj.Depth,
			Metadata: nodeObj.Metadata,
		}
		return nil
	})
	return info, err
}

// GetChildren gets children of a node
func (b *BoltStore) GetChildren(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	var children []store_interface.NodeID

	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}

		// Check if parent node exists
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		// Find all children
		type childEntry struct {
			id       store_interface.NodeID
			position *float64
		}
		var childEntries []childEntry

		c := nodesBkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var n nodeData
			if err := json.Unmarshal(v, &n); err != nil {
				continue
			}
			if n.Parent != nil && *n.Parent == string(node) {
				childEntries = append(childEntries, childEntry{
					id:       store_interface.NodeID(n.ID),
					position: n.Position,
				})
			}
		}

		// Sort by position, then by ID
		sort.Slice(childEntries, func(i, j int) bool {
			if childEntries[i].position != nil && childEntries[j].position != nil {
				if *childEntries[i].position != *childEntries[j].position {
					return *childEntries[i].position < *childEntries[j].position
				}
			} else if childEntries[i].position != nil {
				return true
			} else if childEntries[j].position != nil {
				return false
			}
			return childEntries[i].id < childEntries[j].id
		})

		for _, entry := range childEntries {
			children = append(children, entry.id)
		}

		return nil
	})

	return children, &store_interface.PaginationResult{NextCursor: nil}, err
}

// GetAncestors gets all ancestors of a node
func (b *BoltStore) GetAncestors(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	var ancestors []store_interface.NodeID

	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		closureBkt := tx.Bucket(groveClosureBucket(space))
		if closureBkt == nil {
			return nil
		}

		type ancestorEntry struct {
			id    store_interface.NodeID
			depth int
		}
		var ancestorEntries []ancestorEntry

		c := closureBkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry closureEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				continue
			}
			if entry.DescendantID == string(node) && entry.AncestorID != string(node) {
				ancestorEntries = append(ancestorEntries, ancestorEntry{
					id:    store_interface.NodeID(entry.AncestorID),
					depth: entry.Depth,
				})
			}
		}

		// Sort by depth descending (closest ancestors first)
		sort.Slice(ancestorEntries, func(i, j int) bool {
			return ancestorEntries[i].depth > ancestorEntries[j].depth
		})

		for _, entry := range ancestorEntries {
			ancestors = append(ancestors, entry.id)
		}

		return nil
	})

	return ancestors, &store_interface.PaginationResult{NextCursor: nil}, err
}

// GetDescendants gets all descendants of a node
func (b *BoltStore) GetDescendants(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
	opts *store_interface.DescendantOptions,
) ([]store_interface.NodeWithDepth, *store_interface.PaginationResult, error) {
	var descendants []store_interface.NodeWithDepth

	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		closureBkt := tx.Bucket(groveClosureBucket(space))
		if closureBkt == nil {
			return nil
		}

		c := closureBkt.Cursor()
		prefix := []byte(fmt.Sprintf("%s:", node))
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var entry closureEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				continue
			}
			if entry.AncestorID == string(node) && entry.DescendantID != string(node) {
				// Apply maxDepth filter if specified
				if opts != nil && opts.MaxDepth != nil && entry.Depth > *opts.MaxDepth {
					continue
				}
				descendants = append(descendants, store_interface.NodeWithDepth{
					NodeID: store_interface.NodeID(entry.DescendantID),
					Depth:  entry.Depth,
				})
			}
		}

		// Sort by depth
		sort.Slice(descendants, func(i, j int) bool {
			return descendants[i].Depth < descendants[j].Depth
		})

		return nil
	})

	return descendants, &store_interface.PaginationResult{NextCursor: nil}, err
}

// ApplyAggregateMutation applies aggregate deltas to a node
func (b *BoltStore) ApplyAggregateMutation(
	space store_interface.TenancySpace,
	mutation store_interface.MutationID,
	node store_interface.NodeID,
	deltas store_interface.AggregateDeltas,
) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		mutationsBkt, err := tx.CreateBucketIfNotExists(groveMutationsBucket(space))
		if err != nil {
			return err
		}

		// Check if mutation already applied
		mutationKey := []byte(fmt.Sprintf("%s:%s", node, mutation))
		if mutationsBkt.Get(mutationKey) != nil {
			return store_interface.ErrMutationConflict
		}

		aggregatesBkt, err := tx.CreateBucketIfNotExists(groveAggregatesBucket(space))
		if err != nil {
			return err
		}

		// Apply deltas
		for key, delta := range deltas {
			aggKey := []byte(fmt.Sprintf("%s:%s", node, key))
			var currentValue int64
			if existing := aggregatesBkt.Get(aggKey); existing != nil {
				if err := json.Unmarshal(existing, &currentValue); err != nil {
					return err
				}
			}
			currentValue += int64(delta)
			valueBytes, err := json.Marshal(currentValue)
			if err != nil {
				return err
			}
			if err := aggregatesBkt.Put(aggKey, valueBytes); err != nil {
				return err
			}
		}

		// Mark mutation as applied
		if err := mutationsBkt.Put(mutationKey, []byte("1")); err != nil {
			return err
		}

		return nil
	})
}

// GetNodeLocalAggregates gets aggregates for the node only
func (b *BoltStore) GetNodeLocalAggregates(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)

	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		aggregatesBkt := tx.Bucket(groveAggregatesBucket(space))
		if aggregatesBkt == nil {
			return nil
		}

		c := aggregatesBkt.Cursor()
		prefix := []byte(fmt.Sprintf("%s:", node))
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			// Extract aggregate key from composite key
			keyStr := string(k[len(prefix):])
			var value int64
			if err := json.Unmarshal(v, &value); err != nil {
				return err
			}
			result[store_interface.AggregateKey(keyStr)] = store_interface.AggregateValue(value)
		}

		return nil
	})

	return result, err
}

// GetNodeWithDescendantsAggregates gets aggregates for node + all descendants
func (b *BoltStore) GetNodeWithDescendantsAggregates(
	space store_interface.TenancySpace,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)

	err := b.db.View(func(tx *bbolt.Tx) error {
		nodesBkt := tx.Bucket(groveNodesBucket(space))
		if nodesBkt == nil {
			return store_interface.ErrNodeNotFound
		}
		if nodesBkt.Get([]byte(node)) == nil {
			return store_interface.ErrNodeNotFound
		}

		closureBkt := tx.Bucket(groveClosureBucket(space))
		aggregatesBkt := tx.Bucket(groveAggregatesBucket(space))
		if aggregatesBkt == nil {
			return nil
		}

		// Get all descendants (including self)
		descendants := []string{string(node)}
		if closureBkt != nil {
			c := closureBkt.Cursor()
			prefix := []byte(fmt.Sprintf("%s:", node))
			for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
				var entry closureEntry
				if err := json.Unmarshal(v, &entry); err != nil {
					continue
				}
				if entry.AncestorID == string(node) && entry.DescendantID != string(node) {
					descendants = append(descendants, entry.DescendantID)
				}
			}
		}

		// Aggregate values from all descendants
		for _, desc := range descendants {
			c := aggregatesBkt.Cursor()
			prefix := []byte(fmt.Sprintf("%s:", desc))
			for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
				// Extract aggregate key from composite key
				keyStr := string(k[len(prefix):])
				var value int64
				if err := json.Unmarshal(v, &value); err != nil {
					return err
				}
				result[store_interface.AggregateKey(keyStr)] += store_interface.AggregateValue(value)
			}
		}

		return nil
	})

	return result, err
}
