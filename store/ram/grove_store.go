package ram

import (
	"fmt"

	"github.com/vixac/bullet/store/store_interface"
)

// Grove data structures in RamStore (defined in ram.go)
// nodes: map[TenancySpace]map[TreeID]map[NodeID]*nodeData
// closure: map[TenancySpace]map[TreeID]map[NodeID]map[NodeID]int  // ancestor -> descendant -> relative_depth
// children: map[TenancySpace]map[TreeID]map[NodeID][]NodeID  // parent -> ordered children
// deletedNodes: map[TenancySpace]map[TreeID]map[NodeID]*nodeData
// mutations: map[TenancySpace]map[TreeID]map[NodeID]map[MutationID]bool
// aggregates: map[TenancySpace]map[TreeID]map[NodeID]map[AggregateKey]AggregateValue

// CreateNode creates a new node in the tree
func (r *RamStore) CreateNode(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	parent *store_interface.NodeID,
	position *store_interface.ChildPosition,
	metadata *store_interface.NodeMetadata,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize maps if needed
	if r.groveNodes == nil {
		r.groveNodes = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]*nodeData)
	}
	if r.groveNodes[space] == nil {
		r.groveNodes[space] = make(map[store_interface.TreeID]map[store_interface.NodeID]*nodeData)
	}
	if r.groveNodes[space][treeID] == nil {
		r.groveNodes[space][treeID] = make(map[store_interface.NodeID]*nodeData)
	}
	if r.groveClosure == nil {
		r.groveClosure = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.NodeID]int)
	}
	if r.groveClosure[space] == nil {
		r.groveClosure[space] = make(map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.NodeID]int)
	}
	if r.groveClosure[space][treeID] == nil {
		r.groveClosure[space][treeID] = make(map[store_interface.NodeID]map[store_interface.NodeID]int)
	}
	if r.groveChildren == nil {
		r.groveChildren = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID][]store_interface.NodeID)
	}
	if r.groveChildren[space] == nil {
		r.groveChildren[space] = make(map[store_interface.TreeID]map[store_interface.NodeID][]store_interface.NodeID)
	}
	if r.groveChildren[space][treeID] == nil {
		r.groveChildren[space][treeID] = make(map[store_interface.NodeID][]store_interface.NodeID)
	}

	// Check if node already exists
	if _, exists := r.groveNodes[space][treeID][node]; exists {
		return store_interface.ErrNodeAlreadyExists
	}

	// Check if parent exists (if specified)
	var depth int
	if parent != nil {
		parentNode, exists := r.groveNodes[space][treeID][*parent]
		if !exists {
			return store_interface.ErrNodeNotFound
		}
		depth = parentNode.depth + 1
	} else {
		depth = 0 // root node
	}

	// Create the node
	nodeObj := &nodeData{
		id:       node,
		parent:   parent,
		position: position,
		metadata: metadata,
		depth:    depth,
	}
	r.groveNodes[space][treeID][node] = nodeObj

	// Update closure table: node is descendant of itself at depth 0
	if r.groveClosure[space][treeID][node] == nil {
		r.groveClosure[space][treeID][node] = make(map[store_interface.NodeID]int)
	}
	r.groveClosure[space][treeID][node][node] = 0

	// If has parent, add relationships to all ancestors
	if parent != nil {
		// Find all ancestors of parent (nodes that have parent as descendant)
		for ancestor, descendants := range r.groveClosure[space][treeID] {
			if depthToParent, hasParent := descendants[*parent]; hasParent {
				// ancestor is an ancestor of parent, so also ancestor of node
				r.groveClosure[space][treeID][ancestor][node] = depthToParent + 1
			}
		}

		// Add to parent's children list
		r.groveChildren[space][treeID][*parent] = append(r.groveChildren[space][treeID][*parent], node)
		// TODO: respect position for ordering
	}

	return nil
}

// DeleteNode deletes a node (soft or hard delete)
func (r *RamStore) DeleteNode(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID, soft bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return store_interface.ErrNodeNotFound
	}

	nodeObj, exists := r.groveNodes[space][treeID][node]
	if !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if node has children - for now, prevent deletion of non-leaf nodes
	if len(r.groveChildren[space][treeID][node]) > 0 {
		return fmt.Errorf("cannot delete node with children")
	}

	if soft {
		// Soft delete: move to deleted nodes map
		if r.groveDeletedNodes == nil {
			r.groveDeletedNodes = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]*nodeData)
		}
		if r.groveDeletedNodes[space] == nil {
			r.groveDeletedNodes[space] = make(map[store_interface.TreeID]map[store_interface.NodeID]*nodeData)
		}
		if r.groveDeletedNodes[space][treeID] == nil {
			r.groveDeletedNodes[space][treeID] = make(map[store_interface.NodeID]*nodeData)
		}
		r.groveDeletedNodes[space][treeID][node] = nodeObj
	}

	// Remove from parent's children list
	if nodeObj.parent != nil {
		children := r.groveChildren[space][treeID][*nodeObj.parent]
		for i, child := range children {
			if child == node {
				r.groveChildren[space][treeID][*nodeObj.parent] = append(children[:i], children[i+1:]...)
				break
			}
		}
	}

	// Remove from closure table (all ancestor relationships)
	for ancestor := range r.groveClosure[space][treeID] {
		delete(r.groveClosure[space][treeID][ancestor], node)
	}
	delete(r.groveClosure[space][treeID], node)

	// Remove from nodes
	delete(r.groveNodes[space][treeID], node)

	return nil
}

// MoveNode moves a node to a new parent
func (r *RamStore) MoveNode(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	newParent *store_interface.NodeID,
	newPosition *store_interface.ChildPosition,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return store_interface.ErrNodeNotFound
	}

	nodeObj, exists := r.groveNodes[space][treeID][node]
	if !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if new parent exists (if specified)
	var newDepth int
	if newParent != nil {
		newParentNode, exists := r.groveNodes[space][treeID][*newParent]
		if !exists {
			return store_interface.ErrNodeNotFound
		}

		// Check for cycles: newParent cannot be a descendant of node
		if r.isDescendant(space, treeID, node, *newParent) {
			return store_interface.ErrCycleDetected
		}

		newDepth = newParentNode.depth + 1
	} else {
		newDepth = 0
	}

	// Remove from old parent's children list
	if nodeObj.parent != nil {
		children := r.groveChildren[space][treeID][*nodeObj.parent]
		for i, child := range children {
			if child == node {
				r.groveChildren[space][treeID][*nodeObj.parent] = append(children[:i], children[i+1:]...)
				break
			}
		}
	}

	// Update closure table: remove old ancestor relationships
	descendants := r.getDescendantsInternal(space, treeID, node)
	descendants = append(descendants, node) // include node itself

	for _, desc := range descendants {
		// Remove all old ancestors (except self-reference)
		for ancestor := range r.groveClosure[space][treeID] {
			if ancestor != desc {
				delete(r.groveClosure[space][treeID][ancestor], desc)
			}
		}
	}

	// Update node's parent and depth
	depthDelta := newDepth - nodeObj.depth
	nodeObj.parent = newParent
	nodeObj.position = newPosition
	nodeObj.depth = newDepth

	// Update depth for all descendants
	for _, desc := range descendants {
		if desc != node {
			descNode := r.groveNodes[space][treeID][desc]
			descNode.depth += depthDelta
		}
	}

	// Rebuild closure table for node and descendants
	// For each descendant, add relationships to all new ancestors
	for _, desc := range descendants {
		descNode := r.groveNodes[space][treeID][desc]
		relativeDepth := descNode.depth - nodeObj.depth

		// Add self-reference if not already present
		if r.groveClosure[space][treeID][desc] == nil {
			r.groveClosure[space][treeID][desc] = make(map[store_interface.NodeID]int)
		}
		r.groveClosure[space][treeID][desc][desc] = 0

		// Add node as ancestor of descendants (if desc != node)
		if desc != node {
			if r.groveClosure[space][treeID][node] == nil {
				r.groveClosure[space][treeID][node] = make(map[store_interface.NodeID]int)
			}
			r.groveClosure[space][treeID][node][desc] = relativeDepth
		}

		if newParent != nil {
			// Find all ancestors of newParent and add desc to their descendant lists
			for ancestor, descendantsOfAncestor := range r.groveClosure[space][treeID] {
				if depthToParent, hasParent := descendantsOfAncestor[*newParent]; hasParent {
					// ancestor is an ancestor of newParent, so also ancestor of desc
					r.groveClosure[space][treeID][ancestor][desc] = depthToParent + 1 + relativeDepth
				}
			}
		}
	}

	// Add to new parent's children list
	if newParent != nil {
		r.groveChildren[space][treeID][*newParent] = append(r.groveChildren[space][treeID][*newParent], node)
		// TODO: respect newPosition for ordering
	}

	return nil
}

// RestoreNode restores a soft-deleted node
func (r *RamStore) RestoreNode(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.groveDeletedNodes == nil || r.groveDeletedNodes[space] == nil || r.groveDeletedNodes[space][treeID] == nil {
		return store_interface.ErrNodeNotFound
	}

	nodeObj, exists := r.groveDeletedNodes[space][treeID][node]
	if !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if parent still exists (if node had a parent)
	if nodeObj.parent != nil {
		if _, exists := r.groveNodes[space][treeID][*nodeObj.parent]; !exists {
			return store_interface.ErrNodeNotFound
		}
	}

	// Remove from deleted nodes
	delete(r.groveDeletedNodes[space][treeID], node)

	// Restore node data
	r.groveNodes[space][treeID][node] = nodeObj

	// Rebuild closure table relationships
	if r.groveClosure[space][treeID][node] == nil {
		r.groveClosure[space][treeID][node] = make(map[store_interface.NodeID]int)
	}
	r.groveClosure[space][treeID][node][node] = 0

	if nodeObj.parent != nil {
		// Find all ancestors of parent and add node to their descendant lists
		for ancestor, descendants := range r.groveClosure[space][treeID] {
			if depthToParent, hasParent := descendants[*nodeObj.parent]; hasParent {
				r.groveClosure[space][treeID][ancestor][node] = depthToParent + 1
			}
		}

		// Add to parent's children list
		r.groveChildren[space][treeID][*nodeObj.parent] = append(r.groveChildren[space][treeID][*nodeObj.parent], node)
	}

	return nil
}

// Exists checks if a node exists
func (r *RamStore) Exists(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return false, nil
	}

	_, exists := r.groveNodes[space][treeID][node]
	return exists, nil
}

// GetNodeInfo gets complete node information
func (r *RamStore) GetNodeInfo(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) (*store_interface.NodeInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, store_interface.ErrNodeNotFound
	}

	nodeObj, exists := r.groveNodes[space][treeID][node]
	if !exists {
		return nil, store_interface.ErrNodeNotFound
	}

	return &store_interface.NodeInfo{
		ID:       nodeObj.id,
		Parent:   nodeObj.parent,
		Position: nodeObj.position,
		Depth:    nodeObj.depth,
		Metadata: nodeObj.metadata,
	}, nil
}

// GetChildren gets children of a node (with pagination stub)
func (r *RamStore) GetChildren(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	children := r.groveChildren[space][treeID][node]
	if children == nil {
		children = []store_interface.NodeID{}
	}

	// TODO: implement pagination
	return children, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// GetAncestors gets all ancestors of a node (with pagination stub)
func (r *RamStore) GetAncestors(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	var ancestors []store_interface.NodeID
	for ancestor, depth := range r.groveClosure[space][treeID] {
		if _, isAncestor := depth[node]; isAncestor && ancestor != node {
			ancestors = append(ancestors, ancestor)
		}
	}

	// TODO: Sort by depth and implement pagination
	return ancestors, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// GetDescendants gets all descendants of a node (with pagination stub)
func (r *RamStore) GetDescendants(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	opts *store_interface.DescendantOptions,
) ([]store_interface.NodeWithDepth, *store_interface.PaginationResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	var result []store_interface.NodeWithDepth

	// Get descendants from closure table
	descendants := r.groveClosure[space][treeID][node]
	for desc, relativeDepth := range descendants {
		// Skip self if not including node itself
		if desc == node {
			continue
		}

		// Apply maxDepth filter if specified
		if opts != nil && opts.MaxDepth != nil && relativeDepth > *opts.MaxDepth {
			continue
		}

		result = append(result, store_interface.NodeWithDepth{
			NodeID: desc,
			Depth:  relativeDepth,
		})
	}

	// TODO: Implement breadth-first vs depth-first ordering, pagination
	return result, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// ApplyAggregateMutation applies aggregate deltas to a node
func (r *RamStore) ApplyAggregateMutation(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	mutation store_interface.MutationID,
	node store_interface.NodeID,
	deltas store_interface.AggregateDeltas,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Initialize maps
	if r.groveMutations == nil {
		r.groveMutations = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.MutationID]bool)
	}
	if r.groveMutations[space] == nil {
		r.groveMutations[space] = make(map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.MutationID]bool)
	}
	if r.groveMutations[space][treeID] == nil {
		r.groveMutations[space][treeID] = make(map[store_interface.NodeID]map[store_interface.MutationID]bool)
	}
	if r.groveMutations[space][treeID][node] == nil {
		r.groveMutations[space][treeID][node] = make(map[store_interface.MutationID]bool)
	}
	if r.groveAggregates == nil {
		r.groveAggregates = make(map[store_interface.TenancySpace]map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.AggregateKey]store_interface.AggregateValue)
	}
	if r.groveAggregates[space] == nil {
		r.groveAggregates[space] = make(map[store_interface.TreeID]map[store_interface.NodeID]map[store_interface.AggregateKey]store_interface.AggregateValue)
	}
	if r.groveAggregates[space][treeID] == nil {
		r.groveAggregates[space][treeID] = make(map[store_interface.NodeID]map[store_interface.AggregateKey]store_interface.AggregateValue)
	}
	if r.groveAggregates[space][treeID][node] == nil {
		r.groveAggregates[space][treeID][node] = make(map[store_interface.AggregateKey]store_interface.AggregateValue)
	}

	// Check if node exists
	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return store_interface.ErrNodeNotFound
	}
	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if mutation already applied
	if r.groveMutations[space][treeID][node][mutation] {
		return store_interface.ErrMutationConflict
	}

	// Apply deltas
	for key, delta := range deltas {
		r.groveAggregates[space][treeID][node][key] += delta
	}

	// Mark mutation as applied
	r.groveMutations[space][treeID][node][mutation] = true

	return nil
}

// GetNodeLocalAggregates gets aggregates for the node only
func (r *RamStore) GetNodeLocalAggregates(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, store_interface.ErrNodeNotFound
	}
	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return nil, store_interface.ErrNodeNotFound
	}

	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)
	if r.groveAggregates != nil && r.groveAggregates[space] != nil && r.groveAggregates[space][treeID] != nil && r.groveAggregates[space][treeID][node] != nil {
		for k, v := range r.groveAggregates[space][treeID][node] {
			result[k] = v
		}
	}

	return result, nil
}

// GetNodeWithDescendantsAggregates gets aggregates for node + all descendants
func (r *RamStore) GetNodeWithDescendantsAggregates(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.groveNodes == nil || r.groveNodes[space] == nil || r.groveNodes[space][treeID] == nil {
		return nil, store_interface.ErrNodeNotFound
	}
	if _, exists := r.groveNodes[space][treeID][node]; !exists {
		return nil, store_interface.ErrNodeNotFound
	}

	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)

	// Include node's own aggregates
	if r.groveAggregates != nil && r.groveAggregates[space] != nil && r.groveAggregates[space][treeID] != nil && r.groveAggregates[space][treeID][node] != nil {
		for k, v := range r.groveAggregates[space][treeID][node] {
			result[k] += v
		}
	}

	// Include all descendants' aggregates
	if r.groveClosure != nil && r.groveClosure[space] != nil && r.groveClosure[space][treeID] != nil {
		descendants := r.groveClosure[space][treeID][node]
		for desc := range descendants {
			if desc != node && r.groveAggregates[space][treeID][desc] != nil {
				for k, v := range r.groveAggregates[space][treeID][desc] {
					result[k] += v
				}
			}
		}
	}

	return result, nil
}

// Helper functions (must be called with lock held)

func (r *RamStore) isDescendant(space store_interface.TenancySpace, treeID store_interface.TreeID, ancestor, node store_interface.NodeID) bool {
	if r.groveClosure == nil || r.groveClosure[space] == nil || r.groveClosure[space][treeID] == nil {
		return false
	}
	descendants := r.groveClosure[space][treeID][ancestor]
	_, isDesc := descendants[node]
	return isDesc
}

func (r *RamStore) getDescendantsInternal(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) []store_interface.NodeID {
	var result []store_interface.NodeID
	if r.groveClosure == nil || r.groveClosure[space] == nil || r.groveClosure[space][treeID] == nil {
		return result
	}
	descendants := r.groveClosure[space][treeID][node]
	for desc := range descendants {
		if desc != node {
			result = append(result, desc)
		}
	}
	return result
}
