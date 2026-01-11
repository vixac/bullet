package sqlite_store

import (
	"database/sql"
	"encoding/json"

	"github.com/vixac/bullet/store/store_interface"
)

// CreateNode creates a new node in the tree
func (s *SQLiteStore) CreateNode(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	parent *store_interface.NodeID,
	position *store_interface.ChildPosition,
	metadata *store_interface.NodeMetadata,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if node already exists
	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return store_interface.ErrNodeAlreadyExists
	}

	// Calculate depth
	var depth int
	if parent != nil {
		err = tx.QueryRow(`
			SELECT depth FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0`,
			space.AppId, space.TenancyId, string(treeID), string(*parent)).Scan(&depth)
		if err == sql.ErrNoRows {
			return store_interface.ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		depth++
	}

	// Serialize metadata
	var metadataJSON *string
	if metadata != nil {
		data, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		metadataStr := string(data)
		metadataJSON = &metadataStr
	}

	// Insert node
	var parentIDStr *string
	if parent != nil {
		p := string(*parent)
		parentIDStr = &p
	}
	var positionVal *float64
	if position != nil {
		p := float64(*position)
		positionVal = &p
	}

	_, err = tx.Exec(`
		INSERT INTO grove_nodes (app_id, tenancy_id, tree_id, node_id, parent_id, position, depth, metadata, is_deleted)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		space.AppId, space.TenancyId, string(treeID), string(node), parentIDStr, positionVal, depth, metadataJSON)
	if err != nil {
		return err
	}

	// Insert self-reference in closure table
	_, err = tx.Exec(`
		INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
		VALUES (?, ?, ?, ?, ?, 0)`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(node))
	if err != nil {
		return err
	}

	// If has parent, add relationships to all ancestors
	if parent != nil {
		_, err = tx.Exec(`
			INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
			SELECT app_id, tenancy_id, tree_id, ancestor_id, ?, depth + 1
			FROM grove_closure
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND descendant_id = ?`,
			string(node), space.AppId, space.TenancyId, string(treeID), string(*parent))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteNode deletes a node (soft or hard delete)
func (s *SQLiteStore) DeleteNode(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID, soft bool) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if node exists
	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if node has children
	var hasChildren bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND parent_id = ? AND is_deleted = 0
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&hasChildren)
	if err != nil {
		return err
	}
	if hasChildren {
		return store_interface.ErrNodeNotFound // Using this error for "cannot delete node with children"
	}

	if soft {
		// Soft delete: mark as deleted
		_, err = tx.Exec(`
			UPDATE grove_nodes SET is_deleted = 1
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ?`,
			space.AppId, space.TenancyId, string(treeID), string(node))
		if err != nil {
			return err
		}
	} else {
		// Hard delete: remove from nodes table
		_, err = tx.Exec(`
			DELETE FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ?`,
			space.AppId, space.TenancyId, string(treeID), string(node))
		if err != nil {
			return err
		}
	}

	// Remove from closure table
	_, err = tx.Exec(`
		DELETE FROM grove_closure
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND descendant_id = ?`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		DELETE FROM grove_closure
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND ancestor_id = ?`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	return tx.Commit()
}

// MoveNode moves a node to a new parent
func (s *SQLiteStore) MoveNode(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	newParent *store_interface.NodeID,
	newPosition *store_interface.ChildPosition,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if node exists
	var currentDepth int
	err = tx.QueryRow(`
		SELECT depth FROM grove_nodes
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0`,
		space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&currentDepth)
	if err == sql.ErrNoRows {
		return store_interface.ErrNodeNotFound
	}
	if err != nil {
		return err
	}

	// Calculate new depth
	var newDepth int
	if newParent != nil {
		err = tx.QueryRow(`
			SELECT depth FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0`,
			space.AppId, space.TenancyId, string(treeID), string(*newParent)).Scan(&newDepth)
		if err == sql.ErrNoRows {
			return store_interface.ErrNodeNotFound
		}
		if err != nil {
			return err
		}

		// Check for cycles: newParent cannot be a descendant of node
		var isCycle bool
		err = tx.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM grove_closure
				WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND ancestor_id = ? AND descendant_id = ?
			)`, space.AppId, space.TenancyId, string(treeID), string(node), string(*newParent)).Scan(&isCycle)
		if err != nil {
			return err
		}
		if isCycle {
			return store_interface.ErrCycleDetected
		}

		newDepth++
	}

	depthDelta := newDepth - currentDepth

	// Get all descendants (including node itself)
	rows, err := tx.Query(`
		SELECT descendant_id, depth FROM grove_closure
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND ancestor_id = ?`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	type descendantInfo struct {
		id    string
		depth int
	}
	var descendants []descendantInfo
	for rows.Next() {
		var d descendantInfo
		if err := rows.Scan(&d.id, &d.depth); err != nil {
			rows.Close()
			return err
		}
		descendants = append(descendants, d)
	}
	rows.Close()

	// Remove old ancestor relationships (except self-references)
	for _, desc := range descendants {
		_, err = tx.Exec(`
			DELETE FROM grove_closure
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND descendant_id = ? AND ancestor_id != ?`,
			space.AppId, space.TenancyId, string(treeID), desc.id, desc.id)
		if err != nil {
			return err
		}
	}

	// Update node's parent and depth
	var parentIDStr *string
	if newParent != nil {
		p := string(*newParent)
		parentIDStr = &p
	}
	var positionVal *float64
	if newPosition != nil {
		p := float64(*newPosition)
		positionVal = &p
	}

	_, err = tx.Exec(`
		UPDATE grove_nodes SET parent_id = ?, position = ?, depth = ?
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ?`,
		parentIDStr, positionVal, newDepth, space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	// Update depth for all descendants
	for _, desc := range descendants {
		if desc.id != string(node) {
			_, err = tx.Exec(`
				UPDATE grove_nodes SET depth = depth + ?
				WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ?`,
				depthDelta, space.AppId, space.TenancyId, string(treeID), desc.id)
			if err != nil {
				return err
			}
		}
	}

	// Rebuild closure table for node and descendants
	if newParent != nil {
		for _, desc := range descendants {
			relativeDepth := desc.depth
			_, err = tx.Exec(`
				INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
				SELECT app_id, tenancy_id, tree_id, ancestor_id, ?, depth + ? + 1
				FROM grove_closure
				WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND descendant_id = ?`,
				desc.id, relativeDepth, space.AppId, space.TenancyId, string(treeID), string(*newParent))
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// Exists checks if a node exists
func (s *SQLiteStore) Exists(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) (bool, error) {
	var exists bool
	err := s.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	return exists, err
}

// GetNodeInfo gets complete node information
func (s *SQLiteStore) GetNodeInfo(space store_interface.TenancySpace, treeID store_interface.TreeID, node store_interface.NodeID) (*store_interface.NodeInfo, error) {
	var parentIDStr *string
	var positionVal *float64
	var depth int
	var metadataJSON *string

	err := s.db.QueryRow(`
		SELECT parent_id, position, depth, metadata
		FROM grove_nodes
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0`,
		space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&parentIDStr, &positionVal, &depth, &metadataJSON)
	if err == sql.ErrNoRows {
		return nil, store_interface.ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}

	var parent *store_interface.NodeID
	if parentIDStr != nil {
		p := store_interface.NodeID(*parentIDStr)
		parent = &p
	}

	var position *store_interface.ChildPosition
	if positionVal != nil {
		p := store_interface.ChildPosition(*positionVal)
		position = &p
	}

	var metadata *store_interface.NodeMetadata
	if metadataJSON != nil {
		var m store_interface.NodeMetadata
		if err := json.Unmarshal([]byte(*metadataJSON), &m); err != nil {
			return nil, err
		}
		metadata = &m
	}

	return &store_interface.NodeInfo{
		ID:       node,
		Parent:   parent,
		Position: position,
		Depth:    depth,
		Metadata: metadata,
	}, nil
}

// GetChildren gets children of a node
func (s *SQLiteStore) GetChildren(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	// Check if node exists
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT node_id FROM grove_nodes
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND parent_id = ? AND is_deleted = 0
		ORDER BY position, node_id`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var children []store_interface.NodeID
	for rows.Next() {
		var childID string
		if err := rows.Scan(&childID); err != nil {
			return nil, nil, err
		}
		children = append(children, store_interface.NodeID(childID))
	}

	return children, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// GetAncestors gets all ancestors of a node
func (s *SQLiteStore) GetAncestors(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	pagination *store_interface.PaginationParams,
) ([]store_interface.NodeID, *store_interface.PaginationResult, error) {
	// Check if node exists
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT ancestor_id FROM grove_closure
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND descendant_id = ? AND ancestor_id != ?
		ORDER BY depth DESC`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(node))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var ancestors []store_interface.NodeID
	for rows.Next() {
		var ancestorID string
		if err := rows.Scan(&ancestorID); err != nil {
			return nil, nil, err
		}
		ancestors = append(ancestors, store_interface.NodeID(ancestorID))
	}

	return ancestors, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// GetDescendants gets all descendants of a node
func (s *SQLiteStore) GetDescendants(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
	opts *store_interface.DescendantOptions,
) ([]store_interface.NodeWithDepth, *store_interface.PaginationResult, error) {
	// Check if node exists
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, store_interface.ErrNodeNotFound
	}

	query := `
		SELECT descendant_id, depth FROM grove_closure
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND ancestor_id = ? AND descendant_id != ?`

	args := []interface{}{space.AppId, space.TenancyId, string(treeID), string(node), string(node)}

	if opts != nil && opts.MaxDepth != nil {
		query += ` AND depth <= ?`
		args = append(args, *opts.MaxDepth)
	}

	query += ` ORDER BY depth`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var descendants []store_interface.NodeWithDepth
	for rows.Next() {
		var descID string
		var depth int
		if err := rows.Scan(&descID, &depth); err != nil {
			return nil, nil, err
		}
		descendants = append(descendants, store_interface.NodeWithDepth{
			NodeID: store_interface.NodeID(descID),
			Depth:  depth,
		})
	}

	return descendants, &store_interface.PaginationResult{NextCursor: nil}, nil
}

// ApplyAggregateMutation applies aggregate deltas to a node
func (s *SQLiteStore) ApplyAggregateMutation(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	mutation store_interface.MutationID,
	node store_interface.NodeID,
	deltas store_interface.AggregateDeltas,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if node exists
	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND is_deleted = 0
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return store_interface.ErrNodeNotFound
	}

	// Check if mutation already applied
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_mutations
			WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ? AND mutation_id = ?
		)`, space.AppId, space.TenancyId, string(treeID), string(node), string(mutation)).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return store_interface.ErrMutationConflict
	}

	// Apply deltas
	for key, delta := range deltas {
		_, err = tx.Exec(`
			INSERT INTO grove_aggregates (app_id, tenancy_id, tree_id, node_id, aggregate_key, aggregate_value)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(app_id, tenancy_id, tree_id, node_id, aggregate_key)
			DO UPDATE SET aggregate_value = aggregate_value + ?`,
			space.AppId, space.TenancyId, string(treeID), string(node), string(key), delta, delta)
		if err != nil {
			return err
		}
	}

	// Mark mutation as applied
	_, err = tx.Exec(`
		INSERT INTO grove_mutations (app_id, tenancy_id, tree_id, node_id, mutation_id)
		VALUES (?, ?, ?, ?, ?)`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(mutation))
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GetNodeLocalAggregates gets aggregates for the node only
func (s *SQLiteStore) GetNodeLocalAggregates(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	// Check if node exists
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store_interface.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT aggregate_key, aggregate_value FROM grove_aggregates
		WHERE app_id = ? AND tenancy_id = ? AND tree_id = ? AND node_id = ?`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)
	for rows.Next() {
		var key string
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[store_interface.AggregateKey(key)] = store_interface.AggregateValue(value)
	}

	return result, nil
}

// GetNodeWithDescendantsAggregates gets aggregates for node + all descendants
func (s *SQLiteStore) GetNodeWithDescendantsAggregates(
	space store_interface.TenancySpace,
	treeID store_interface.TreeID,
	node store_interface.NodeID,
) (map[store_interface.AggregateKey]store_interface.AggregateValue, error) {
	// Check if node exists
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store_interface.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT ga.aggregate_key, SUM(ga.aggregate_value) as total
		FROM grove_aggregates ga
		INNER JOIN grove_closure gc ON
			ga.app_id = gc.app_id AND
			ga.tenancy_id = gc.tenancy_id AND
			ga.tree_id = gc.tree_id AND
			ga.node_id = gc.descendant_id
		WHERE gc.app_id = ? AND gc.tenancy_id = ? AND gc.tree_id = ? AND gc.ancestor_id = ?
		GROUP BY ga.aggregate_key`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[store_interface.AggregateKey]store_interface.AggregateValue)
	for rows.Next() {
		var key string
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[store_interface.AggregateKey(key)] = store_interface.AggregateValue(value)
	}

	return result, nil
}
