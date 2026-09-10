package postgresql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/vixac/bullet/model"
)

func (s *PostgreSQLStore) CreateNode(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	parent *model.NodeID,
	position *model.ChildPosition,
	metadata *model.NodeMetadata,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return model.ErrNodeAlreadyExists
	}

	if parent != nil {
		var parentExists bool
		err = tx.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM grove_nodes
				WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
			)`, space.AppId, space.TenancyId, string(treeID), string(*parent)).Scan(&parentExists)
		if err != nil {
			return err
		}
		if !parentExists {
			return model.ErrNodeNotFound
		}
	}

	var metadataJSON *string
	if metadata != nil {
		data, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		s := string(data)
		metadataJSON = &s
	}

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
		INSERT INTO grove_nodes (app_id, tenancy_id, tree_id, node_id, parent_id, position, metadata, is_deleted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)`,
		space.AppId, space.TenancyId, string(treeID), string(node), parentIDStr, positionVal, metadataJSON)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
		VALUES ($1, $2, $3, $4, $5, 0)`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(node))
	if err != nil {
		return err
	}

	if parent != nil {
		_, err = tx.Exec(`
			INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
			SELECT app_id, tenancy_id, tree_id, ancestor_id, $1, depth + 1
			FROM grove_closure
			WHERE app_id=$2 AND tenancy_id=$3 AND tree_id=$4 AND descendant_id=$5`,
			string(node), space.AppId, space.TenancyId, string(treeID), string(*parent))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *PostgreSQLStore) DeleteNode(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	soft bool,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return model.ErrNodeNotFound
	}

	var hasChildren bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND parent_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&hasChildren)
	if err != nil {
		return err
	}
	if hasChildren {
		return model.ErrNodeNotFound
	}

	if soft {
		_, err = tx.Exec(`
			UPDATE grove_nodes SET is_deleted=TRUE
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4`,
			space.AppId, space.TenancyId, string(treeID), string(node))
	} else {
		_, err = tx.Exec(`
			DELETE FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4`,
			space.AppId, space.TenancyId, string(treeID), string(node))
	}
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		DELETE FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND descendant_id=$4`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		DELETE FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND ancestor_id=$4`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *PostgreSQLStore) MoveNode(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	newParent *model.NodeID,
	newPosition *model.ChildPosition,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return model.ErrNodeNotFound
	}

	if newParent != nil {
		err = tx.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM grove_nodes
				WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
			)`, space.AppId, space.TenancyId, string(treeID), string(*newParent)).Scan(&exists)
		if err != nil {
			return err
		}
		if !exists {
			return model.ErrNodeNotFound
		}

		var isCycle bool
		err = tx.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM grove_closure
				WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND ancestor_id=$4 AND descendant_id=$5
			)`, space.AppId, space.TenancyId, string(treeID), string(node), string(*newParent)).Scan(&isCycle)
		if err != nil {
			return err
		}
		if isCycle {
			return model.ErrCycleDetected
		}
	}

	rows, err := tx.Query(`
		SELECT descendant_id, depth FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND ancestor_id=$4`,
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

	// Remove external ancestor relationships while preserving intra-subtree ones.
	_, err = tx.Exec(`
		DELETE FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3
		AND descendant_id IN (
			SELECT descendant_id FROM grove_closure
			WHERE app_id=$4 AND tenancy_id=$5 AND tree_id=$6 AND ancestor_id=$7
		)
		AND ancestor_id NOT IN (
			SELECT descendant_id FROM grove_closure
			WHERE app_id=$8 AND tenancy_id=$9 AND tree_id=$10 AND ancestor_id=$11
		)`,
		space.AppId, space.TenancyId, string(treeID),
		space.AppId, space.TenancyId, string(treeID), string(node),
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

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
		UPDATE grove_nodes SET parent_id=$1, position=$2
		WHERE app_id=$3 AND tenancy_id=$4 AND tree_id=$5 AND node_id=$6`,
		parentIDStr, positionVal, space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return err
	}

	if newParent != nil {
		for _, desc := range descendants {
			_, err = tx.Exec(`
				INSERT INTO grove_closure (app_id, tenancy_id, tree_id, ancestor_id, descendant_id, depth)
				SELECT app_id, tenancy_id, tree_id, ancestor_id, $1, depth + $2 + 1
				FROM grove_closure
				WHERE app_id=$3 AND tenancy_id=$4 AND tree_id=$5 AND descendant_id=$6`,
				desc.id, desc.depth, space.AppId, space.TenancyId, string(treeID), string(*newParent))
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

func (s *PostgreSQLStore) Exists(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
) (bool, error) {
	var exists bool
	err := s.db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	return exists, err
}

func (s *PostgreSQLStore) GetNodeInfo(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
) (*model.NodeInfo, error) {
	var parentIDStr *string
	var positionVal *float64
	var depth int
	var metadataJSON *string

	err := s.db.QueryRow(`
		SELECT
			n.parent_id,
			n.position,
			n.metadata,
			COALESCE((
				SELECT MAX(depth) FROM grove_closure
				WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND descendant_id=$4
			), 0) as depth
		FROM grove_nodes n
		WHERE n.app_id=$5 AND n.tenancy_id=$6 AND n.tree_id=$7 AND n.node_id=$8 AND n.is_deleted=FALSE`,
		space.AppId, space.TenancyId, string(treeID), string(node),
		space.AppId, space.TenancyId, string(treeID), string(node),
	).Scan(&parentIDStr, &positionVal, &metadataJSON, &depth)
	if err == sql.ErrNoRows {
		return nil, model.ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}

	var parent *model.NodeID
	if parentIDStr != nil {
		p := model.NodeID(*parentIDStr)
		parent = &p
	}

	var position *model.ChildPosition
	if positionVal != nil {
		p := model.ChildPosition(*positionVal)
		position = &p
	}

	var metadata *model.NodeMetadata
	if metadataJSON != nil {
		var m model.NodeMetadata
		if err := json.Unmarshal([]byte(*metadataJSON), &m); err != nil {
			return nil, err
		}
		metadata = &m
	}

	return &model.NodeInfo{
		ID:       node,
		Parent:   parent,
		Position: position,
		Depth:    depth,
		Metadata: metadata,
	}, nil
}

func (s *PostgreSQLStore) GetChildren(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	pagination *model.PaginationParams,
) ([]model.NodeID, *model.PaginationResult, error) {
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, model.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT node_id FROM grove_nodes
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND parent_id=$4 AND is_deleted=FALSE
		ORDER BY position, node_id`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var children []model.NodeID
	for rows.Next() {
		var childID string
		if err := rows.Scan(&childID); err != nil {
			return nil, nil, err
		}
		children = append(children, model.NodeID(childID))
	}

	return children, &model.PaginationResult{NextCursor: nil}, nil
}

func (s *PostgreSQLStore) GetAncestors(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	pagination *model.PaginationParams,
) ([]model.NodeID, *model.PaginationResult, error) {
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, model.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT ancestor_id FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND descendant_id=$4 AND ancestor_id!=$5
		ORDER BY depth DESC`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(node))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var ancestors []model.NodeID
	for rows.Next() {
		var ancestorID string
		if err := rows.Scan(&ancestorID); err != nil {
			return nil, nil, err
		}
		ancestors = append(ancestors, model.NodeID(ancestorID))
	}

	return ancestors, &model.PaginationResult{NextCursor: nil}, nil
}

func (s *PostgreSQLStore) GetAncestorsBulk(
	space model.TenancySpace,
	treeID model.TreeID,
	nodes []model.NodeID,
) (map[model.NodeID][]model.NodeID, []model.NodeID, error) {
	if len(nodes) == 0 {
		return map[model.NodeID][]model.NodeID{}, nil, nil
	}

	args := []interface{}{space.AppId, space.TenancyId, string(treeID)}
	for _, node := range nodes {
		args = append(args, string(node))
	}

	query := `
		SELECT descendant_id, ancestor_id FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3
		AND descendant_id IN (` + placeholders(4, len(nodes)) + `)
		ORDER BY descendant_id, depth DESC`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[model.NodeID][]model.NodeID)
	seen := make(map[model.NodeID]bool)

	for rows.Next() {
		var descID, ancID string
		if err := rows.Scan(&descID, &ancID); err != nil {
			return nil, nil, err
		}
		nodeID := model.NodeID(descID)
		seen[nodeID] = true
		if ancID != descID {
			result[nodeID] = append(result[nodeID], model.NodeID(ancID))
		} else if _, ok := result[nodeID]; !ok {
			result[nodeID] = []model.NodeID{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var notFound []model.NodeID
	for _, node := range nodes {
		if !seen[node] {
			notFound = append(notFound, node)
		}
	}

	return result, notFound, nil
}

func (s *PostgreSQLStore) GetDescendants(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
	opts *model.DescendantOptions,
) ([]model.NodeWithDepth, *model.PaginationResult, error) {
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, model.ErrNodeNotFound
	}

	query := `
		SELECT descendant_id, depth FROM grove_closure
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND ancestor_id=$4 AND descendant_id!=$5`

	args := []interface{}{space.AppId, space.TenancyId, string(treeID), string(node), string(node)}

	if opts != nil && opts.MaxDepth != nil {
		query += fmt.Sprintf(" AND depth<=$%d", len(args)+1)
		args = append(args, *opts.MaxDepth)
	}

	query += " ORDER BY depth"

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var descendants []model.NodeWithDepth
	for rows.Next() {
		var descID string
		var depth int
		if err := rows.Scan(&descID, &depth); err != nil {
			return nil, nil, err
		}
		descendants = append(descendants, model.NodeWithDepth{
			NodeID: model.NodeID(descID),
			Depth:  depth,
		})
	}

	return descendants, &model.PaginationResult{NextCursor: nil}, nil
}

func (s *PostgreSQLStore) ApplyAggregateMutation(
	space model.TenancySpace,
	treeID model.TreeID,
	mutation model.MutationID,
	node model.NodeID,
	deltas model.AggregateDeltas,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var exists bool
	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_nodes
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND is_deleted=FALSE
		)`, space.AppId, space.TenancyId, string(treeID), string(node)).Scan(&exists)
	if err != nil {
		return err
	}
	if !exists {
		return model.ErrNodeNotFound
	}

	err = tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM grove_mutations
			WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4 AND mutation_id=$5
		)`, space.AppId, space.TenancyId, string(treeID), string(node), string(mutation)).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return model.ErrMutationConflict
	}

	for key, delta := range deltas {
		_, err = tx.Exec(`
			INSERT INTO grove_aggregates (app_id, tenancy_id, tree_id, node_id, aggregate_key, aggregate_value)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT(app_id, tenancy_id, tree_id, node_id, aggregate_key)
			DO UPDATE SET aggregate_value = grove_aggregates.aggregate_value + excluded.aggregate_value`,
			space.AppId, space.TenancyId, string(treeID), string(node), string(key), delta)
		if err != nil {
			return err
		}
	}

	_, err = tx.Exec(`
		INSERT INTO grove_mutations (app_id, tenancy_id, tree_id, node_id, mutation_id)
		VALUES ($1, $2, $3, $4, $5)`,
		space.AppId, space.TenancyId, string(treeID), string(node), string(mutation))
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *PostgreSQLStore) GetNodeLocalAggregates(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
) (map[model.AggregateKey]model.AggregateValue, error) {
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, model.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT aggregate_key, aggregate_value FROM grove_aggregates
		WHERE app_id=$1 AND tenancy_id=$2 AND tree_id=$3 AND node_id=$4`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[model.AggregateKey]model.AggregateValue)
	for rows.Next() {
		var key string
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[model.AggregateKey(key)] = model.AggregateValue(value)
	}
	return result, nil
}

func (s *PostgreSQLStore) GetNodeLocalAggregatesBulk(
	space model.TenancySpace,
	treeID model.TreeID,
	nodes []model.NodeID,
) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	if len(nodes) == 0 {
		return map[model.NodeID]map[model.AggregateKey]model.AggregateValue{}, nil, nil
	}

	args := []interface{}{space.AppId, space.TenancyId, string(treeID)}
	for _, node := range nodes {
		args = append(args, string(node))
	}

	phs := placeholders(4, len(nodes))
	query := `
		SELECT gn.node_id, ga.aggregate_key, ga.aggregate_value
		FROM grove_nodes gn
		LEFT JOIN grove_aggregates ga
		  ON  gn.app_id     = ga.app_id
		  AND gn.tenancy_id = ga.tenancy_id
		  AND gn.tree_id    = ga.tree_id
		  AND gn.node_id    = ga.node_id
		WHERE gn.app_id     = $1
		  AND gn.tenancy_id = $2
		  AND gn.tree_id    = $3
		  AND gn.node_id    IN (` + phs + `)
		  AND gn.is_deleted = FALSE`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[model.NodeID]map[model.AggregateKey]model.AggregateValue)

	for rows.Next() {
		var nodeStr string
		var aggKey *string
		var aggVal *int64
		if err := rows.Scan(&nodeStr, &aggKey, &aggVal); err != nil {
			return nil, nil, err
		}
		nodeID := model.NodeID(nodeStr)
		if _, ok := result[nodeID]; !ok {
			result[nodeID] = make(map[model.AggregateKey]model.AggregateValue)
		}
		if aggKey != nil {
			result[nodeID][model.AggregateKey(*aggKey)] = model.AggregateValue(*aggVal)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var notFound []model.NodeID
	for _, node := range nodes {
		if _, ok := result[node]; !ok {
			notFound = append(notFound, node)
		}
	}
	return result, notFound, nil
}

func (s *PostgreSQLStore) GetNodeWithDescendantsAggregatesBulk(
	space model.TenancySpace,
	treeID model.TreeID,
	nodes []model.NodeID,
) (map[model.NodeID]map[model.AggregateKey]model.AggregateValue, []model.NodeID, error) {
	if len(nodes) == 0 {
		return map[model.NodeID]map[model.AggregateKey]model.AggregateValue{}, nil, nil
	}

	args := []interface{}{space.AppId, space.TenancyId, string(treeID)}
	for _, node := range nodes {
		args = append(args, string(node))
	}

	phs := placeholders(4, len(nodes))
	query := `
		SELECT gc.ancestor_id, ga.aggregate_key, SUM(ga.aggregate_value)
		FROM grove_closure gc
		LEFT JOIN grove_aggregates ga
		  ON  ga.app_id     = gc.app_id
		  AND ga.tenancy_id = gc.tenancy_id
		  AND ga.tree_id    = gc.tree_id
		  AND ga.node_id    = gc.descendant_id
		WHERE gc.app_id     = $1
		  AND gc.tenancy_id = $2
		  AND gc.tree_id    = $3
		  AND gc.ancestor_id IN (` + phs + `)
		GROUP BY gc.ancestor_id, ga.aggregate_key`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	result := make(map[model.NodeID]map[model.AggregateKey]model.AggregateValue)

	for rows.Next() {
		var nodeStr string
		var aggKey *string
		var aggVal *int64
		if err := rows.Scan(&nodeStr, &aggKey, &aggVal); err != nil {
			return nil, nil, err
		}
		nodeID := model.NodeID(nodeStr)
		if _, ok := result[nodeID]; !ok {
			result[nodeID] = make(map[model.AggregateKey]model.AggregateValue)
		}
		if aggKey != nil {
			result[nodeID][model.AggregateKey(*aggKey)] = model.AggregateValue(*aggVal)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var notFound []model.NodeID
	for _, node := range nodes {
		if _, ok := result[node]; !ok {
			notFound = append(notFound, node)
		}
	}
	return result, notFound, nil
}

func (s *PostgreSQLStore) GetNodeWithDescendantsAggregates(
	space model.TenancySpace,
	treeID model.TreeID,
	node model.NodeID,
) (map[model.AggregateKey]model.AggregateValue, error) {
	exists, err := s.Exists(space, treeID, node)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, model.ErrNodeNotFound
	}

	rows, err := s.db.Query(`
		SELECT ga.aggregate_key, SUM(ga.aggregate_value) as total
		FROM grove_aggregates ga
		INNER JOIN grove_closure gc ON
			ga.app_id     = gc.app_id AND
			ga.tenancy_id = gc.tenancy_id AND
			ga.tree_id    = gc.tree_id AND
			ga.node_id    = gc.descendant_id
		WHERE gc.app_id=$1 AND gc.tenancy_id=$2 AND gc.tree_id=$3 AND gc.ancestor_id=$4
		GROUP BY ga.aggregate_key`,
		space.AppId, space.TenancyId, string(treeID), string(node))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[model.AggregateKey]model.AggregateValue)
	for rows.Next() {
		var key string
		var value int64
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		result[model.AggregateKey(key)] = model.AggregateValue(value)
	}
	return result, nil
}
