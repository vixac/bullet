package migrator

import (
	"fmt"

	"github.com/vixac/bullet/store/store_interface"
)

type GroveMigrator struct {
	SourceGrove store_interface.GroveStore
	TargetGrove store_interface.GroveStore
	Tenancy     store_interface.TenancySpace
}

// MigrateTree migrates a single tree's node structure from source to target.
// Note: This migrates node hierarchy, positions, and metadata.
// Aggregate migrations are not supported yet as mutation enumeration is not available in the interface.
func (g *GroveMigrator) MigrateTree(treeID store_interface.TreeID, rootNode store_interface.NodeID) error {
	// Get all descendants in breadth-first order (ensures parents are created before children)
	opts := &store_interface.DescendantOptions{
		IncludeDepth: true,
		BreadthFirst: true,
	}

	descendants, _, err := g.SourceGrove.GetDescendants(g.Tenancy, treeID, rootNode, opts)
	if err != nil {
		return fmt.Errorf("failed to get descendants for tree %s: %w", treeID, err)
	}

	// Start with the root node
	allNodes := append([]store_interface.NodeWithDepth{{NodeID: rootNode, Depth: 0}}, descendants...)

	fmt.Printf("Grove: migrating tree %s with %d nodes\n", treeID, len(allNodes))

	// Migrate each node in breadth-first order
	for _, nodeWithDepth := range allNodes {
		nodeID := nodeWithDepth.NodeID

		// Get node info from source
		nodeInfo, err := g.SourceGrove.GetNodeInfo(g.Tenancy, treeID, nodeID)
		if err != nil {
			return fmt.Errorf("failed to get info for node %s: %w", nodeID, err)
		}

		// Create node in target
		err = g.TargetGrove.CreateNode(
			g.Tenancy,
			treeID,
			nodeID,
			nodeInfo.Parent,
			nodeInfo.Position,
			nodeInfo.Metadata,
		)
		if err != nil {
			return fmt.Errorf("failed to create node %s in target: %w", nodeID, err)
		}
	}

	fmt.Printf("Grove: successfully migrated tree %s (%d nodes)\n", treeID, len(allNodes))
	return nil
}

// MigrateTrees migrates multiple trees
func (g *GroveMigrator) MigrateTrees(trees map[store_interface.TreeID]store_interface.NodeID) error {
	for treeID, rootNode := range trees {
		if err := g.MigrateTree(treeID, rootNode); err != nil {
			return err
		}
	}
	return nil
}
