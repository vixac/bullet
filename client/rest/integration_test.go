package rest_test

import (
	"context"
	"math"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/api"
	"github.com/vixac/bullet/client"
	"github.com/vixac/bullet/client/local"
	"github.com/vixac/bullet/client/rest"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/ram"
	sqlite "github.com/vixac/bullet/store/sqlite"
	"github.com/vixac/bullet/store/store_interface"
)

type clientPair struct {
	name               string
	local, rest, other client.Client
}

func buildClientPairs(t *testing.T) []clientPair {
	t.Helper()
	gin.SetMode(gin.TestMode)
	disk, err := sqlite.NewSQLiteStore(filepath.Join(t.TempDir(), "store.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, disk.TrackClose()) })
	var pairs []clientPair
	for name, store := range map[string]store_interface.Store{"ram": ram.NewRamStore(), "sqlite": disk} {
		engine := gin.New()
		api.SetupTrackRouter(store, "/track", engine)
		api.SetupDepotRouter(store, "/depot", engine)
		api.SetupGroveRouter(store, "/grove", engine)
		api.SetupLedgerRouter(store, "/ledger", engine)
		api.SetupWarehouseRouter(store, "/warehouse", engine)
		server := httptest.NewServer(engine)
		t.Cleanup(server.Close)
		space := model.TenancySpace{AppId: 12, TenancyId: 100}
		pairs = append(pairs, clientPair{name, local.New(store, space), rest.New(server.URL, space), rest.New(server.URL, model.TenancySpace{AppId: 12, TenancyId: 101})})
	}
	return pairs
}

func TestTrack(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			tag, metric := int64(math.MaxInt64), 3.5
			key := "a /?#%+雪\n"
			require.NoError(t, pair.local.TrackPut(1, key, model.TrackValue{Value: math.MaxInt64, Tag: &tag, Metric: &metric}))
			value, err := pair.rest.TrackGet(1, key, model.TrackReadOptions{})
			require.NoError(t, err)
			require.Equal(t, int64(math.MaxInt64), value.Value)
			require.NoError(t, pair.rest.TrackPutMany(map[int32][]model.TrackKeyValueItem{
				1:  {{Key: "b", Value: model.TrackValue{Value: math.MinInt64}}, {Key: "b", Value: model.TrackValue{Value: 42}}},
				-2: {{Key: "c", Value: model.TrackValue{Value: 7, Tag: &tag, Metric: &metric}}},
			}))
			for _, keys := range []map[int32][]string{nil, {}, {1: {}}, {1: {key, "b", "missing"}, -2: {"c"}, 3: {"missing"}}} {
				lv, lm, err := pair.local.TrackGetMany(keys, model.TrackReadOptions{})
				require.NoError(t, err)
				rv, rm, err := pair.rest.TrackGetMany(keys, model.TrackReadOptions{})
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lm, rm)
			}
			for _, prefix := range []string{"", "a", "missing"} {
				for _, gt := range []bool{true, false} {
					lv, err := pair.local.GetItemsByKeyPrefix(1, prefix, []int64{tag}, &metric, gt)
					require.NoError(t, err)
					rv, err := pair.rest.GetItemsByKeyPrefix(1, prefix, []int64{tag}, &metric, gt)
					require.NoError(t, err)
					require.ElementsMatch(t, lv, rv)
				}
			}
			lv, err := pair.local.GetItemsByKeyPrefixes(1, []string{"a", "b", "a"}, nil, nil, false)
			require.NoError(t, err)
			rv, err := pair.rest.GetItemsByKeyPrefixes(1, []string{"a", "b", "a"}, nil, nil, false)
			require.NoError(t, err)
			require.ElementsMatch(t, lv, rv)
			require.Len(t, rv, 2)
			require.NoError(t, pair.rest.TrackDeleteMany([]model.TrackKey{{BucketID: 1, Key: key}, {BucketID: -2, Key: "c"}}))
			_, err = pair.local.TrackGet(1, key, model.TrackReadOptions{})
			require.Error(t, err)
			mutation := model.TrackMutation{MutationID: "track-batch", Puts: []model.TrackPut{{BucketID: 1, Key: "new", Value: model.TrackValue{Value: math.MaxInt64}}, {BucketID: 2, Key: "other", Value: model.TrackValue{Value: 8}}}, Deletes: []model.TrackKey{{BucketID: 1, Key: "b"}}}
			result, err := pair.rest.TrackMutate(mutation)
			require.NoError(t, err)
			require.True(t, result.Applied)
			result, err = pair.local.TrackMutate(mutation)
			require.NoError(t, err)
			require.False(t, result.Applied)
			result, err = pair.rest.TrackMutate(mutation)
			require.NoError(t, err)
			require.False(t, result.Applied)
			value, err = pair.local.TrackGet(1, "new", model.TrackReadOptions{})
			require.NoError(t, err)
			require.Equal(t, int64(math.MaxInt64), value.Value)
			_, err = pair.other.TrackGet(1, "new", model.TrackReadOptions{})
			require.Error(t, err)
			// Mutation IDs are store-wide, even though the operation is tenant-scoped.
			result, err = pair.other.TrackMutate(mutation)
			require.NoError(t, err)
			require.False(t, result.Applied)
			for _, c := range []client.Client{pair.local, pair.rest} {
				require.NoError(t, c.TrackPutMany(nil))
				require.NoError(t, c.TrackDeleteMany(nil))
				_, err = c.TrackMutate(model.TrackMutation{})
				require.NoError(t, err)
			}
		})
	}
}

func TestDepot(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			id, err := pair.rest.DepotCreate(3, "original")
			require.NoError(t, err)
			value, err := pair.local.DepotGet(id)
			require.NoError(t, err)
			require.Equal(t, "original", value)
			require.NoError(t, pair.rest.DepotUpdate(id, "updated"))
			ids, err := pair.local.DepotCreateMany(3, []string{"one", "two"})
			require.NoError(t, err)
			require.Len(t, ids, 2)
			for _, query := range [][]int64{nil, {}, {id, ids[0], ids[1], -1}} {
				lv, lm, err := pair.local.DepotGetMany(query)
				require.NoError(t, err)
				rv, rm, err := pair.rest.DepotGetMany(query)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lm, rm)
			}
			lv, err := pair.local.DepotGetAllByBucket(3)
			require.NoError(t, err)
			rv, err := pair.rest.DepotGetAllByBucket(3)
			require.NoError(t, err)
			require.Equal(t, lv, rv)
			require.Equal(t, "updated", rv[id])
			_, err = pair.other.DepotGet(id)
			require.Error(t, err)
			require.NoError(t, pair.rest.DepotDelete(id))
			_, err = pair.local.DepotGet(id)
			require.Error(t, err)
			require.NoError(t, pair.rest.DepotDeleteByBucket(3))
			values, err := pair.local.DepotGetAllByBucket(3)
			require.NoError(t, err)
			require.Empty(t, values)
			for _, c := range []client.Client{pair.local, pair.rest} {
				ids, err := c.DepotCreateMany(3, nil)
				require.NoError(t, err)
				require.Empty(t, ids)
			}
		})
	}
}

func TestLedger(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			original, err := pair.local.LedgerAppend("orders", "one", "payload")
			require.NoError(t, err)
			retry, err := pair.rest.LedgerAppend("orders", "one", "payload")
			require.NoError(t, err)
			require.Equal(t, original, retry)
			batch, err := pair.rest.LedgerAppendMany("payments", []model.LedgerAppendItem{{AppendID: "two", Payload: "2"}, {AppendID: "three", Payload: "3"}})
			require.NoError(t, err)
			require.Len(t, batch, 2)
			for _, c := range []client.Client{pair.local, pair.rest} {
				page, err := c.LedgerReadBackward(model.LedgerSelector{All: true}, nil, 2)
				require.NoError(t, err)
				require.Len(t, page.Records, 2)
				require.NotNil(t, page.NextCursor)
				next, err := c.LedgerReadBackward(model.LedgerSelector{All: true}, page.NextCursor, 2)
				require.NoError(t, err)
				require.Equal(t, []model.LedgerRecord{original}, next.Records)
				through := batch[1].Position
				forward, err := c.LedgerReadForward(model.LedgerSelector{LedgerIDs: []model.LedgerID{"orders", "payments"}}, original.Position, &through, 10)
				require.NoError(t, err)
				require.Equal(t, batch, forward)
				_, err = c.LedgerAppend("orders", "one", "changed")
				require.ErrorIs(t, err, model.ErrLedgerAppendConflict)
				_, err = c.LedgerAppend("bad/id", "one", "")
				require.ErrorIs(t, err, model.ErrLedgerInvalidID)
			}
			empty, err := pair.other.LedgerReadBackward(model.LedgerSelector{All: true}, nil, 10)
			require.NoError(t, err)
			require.Empty(t, empty.Records)
		})
	}
}

func TestGrove(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			tree, root := model.TreeID("tree /?#%+雪"), model.NodeID("root /?#%+雪")
			metadata := model.NodeMetadata{"name": "root", "number": float64(42)}
			require.NoError(t, pair.rest.CreateNode(tree, root, nil, nil, &metadata))
			for i, id := range []model.NodeID{"a", "b", "c"} {
				pos := model.ChildPosition(i)
				require.NoError(t, pair.local.CreateNode(tree, id, &root, &pos, nil))
			}
			a := model.NodeID("a")
			require.NoError(t, pair.rest.CreateNode(tree, "grandchild", &a, nil, nil))
			li, err := pair.local.GetNodeInfo(tree, root)
			require.NoError(t, err)
			ri, err := pair.rest.GetNodeInfo(tree, root)
			require.NoError(t, err)
			require.Equal(t, li, ri)
			for _, c := range []client.Client{pair.local, pair.rest} {
				exists, err := c.Exists(tree, root)
				require.NoError(t, err)
				require.True(t, exists)
				require.ErrorIs(t, c.CreateNode(tree, root, nil, nil, nil), model.ErrNodeAlreadyExists)
				_, err = c.GetNodeInfo(tree, "missing")
				require.ErrorIs(t, err, model.ErrNodeNotFound)
			}
			exists, err := pair.other.Exists(tree, root)
			require.NoError(t, err)
			require.False(t, exists)
			for _, p := range []*model.PaginationParams{nil, {Limit: 1}, {Limit: 2}} {
				lv, lp, err := pair.local.GetChildren(tree, root, p)
				require.NoError(t, err)
				rv, rp, err := pair.rest.GetChildren(tree, root, p)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lp, rp)
				if lp != nil && lp.NextCursor != nil {
					q := &model.PaginationParams{Limit: 1, Cursor: lp.NextCursor}
					lv, lp, err = pair.local.GetChildren(tree, root, q)
					require.NoError(t, err)
					rv, rp, err = pair.rest.GetChildren(tree, root, q)
					require.NoError(t, err)
					require.Equal(t, lv, rv)
					require.Equal(t, lp, rp)
				}
				lv, lp, err = pair.local.GetAncestors(tree, "grandchild", p)
				require.NoError(t, err)
				rv, rp, err = pair.rest.GetAncestors(tree, "grandchild", p)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lp, rp)
			}
			maxDepth := 1
			for _, opts := range []*model.DescendantOptions{nil, {}, {MaxDepth: &maxDepth, IncludeDepth: true}, {BreadthFirst: true, IncludeDepth: true, Pagination: &model.PaginationParams{Limit: 2}}} {
				lv, lp, err := pair.local.GetDescendants(tree, root, opts)
				require.NoError(t, err)
				rv, rp, err := pair.rest.GetDescendants(tree, root, opts)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lp, rp)
				if lp != nil && lp.NextCursor != nil {
					next := *opts
					page := *opts.Pagination
					page.Cursor = lp.NextCursor
					next.Pagination = &page
					lv, lp, err = pair.local.GetDescendants(tree, root, &next)
					require.NoError(t, err)
					rv, rp, err = pair.rest.GetDescendants(tree, root, &next)
					require.NoError(t, err)
					require.Equal(t, lv, rv)
					require.Equal(t, lp, rp)
				}
			}
			require.NoError(t, pair.rest.ApplyAggregateMutation(tree, "aggregate", a, model.AggregateDeltas{"count": 9}))
			for _, id := range []model.NodeID{root, a} {
				lv, err := pair.local.GetNodeLocalAggregates(tree, id)
				require.NoError(t, err)
				rv, err := pair.rest.GetNodeLocalAggregates(tree, id)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				lv, err = pair.local.GetNodeWithDescendantsAggregates(tree, id)
				require.NoError(t, err)
				rv, err = pair.rest.GetNodeWithDescendantsAggregates(tree, id)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
			}
			for _, ids := range [][]model.NodeID{nil, {root, a, "missing"}} {
				lv, lm, err := pair.local.GetAncestorsBulk(tree, ids)
				require.NoError(t, err)
				rv, rm, err := pair.rest.GetAncestorsBulk(tree, ids)
				require.NoError(t, err)
				require.Equal(t, lv, rv)
				require.Equal(t, lm, rm)
				for _, subtree := range []bool{false, true} {
					lf, rf := pair.local.GetNodeLocalAggregatesBulk, pair.rest.GetNodeLocalAggregatesBulk
					if subtree {
						lf, rf = pair.local.GetNodeWithDescendantsAggregatesBulk, pair.rest.GetNodeWithDescendantsAggregatesBulk
					}
					lv, lm, err := lf(tree, ids)
					require.NoError(t, err)
					rv, rm, err := rf(tree, ids)
					require.NoError(t, err)
					require.Equal(t, lv, rv)
					require.Equal(t, lm, rm)
				}
			}
			require.NoError(t, pair.rest.MoveNode(tree, "grandchild", &root, nil))
			moved, err := pair.local.GetNodeInfo(tree, "grandchild")
			require.NoError(t, err)
			require.Equal(t, &root, moved.Parent)
			require.NoError(t, pair.rest.DeleteNode(tree, "grandchild", true))
			exists, err = pair.local.Exists(tree, "grandchild")
			require.NoError(t, err)
			require.False(t, exists)
			require.NoError(t, pair.rest.DeleteNode(tree, "c", false))
			exists, err = pair.local.Exists(tree, "c")
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestWarehouseTenancy(t *testing.T) {
	for _, pair := range buildClientPairs(t) {
		t.Run(pair.name, func(t *testing.T) {
			blob, err := pair.rest.WarehousePut(context.Background(), model.PutBlobRequest{PutID: "put", Value: []byte("value")})
			require.NoError(t, err)
			_, err = pair.other.WarehouseGet(context.Background(), blob.ID)
			require.ErrorIs(t, err, model.ErrBlobNotFound)
		})
	}
}
