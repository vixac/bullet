package sqlite_store

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/vixac/bullet/store/store_interface"
)

func newLedgerTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("create SQLite store: %v", err)
	}
	store.db.SetMaxOpenConns(1)
	t.Cleanup(func() { store.db.Close() })
	return store
}

func TestLedgerAppendIsOrderedAcrossLedgers(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 1, TenancyId: 10}

	first, err := store.LedgerAppend(space, "orders", "order-1", `{"order":1}`)
	if err != nil {
		t.Fatalf("append first record: %v", err)
	}
	second, err := store.LedgerAppend(space, "payments", "payment-1", `{"payment":1}`)
	if err != nil {
		t.Fatalf("append second record: %v", err)
	}
	third, err := store.LedgerAppend(space, "orders", "order-2", `{"order":2}`)
	if err != nil {
		t.Fatalf("append third record: %v", err)
	}

	if first.Position != 1 || second.Position != 2 || third.Position != 3 {
		t.Fatalf("positions = %d, %d, %d; want 1, 2, 3", first.Position, second.Position, third.Position)
	}
	page, err := store.LedgerReadBackward(space, store_interface.LedgerSelector{All: true}, nil, 10)
	if err != nil {
		t.Fatalf("read all ledgers: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("got %d records, want 3", len(page.Records))
	}
	if page.Records[0].LedgerID != "orders" || page.Records[0].Position != 3 || page.Records[1].LedgerID != "payments" || page.Records[2].Position != 1 {
		t.Errorf("unexpected merged order: %+v", page.Records)
	}
	if page.NextCursor != nil {
		t.Error("unexpected cursor for exhausted page")
	}
}

func TestLedgerAppendIdempotencyAndConflict(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 2, TenancyId: 20}

	original, err := store.LedgerAppend(space, "audit", "request-1", "same bytes")
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	retried, err := store.LedgerAppend(space, "audit", "request-1", "same bytes")
	if err != nil {
		t.Fatalf("retry append: %v", err)
	}
	if retried.Position != original.Position || !retried.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("retry returned a different record: original=%+v retried=%+v", original, retried)
	}
	_, err = store.LedgerAppend(space, "audit", "request-1", "different bytes")
	if !errors.Is(err, store_interface.ErrLedgerAppendConflict) {
		t.Fatalf("different retry error = %v, want append conflict", err)
	}
}

func TestLedgerAppendManyIsAtomicOrderedAndIdempotent(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 3, TenancyId: 30}
	items := []store_interface.LedgerAppendItem{
		{AppendID: "a", Payload: "one"},
		{AppendID: "b", Payload: "two"},
		{AppendID: "c", Payload: "three"},
	}
	records, err := store.LedgerAppendMany(space, "events", items)
	if err != nil {
		t.Fatalf("append many: %v", err)
	}
	for i, record := range records {
		if record.Position != store_interface.LedgerPosition(i+1) {
			t.Errorf("record %d position = %d, want %d", i, record.Position, i+1)
		}
	}
	retried, err := store.LedgerAppendMany(space, "events", items)
	if err != nil {
		t.Fatalf("retry batch: %v", err)
	}
	for i := range retried {
		if retried[i].Position != records[i].Position {
			t.Errorf("retry position %d = %d, want %d", i, retried[i].Position, records[i].Position)
		}
	}
	_, err = store.LedgerAppendMany(space, "events", []store_interface.LedgerAppendItem{{AppendID: "a", Payload: "one"}, {AppendID: "new", Payload: "new"}})
	if !errors.Is(err, store_interface.ErrLedgerBatchConflict) {
		t.Fatalf("mixed batch error = %v, want batch conflict", err)
	}
	page, err := store.LedgerReadBackward(space, store_interface.LedgerSelector{All: true}, nil, 10)
	if err != nil {
		t.Fatalf("read after conflict: %v", err)
	}
	if len(page.Records) != 3 {
		t.Fatalf("conflicted batch wrote records: got %d records", len(page.Records))
	}
}

func TestLedgerBackwardPaginationIsStatelessAndAnchored(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 4, TenancyId: 40}
	for i := 1; i <= 5; i++ {
		if _, err := store.LedgerAppend(space, "feed", store_interface.LedgerAppendID(fmt.Sprintf("id-%d", i)), fmt.Sprintf("payload-%d", i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	selector := store_interface.LedgerSelector{LedgerIDs: []store_interface.LedgerID{"feed"}}
	first, err := store.LedgerReadBackward(space, selector, nil, 2)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	assertPositions(t, first.Records, 5, 4)
	if first.NextCursor == nil {
		t.Fatal("first page has no next cursor")
	}
	if _, err := store.LedgerAppend(space, "feed", "id-6", "payload-6"); err != nil {
		t.Fatalf("append after snapshot: %v", err)
	}
	second, err := store.LedgerReadBackward(space, selector, first.NextCursor, 2)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	assertPositions(t, second.Records, 3, 2)
	third, err := store.LedgerReadBackward(space, selector, second.NextCursor, 2)
	if err != nil {
		t.Fatalf("third page: %v", err)
	}
	assertPositions(t, third.Records, 1)
	if third.NextCursor != nil {
		t.Error("final page has a next cursor")
	}
}

func TestLedgerCursorIsBoundToSelector(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 5, TenancyId: 50}
	for i := 0; i < 2; i++ {
		_, _ = store.LedgerAppend(space, "one", store_interface.LedgerAppendID(fmt.Sprintf("id-%d", i)), "x")
	}
	page, err := store.LedgerReadBackward(space, store_interface.LedgerSelector{LedgerIDs: []store_interface.LedgerID{"one"}}, nil, 1)
	if err != nil || page.NextCursor == nil {
		t.Fatalf("get cursor: page=%+v err=%v", page, err)
	}
	_, err = store.LedgerReadBackward(space, store_interface.LedgerSelector{All: true}, page.NextCursor, 1)
	if !errors.Is(err, store_interface.ErrLedgerInvalidCursor) {
		t.Fatalf("selector mismatch error = %v, want invalid cursor", err)
	}
}

func TestLedgerReadForwardAndTenantIsolation(t *testing.T) {
	store := newLedgerTestStore(t)
	spaceA := store_interface.TenancySpace{AppId: 6, TenancyId: 60}
	spaceB := store_interface.TenancySpace{AppId: 6, TenancyId: 61}
	for i := 1; i <= 4; i++ {
		_, _ = store.LedgerAppend(spaceA, "events", store_interface.LedgerAppendID(fmt.Sprintf("a-%d", i)), fmt.Sprintf("a%d", i))
	}
	_, _ = store.LedgerAppend(spaceB, "events", "b-1", "private")
	through := store_interface.LedgerPosition(3)
	records, err := store.LedgerReadForward(spaceA, store_interface.LedgerSelector{All: true}, 1, &through, 10)
	if err != nil {
		t.Fatalf("read forward: %v", err)
	}
	assertPositions(t, records, 2, 3)
	other, err := store.LedgerReadForward(spaceB, store_interface.LedgerSelector{All: true}, 0, nil, 10)
	if err != nil {
		t.Fatalf("read other tenant: %v", err)
	}
	if len(other) != 1 || other[0].Payload != "private" || other[0].Position != 1 {
		t.Fatalf("tenant B records = %+v", other)
	}
}

func TestLedgerDeleteIsScopedAndIdempotent(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 7, TenancyId: 70}
	_, _ = store.LedgerAppend(space, "remove", "r-1", "remove")
	_, _ = store.LedgerAppend(space, "keep", "k-1", "keep")
	if err := store.LedgerDelete(space, "remove"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.LedgerDelete(space, "remove"); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
	page, err := store.LedgerReadBackward(space, store_interface.LedgerSelector{All: true}, nil, 10)
	if err != nil {
		t.Fatalf("read after delete: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].LedgerID != "keep" {
		t.Fatalf("records after delete = %+v", page.Records)
	}
	created, err := store.LedgerAppend(space, "remove", "r-1", "recreated")
	if err != nil {
		t.Fatalf("recreate deleted ledger: %v", err)
	}
	if created.Position != 3 {
		t.Errorf("recreated position = %d, want 3", created.Position)
	}
}

func TestLedgerValidation(t *testing.T) {
	store := newLedgerTestStore(t)
	space := store_interface.TenancySpace{AppId: 8, TenancyId: 80}
	_, err := store.LedgerAppend(space, "bad ledger", "id", "payload")
	if !errors.Is(err, store_interface.ErrLedgerInvalidID) {
		t.Errorf("invalid ledger error = %v", err)
	}
	_, err = store.LedgerAppend(space, "valid", "", "payload")
	if !errors.Is(err, store_interface.ErrLedgerInvalidAppendID) {
		t.Errorf("empty append id error = %v", err)
	}
	_, err = store.LedgerAppend(space, "valid", "large", strings.Repeat("x", store_interface.LedgerMaxPayloadBytes+1))
	if !errors.Is(err, store_interface.ErrLedgerPayloadTooLarge) {
		t.Errorf("large payload error = %v", err)
	}
	_, err = store.LedgerReadBackward(space, store_interface.LedgerSelector{}, nil, 10)
	if !errors.Is(err, store_interface.ErrLedgerInvalidSelector) {
		t.Errorf("empty selector error = %v", err)
	}
	_, err = store.LedgerReadBackward(space, store_interface.LedgerSelector{All: true}, nil, 0)
	if !errors.Is(err, store_interface.ErrLedgerInvalidPageSize) {
		t.Errorf("page size error = %v", err)
	}
}

func assertPositions(t *testing.T, records []store_interface.LedgerRecord, want ...store_interface.LedgerPosition) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("got %d records, want %d: %+v", len(records), len(want), records)
	}
	for i := range want {
		if records[i].Position != want[i] {
			t.Errorf("record %d position = %d, want %d", i, records[i].Position, want[i])
		}
	}
}
