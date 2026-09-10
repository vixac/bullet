package store_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

func TestLedgerContract(t *testing.T) {
	for name, store := range ledgerStores {
		t.Run(name, func(t *testing.T) {
			testLedgerContract(t, store)
		})
	}
}

func testLedgerContract(t *testing.T, store store_interface.LedgerStore) {
	space := model.TenancySpace{AppId: 900, TenancyId: 1}
	for _, ledgerID := range []model.LedgerID{"orders", "payments"} {
		if err := store.LedgerDelete(space, ledgerID); err != nil {
			t.Fatalf("clear ledger %s: %v", ledgerID, err)
		}
	}

	first, err := store.LedgerAppend(space, "orders", "order-1", "first")
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	batch, err := store.LedgerAppendMany(space, "payments", []model.LedgerAppendItem{{AppendID: "payment-1", Payload: "second"}, {AppendID: "payment-2", Payload: "third"}})
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if batch[0].Position != first.Position+1 || batch[1].Position != batch[0].Position+1 {
		t.Fatalf("positions are not tenant ordered: first=%d batch=%v", first.Position, batch)
	}
	retry, err := store.LedgerAppend(space, "orders", "order-1", "first")
	if err != nil || retry.Position != first.Position {
		t.Fatalf("idempotent retry = %+v, %v", retry, err)
	}
	_, err = store.LedgerAppend(space, "orders", "order-1", "changed")
	if !errors.Is(err, model.ErrLedgerAppendConflict) {
		t.Fatalf("changed retry error = %v", err)
	}

	selector := model.LedgerSelector{All: true}
	page1, err := store.LedgerReadBackward(space, selector, nil, 2)
	if err != nil {
		t.Fatalf("read first page: %v", err)
	}
	if len(page1.Records) != 2 || page1.Records[0].Payload != "third" || page1.Records[1].Payload != "second" || page1.NextCursor == nil {
		t.Fatalf("first page = %+v", page1)
	}
	newRecord, err := store.LedgerAppend(space, "orders", "order-2", "new-after-page")
	if err != nil {
		t.Fatalf("append after first page: %v", err)
	}
	page2, err := store.LedgerReadBackward(space, selector, page1.NextCursor, 2)
	if err != nil {
		t.Fatalf("read second page: %v", err)
	}
	if len(page2.Records) != 1 || page2.Records[0].Position != first.Position || page2.NextCursor != nil {
		t.Fatalf("anchored second page = %+v", page2)
	}

	forward, err := store.LedgerReadForward(space, selector, first.Position, &newRecord.Position, 10)
	if err != nil {
		t.Fatalf("read forward: %v", err)
	}
	if len(forward) != 3 || forward[0].Payload != "second" || forward[2].Payload != "new-after-page" {
		t.Fatalf("forward records = %+v", forward)
	}
	if err := store.LedgerDelete(space, "payments"); err != nil {
		t.Fatalf("delete payments: %v", err)
	}
	remaining, err := store.LedgerReadBackward(space, selector, nil, 10)
	if err != nil {
		t.Fatalf("read remaining: %v", err)
	}
	for _, record := range remaining.Records {
		if record.LedgerID == "payments" {
			t.Fatalf("deleted record remains: %+v", record)
		}
	}
	if _, err := store.LedgerAppend(space, "bad ledger", model.LedgerAppendID(fmt.Sprintf("bad-%s", t.Name())), "x"); !errors.Is(err, model.ErrLedgerInvalidID) {
		t.Fatalf("invalid ledger error = %v", err)
	}
}

func TestLedgerPrefix(t *testing.T) {
	for name, store := range ledgerStores {
		t.Run(name, func(t *testing.T) {
			space := model.TenancySpace{AppId: 901, TenancyId: 1}
			ids := []model.LedgerID{"orders_", "orders_a", "ordersXa", "Orders_a", "orders_b", "other"}
			for _, id := range ids {
				if _, err := store.LedgerAppend(space, id, "one", string(id)); err != nil {
					t.Fatal(err)
				}
			}
			other := model.TenancySpace{AppId: 901, TenancyId: 2}
			if _, err := store.LedgerAppend(other, "orders_hidden", "one", "hidden"); err != nil {
				t.Fatal(err)
			}
			selector := model.LedgerSelector{Prefix: "orders_"}
			page, err := store.LedgerReadBackward(space, selector, nil, 2)
			if err != nil || len(page.Records) != 2 || page.Records[0].LedgerID != "orders_b" || page.Records[1].LedgerID != "orders_a" || page.NextCursor == nil {
				t.Fatalf("page = %+v, %v", page, err)
			}
			added, err := store.LedgerAppend(space, "orders_new", "one", "new")
			if err != nil {
				t.Fatal(err)
			}
			next, err := store.LedgerReadBackward(space, selector, page.NextCursor, 2)
			if err != nil || len(next.Records) != 1 || next.Records[0].LedgerID != "orders_" || next.NextCursor != nil {
				t.Fatalf("next = %+v, %v", next, err)
			}
			for _, changed := range []model.LedgerSelector{{Prefix: "orders"}, {LedgerIDs: []model.LedgerID{"orders_"}}} {
				if _, err := store.LedgerReadBackward(space, changed, page.NextCursor, 2); !errors.Is(err, model.ErrLedgerInvalidCursor) {
					t.Fatalf("cursor error = %v", err)
				}
			}
			through := added.Position - 1
			forward, err := store.LedgerReadForward(space, selector, next.Records[0].Position, &through, 10)
			if err != nil || len(forward) != 2 || forward[0].LedgerID != "orders_a" || forward[1].LedgerID != "orders_b" {
				t.Fatalf("forward = %+v, %v", forward, err)
			}
			empty, err := store.LedgerReadForward(space, model.LedgerSelector{Prefix: "missing"}, 0, nil, 10)
			if err != nil || len(empty) != 0 {
				t.Fatalf("missing = %+v, %v", empty, err)
			}
			for _, invalid := range []model.LedgerSelector{{}, {All: true, Prefix: "orders"}, {LedgerIDs: []model.LedgerID{"orders"}, Prefix: "orders"}, {Prefix: "bad%prefix"}} {
				if _, err := store.LedgerReadBackward(space, invalid, nil, 10); err == nil {
					t.Fatalf("accepted %+v", invalid)
				}
				if _, err := store.LedgerReadForward(space, invalid, 0, nil, 10); err == nil {
					t.Fatalf("accepted %+v", invalid)
				}
			}
		})
	}
}
