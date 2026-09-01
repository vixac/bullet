package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/ram"
)

func ledgerRequest(t *testing.T, engine *gin.Engine, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-App-Id", "1")
	req.Header.Set("X-Tenancy-Id", "2")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, req)
	return response
}

func TestLedgerRESTLifecycle(t *testing.T) {
	engine := gin.New()
	SetupLedgerRouter(ram.NewRamStore(), "/ledger", engine)

	first := ledgerRequest(t, engine, http.MethodPost, "/ledger/orders/entries", model.LedgerAppendRequest{AppendID: "order-1", Payload: "first"})
	if first.Code != http.StatusCreated {
		t.Fatalf("append status = %d body=%s", first.Code, first.Body.String())
	}
	var firstRecord model.LedgerRecordResponse
	if err := json.Unmarshal(first.Body.Bytes(), &firstRecord); err != nil {
		t.Fatalf("decode append: %v", err)
	}
	if firstRecord.Position != "1" || firstRecord.LedgerID != "orders" {
		t.Fatalf("append response = %+v", firstRecord)
	}

	batch := ledgerRequest(t, engine, http.MethodPost, "/ledger/payments/entries/batch", model.LedgerAppendManyRequest{Items: []model.LedgerAppendRequest{{AppendID: "payment-1", Payload: "second"}, {AppendID: "payment-2", Payload: "third"}}})
	if batch.Code != http.StatusCreated {
		t.Fatalf("batch status = %d body=%s", batch.Code, batch.Body.String())
	}

	backward := ledgerRequest(t, engine, http.MethodPost, "/ledger/read/backward", model.LedgerReadBackwardRequest{LedgerSelectorRequest: model.LedgerSelectorRequest{All: true}, Limit: 2})
	if backward.Code != http.StatusOK {
		t.Fatalf("backward status = %d body=%s", backward.Code, backward.Body.String())
	}
	var page model.LedgerPageResponse
	if err := json.Unmarshal(backward.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if len(page.Records) != 2 || page.Records[0].Position != "3" || page.NextCursor == nil {
		t.Fatalf("backward page = %+v", page)
	}

	through := "3"
	forward := ledgerRequest(t, engine, http.MethodPost, "/ledger/read/forward", model.LedgerReadForwardRequest{LedgerSelectorRequest: model.LedgerSelectorRequest{LedgerIDs: []string{"orders", "payments"}}, AfterPosition: "1", ThroughPosition: &through, Limit: 10})
	if forward.Code != http.StatusOK {
		t.Fatalf("forward status = %d body=%s", forward.Code, forward.Body.String())
	}
	var forwardBody model.LedgerReadForwardResponse
	_ = json.Unmarshal(forward.Body.Bytes(), &forwardBody)
	if len(forwardBody.Records) != 2 || forwardBody.Records[0].Position != "2" {
		t.Fatalf("forward records = %+v", forwardBody.Records)
	}

	conflict := ledgerRequest(t, engine, http.MethodPost, "/ledger/orders/entries", model.LedgerAppendRequest{AppendID: "order-1", Payload: "changed"})
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body=%s", conflict.Code, conflict.Body.String())
	}
	deleted := ledgerRequest(t, engine, http.MethodDelete, "/ledger/payments", nil)
	if deleted.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", deleted.Code)
	}
}

func TestLedgerRESTValidation(t *testing.T) {
	engine := gin.New()
	SetupLedgerRouter(ram.NewRamStore(), "/ledger", engine)
	invalid := ledgerRequest(t, engine, http.MethodPost, "/ledger/bad%20ledger/entries", model.LedgerAppendRequest{AppendID: "id", Payload: "x"})
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid ledger status = %d body=%s", invalid.Code, invalid.Body.String())
	}
	invalidPosition := ledgerRequest(t, engine, http.MethodPost, "/ledger/read/forward", model.LedgerReadForwardRequest{LedgerSelectorRequest: model.LedgerSelectorRequest{All: true}, AfterPosition: "not-a-number", Limit: 10})
	if invalidPosition.Code != http.StatusBadRequest {
		t.Fatalf("invalid position status = %d", invalidPosition.Code)
	}
}
