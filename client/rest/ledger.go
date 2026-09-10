package rest

import (
	"fmt"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	"net/http"
	"net/url"
	"strconv"
)

func ledgerPath(id model.LedgerID) string { return "/ledger/" + url.PathEscape(string(id)) }
func ledgerRecord(w protocol.LedgerRecordResponse) (model.LedgerRecord, error) {
	p, err := strconv.ParseInt(w.Position, 10, 64)
	if err != nil {
		return model.LedgerRecord{}, fmt.Errorf("invalid ledger position %q: %w", w.Position, err)
	}
	return model.LedgerRecord{LedgerID: model.LedgerID(w.LedgerID), AppendID: model.LedgerAppendID(w.AppendID), Position: model.LedgerPosition(p), CreatedAt: w.CreatedAt, Payload: w.Payload}, nil
}
func ledgerRecords(ws []protocol.LedgerRecordResponse) ([]model.LedgerRecord, error) {
	if ws == nil {
		return nil, nil
	}
	r := make([]model.LedgerRecord, len(ws))
	for i, w := range ws {
		var err error
		r[i], err = ledgerRecord(w)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
func ledgerSelector(s model.LedgerSelector) protocol.LedgerSelectorRequest {
	var ids []string
	if s.LedgerIDs != nil {
		ids = make([]string, len(s.LedgerIDs))
		for i, id := range s.LedgerIDs {
			ids[i] = string(id)
		}
	}
	return protocol.LedgerSelectorRequest{All: s.All, LedgerIDs: ids, Prefix: s.Prefix}
}
func (c *Client) LedgerAppend(id model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error) {
	var r protocol.LedgerRecordResponse
	err := c.call(http.MethodPost, ledgerPath(id)+"/entries", protocol.LedgerAppendRequest{AppendID: string(appendID), Payload: payload}, &r, http.StatusCreated)
	if err != nil {
		return model.LedgerRecord{}, err
	}
	return ledgerRecord(r)
}
func (c *Client) LedgerAppendMany(id model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error) {
	req := protocol.LedgerAppendManyRequest{Items: make([]protocol.LedgerAppendRequest, len(items))}
	for i, v := range items {
		req.Items[i] = protocol.LedgerAppendRequest{AppendID: string(v.AppendID), Payload: v.Payload}
	}
	var r protocol.LedgerAppendManyResponse
	if err := c.call(http.MethodPost, ledgerPath(id)+"/entries/batch", req, &r, http.StatusCreated); err != nil {
		return nil, err
	}
	return ledgerRecords(r.Records)
}
func (c *Client) LedgerReadBackward(selector model.LedgerSelector, cursor *string, limit int) (model.LedgerPage, error) {
	var r protocol.LedgerPageResponse
	if err := c.call(http.MethodPost, "/ledger/read/backward", protocol.LedgerReadBackwardRequest{LedgerSelectorRequest: ledgerSelector(selector), Cursor: cursor, Limit: limit}, &r, http.StatusOK); err != nil {
		return model.LedgerPage{}, err
	}
	records, err := ledgerRecords(r.Records)
	if err != nil {
		return model.LedgerPage{}, err
	}
	return model.LedgerPage{Records: records, NextCursor: r.NextCursor}, nil
}
func (c *Client) LedgerReadForward(selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error) {
	req := protocol.LedgerReadForwardRequest{LedgerSelectorRequest: ledgerSelector(selector), AfterPosition: strconv.FormatInt(int64(after), 10), Limit: limit}
	if through != nil {
		s := strconv.FormatInt(int64(*through), 10)
		req.ThroughPosition = &s
	}
	var r protocol.LedgerReadForwardResponse
	if err := c.call(http.MethodPost, "/ledger/read/forward", req, &r, http.StatusOK); err != nil {
		return nil, err
	}
	return ledgerRecords(r.Records)
}
func (c *Client) LedgerDelete(id model.LedgerID) error {
	return c.call(http.MethodDelete, ledgerPath(id), nil, nil, http.StatusNoContent)
}
