package ram

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"sort"
	"time"

	"github.com/vixac/bullet/store/store_interface"
)

var ramLedgerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type ledgerSpaceData struct {
	nextPosition store_interface.LedgerPosition
	records      map[store_interface.LedgerPosition]store_interface.LedgerRecord
	appendIDs    map[store_interface.LedgerID]map[store_interface.LedgerAppendID]store_interface.LedgerPosition
}

type ramLedgerCursor struct {
	Version  int    `json:"v"`
	Upper    int64  `json:"upper"`
	Before   int64  `json:"before"`
	Selector string `json:"selector"`
}

func validateRamLedgerWrite(ledgerID store_interface.LedgerID, appendID store_interface.LedgerAppendID, payload string) error {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return store_interface.ErrLedgerInvalidID
	}
	if len(appendID) == 0 || len(appendID) > 255 {
		return store_interface.ErrLedgerInvalidAppendID
	}
	if len(payload) > store_interface.LedgerMaxPayloadBytes {
		return store_interface.ErrLedgerPayloadTooLarge
	}
	return nil
}

func normalizeRamLedgerSelector(selector store_interface.LedgerSelector) (map[store_interface.LedgerID]struct{}, string, error) {
	if selector.All == (len(selector.LedgerIDs) > 0) || len(selector.LedgerIDs) > store_interface.LedgerMaxSelected {
		return nil, "", store_interface.ErrLedgerInvalidSelector
	}
	if selector.All {
		return nil, "all", nil
	}
	ids := append([]store_interface.LedgerID(nil), selector.LedgerIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	selected := make(map[store_interface.LedgerID]struct{}, len(ids))
	h := sha256.New()
	for _, id := range ids {
		if !ramLedgerIDPattern.MatchString(string(id)) {
			return nil, "", store_interface.ErrLedgerInvalidID
		}
		if _, duplicate := selected[id]; duplicate {
			return nil, "", store_interface.ErrLedgerInvalidSelector
		}
		selected[id] = struct{}{}
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return selected, base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

func ramLedgerSelected(record store_interface.LedgerRecord, all bool, selected map[store_interface.LedgerID]struct{}) bool {
	if all {
		return true
	}
	_, ok := selected[record.LedgerID]
	return ok
}

func encodeRamLedgerCursor(cursor ramLedgerCursor) string {
	b, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeRamLedgerCursor(value string) (ramLedgerCursor, error) {
	var cursor ramLedgerCursor
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || json.Unmarshal(b, &cursor) != nil || cursor.Version != 1 || cursor.Upper < 0 || cursor.Before < 0 {
		return cursor, store_interface.ErrLedgerInvalidCursor
	}
	return cursor, nil
}

func (s *RamStore) ensureLedgerSpace(space store_interface.TenancySpace) *ledgerSpaceData {
	data := s.ledgers[space]
	if data == nil {
		data = &ledgerSpaceData{records: make(map[store_interface.LedgerPosition]store_interface.LedgerRecord), appendIDs: make(map[store_interface.LedgerID]map[store_interface.LedgerAppendID]store_interface.LedgerPosition)}
		s.ledgers[space] = data
	}
	return data
}

func (s *RamStore) LedgerAppend(space store_interface.TenancySpace, ledgerID store_interface.LedgerID, appendID store_interface.LedgerAppendID, payload string) (store_interface.LedgerRecord, error) {
	records, err := s.LedgerAppendMany(space, ledgerID, []store_interface.LedgerAppendItem{{AppendID: appendID, Payload: payload}})
	if err != nil {
		return store_interface.LedgerRecord{}, err
	}
	return records[0], nil
}

func (s *RamStore) LedgerAppendMany(space store_interface.TenancySpace, ledgerID store_interface.LedgerID, items []store_interface.LedgerAppendItem) ([]store_interface.LedgerRecord, error) {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return nil, store_interface.ErrLedgerInvalidID
	}
	if len(items) == 0 {
		return []store_interface.LedgerRecord{}, nil
	}
	seen := make(map[store_interface.LedgerAppendID]struct{}, len(items))
	for _, item := range items {
		if err := validateRamLedgerWrite(ledgerID, item.AppendID, item.Payload); err != nil {
			return nil, err
		}
		if _, duplicate := seen[item.AppendID]; duplicate {
			return nil, store_interface.ErrLedgerInvalidAppendID
		}
		seen[item.AppendID] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.ensureLedgerSpace(space)
	ledgerAppendIDs := data.appendIDs[ledgerID]
	if ledgerAppendIDs == nil {
		ledgerAppendIDs = make(map[store_interface.LedgerAppendID]store_interface.LedgerPosition)
		data.appendIDs[ledgerID] = ledgerAppendIDs
	}
	existing := make([]store_interface.LedgerRecord, len(items))
	existingCount := 0
	for i, item := range items {
		position, ok := ledgerAppendIDs[item.AppendID]
		if !ok {
			continue
		}
		existingCount++
		existing[i] = data.records[position]
		if existing[i].Payload != item.Payload {
			return nil, store_interface.ErrLedgerAppendConflict
		}
	}
	if existingCount == len(items) {
		return existing, nil
	}
	if existingCount != 0 {
		return nil, store_interface.ErrLedgerBatchConflict
	}
	createdAt := time.Now().UTC()
	records := make([]store_interface.LedgerRecord, len(items))
	for i, item := range items {
		data.nextPosition++
		record := store_interface.LedgerRecord{LedgerID: ledgerID, Position: data.nextPosition, AppendID: item.AppendID, CreatedAt: createdAt, Payload: item.Payload}
		data.records[record.Position] = record
		ledgerAppendIDs[item.AppendID] = record.Position
		records[i] = record
	}
	return records, nil
}

func (s *RamStore) LedgerReadBackward(space store_interface.TenancySpace, selector store_interface.LedgerSelector, cursorValue *string, limit int) (store_interface.LedgerPage, error) {
	if limit < 1 || limit > store_interface.LedgerMaxPageSize {
		return store_interface.LedgerPage{}, store_interface.ErrLedgerInvalidPageSize
	}
	selected, selectorKey, err := normalizeRamLedgerSelector(selector)
	if err != nil {
		return store_interface.LedgerPage{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := s.ledgers[space]
	if data == nil {
		return store_interface.LedgerPage{Records: []store_interface.LedgerRecord{}}, nil
	}
	upper, before := data.nextPosition, store_interface.LedgerPosition(0)
	if cursorValue != nil {
		cursor, err := decodeRamLedgerCursor(*cursorValue)
		if err != nil || cursor.Selector != selectorKey {
			return store_interface.LedgerPage{}, store_interface.ErrLedgerInvalidCursor
		}
		upper, before = store_interface.LedgerPosition(cursor.Upper), store_interface.LedgerPosition(cursor.Before)
	}
	records := make([]store_interface.LedgerRecord, 0, limit+1)
	for position := upper; position > 0 && len(records) <= limit; position-- {
		if before > 0 && position >= before {
			continue
		}
		record, ok := data.records[position]
		if ok && ramLedgerSelected(record, selector.All, selected) {
			records = append(records, record)
		}
	}
	page := store_interface.LedgerPage{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		next := encodeRamLedgerCursor(ramLedgerCursor{Version: 1, Upper: int64(upper), Before: int64(page.Records[len(page.Records)-1].Position), Selector: selectorKey})
		page.NextCursor = &next
	}
	return page, nil
}

func (s *RamStore) LedgerReadForward(space store_interface.TenancySpace, selector store_interface.LedgerSelector, after store_interface.LedgerPosition, through *store_interface.LedgerPosition, limit int) ([]store_interface.LedgerRecord, error) {
	if limit < 1 || limit > store_interface.LedgerMaxPageSize {
		return nil, store_interface.ErrLedgerInvalidPageSize
	}
	if after < 0 || (through != nil && *through < 0) {
		return nil, store_interface.ErrLedgerInvalidCursor
	}
	selected, _, err := normalizeRamLedgerSelector(selector)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := s.ledgers[space]
	if data == nil {
		return []store_interface.LedgerRecord{}, nil
	}
	upper := data.nextPosition
	if through != nil && *through < upper {
		upper = *through
	}
	records := make([]store_interface.LedgerRecord, 0, limit)
	for position := after + 1; position <= upper && len(records) < limit; position++ {
		record, ok := data.records[position]
		if ok && ramLedgerSelected(record, selector.All, selected) {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *RamStore) LedgerDelete(space store_interface.TenancySpace, ledgerID store_interface.LedgerID) error {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return store_interface.ErrLedgerInvalidID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.ledgers[space]
	if data == nil {
		return nil
	}
	for position, record := range data.records {
		if record.LedgerID == ledgerID {
			delete(data.records, position)
		}
	}
	delete(data.appendIDs, ledgerID)
	return nil
}
