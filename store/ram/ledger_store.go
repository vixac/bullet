package ram

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/vixac/bullet/model"
)

var ramLedgerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type ledgerSpaceData struct {
	nextPosition model.LedgerPosition
	records      map[model.LedgerPosition]model.LedgerRecord
	appendIDs    map[model.LedgerID]map[model.LedgerAppendID]model.LedgerPosition
}

type ramLedgerCursor struct {
	Version  int    `json:"v"`
	Upper    int64  `json:"upper"`
	Before   int64  `json:"before"`
	Selector string `json:"selector"`
}

func validateRamLedgerWrite(ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) error {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return model.ErrLedgerInvalidID
	}
	if len(appendID) == 0 || len(appendID) > 255 {
		return model.ErrLedgerInvalidAppendID
	}
	if len(payload) > model.LedgerMaxPayloadBytes {
		return model.ErrLedgerPayloadTooLarge
	}
	return nil
}

func normalizeRamLedgerSelector(selector model.LedgerSelector) (map[model.LedgerID]struct{}, string, error) {
	modes := 0
	for _, enabled := range []bool{selector.All, len(selector.LedgerIDs) > 0, selector.Prefix != ""} {
		if enabled {
			modes++
		}
	}
	if modes != 1 || len(selector.LedgerIDs) > model.LedgerMaxSelected {
		return nil, "", model.ErrLedgerInvalidSelector
	}
	if selector.All {
		return nil, "all", nil
	}
	if selector.Prefix != "" {
		if !ramLedgerIDPattern.MatchString(selector.Prefix) {
			return nil, "", model.ErrLedgerInvalidID
		}
		return nil, "prefix:" + selector.Prefix, nil
	}
	ids := append([]model.LedgerID(nil), selector.LedgerIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	selected := make(map[model.LedgerID]struct{}, len(ids))
	h := sha256.New()
	for _, id := range ids {
		if !ramLedgerIDPattern.MatchString(string(id)) {
			return nil, "", model.ErrLedgerInvalidID
		}
		if _, duplicate := selected[id]; duplicate {
			return nil, "", model.ErrLedgerInvalidSelector
		}
		selected[id] = struct{}{}
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return selected, base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

func ramLedgerSelected(record model.LedgerRecord, all bool, prefix string, selected map[model.LedgerID]struct{}) bool {
	if all {
		return true
	}
	if prefix != "" {
		return strings.HasPrefix(string(record.LedgerID), prefix)
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
		return cursor, model.ErrLedgerInvalidCursor
	}
	return cursor, nil
}

func (s *RamStore) ensureLedgerSpace(space model.TenancySpace) *ledgerSpaceData {
	data := s.ledgers[space]
	if data == nil {
		data = &ledgerSpaceData{records: make(map[model.LedgerPosition]model.LedgerRecord), appendIDs: make(map[model.LedgerID]map[model.LedgerAppendID]model.LedgerPosition)}
		s.ledgers[space] = data
	}
	return data
}

func (s *RamStore) LedgerAppend(space model.TenancySpace, ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error) {
	records, err := s.LedgerAppendMany(space, ledgerID, []model.LedgerAppendItem{{AppendID: appendID, Payload: payload}})
	if err != nil {
		return model.LedgerRecord{}, err
	}
	return records[0], nil
}

func (s *RamStore) LedgerAppendMany(space model.TenancySpace, ledgerID model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error) {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return nil, model.ErrLedgerInvalidID
	}
	if len(items) == 0 {
		return []model.LedgerRecord{}, nil
	}
	seen := make(map[model.LedgerAppendID]struct{}, len(items))
	for _, item := range items {
		if err := validateRamLedgerWrite(ledgerID, item.AppendID, item.Payload); err != nil {
			return nil, err
		}
		if _, duplicate := seen[item.AppendID]; duplicate {
			return nil, model.ErrLedgerInvalidAppendID
		}
		seen[item.AppendID] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.ensureLedgerSpace(space)
	ledgerAppendIDs := data.appendIDs[ledgerID]
	if ledgerAppendIDs == nil {
		ledgerAppendIDs = make(map[model.LedgerAppendID]model.LedgerPosition)
		data.appendIDs[ledgerID] = ledgerAppendIDs
	}
	existing := make([]model.LedgerRecord, len(items))
	existingCount := 0
	for i, item := range items {
		position, ok := ledgerAppendIDs[item.AppendID]
		if !ok {
			continue
		}
		existingCount++
		existing[i] = data.records[position]
		if existing[i].Payload != item.Payload {
			return nil, model.ErrLedgerAppendConflict
		}
	}
	if existingCount == len(items) {
		return existing, nil
	}
	if existingCount != 0 {
		return nil, model.ErrLedgerBatchConflict
	}
	createdAt := time.Now().UTC()
	records := make([]model.LedgerRecord, len(items))
	for i, item := range items {
		data.nextPosition++
		record := model.LedgerRecord{LedgerID: ledgerID, Position: data.nextPosition, AppendID: item.AppendID, CreatedAt: createdAt, Payload: item.Payload}
		data.records[record.Position] = record
		ledgerAppendIDs[item.AppendID] = record.Position
		records[i] = record
	}
	return records, nil
}

func (s *RamStore) LedgerReadBackward(space model.TenancySpace, selector model.LedgerSelector, cursorValue *string, limit int) (model.LedgerPage, error) {
	if limit < 1 || limit > model.LedgerMaxPageSize {
		return model.LedgerPage{}, model.ErrLedgerInvalidPageSize
	}
	selected, selectorKey, err := normalizeRamLedgerSelector(selector)
	if err != nil {
		return model.LedgerPage{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := s.ledgers[space]
	if data == nil {
		return model.LedgerPage{Records: []model.LedgerRecord{}}, nil
	}
	upper, before := data.nextPosition, model.LedgerPosition(0)
	if cursorValue != nil {
		cursor, err := decodeRamLedgerCursor(*cursorValue)
		if err != nil || cursor.Selector != selectorKey {
			return model.LedgerPage{}, model.ErrLedgerInvalidCursor
		}
		upper, before = model.LedgerPosition(cursor.Upper), model.LedgerPosition(cursor.Before)
	}
	records := make([]model.LedgerRecord, 0, limit+1)
	for position := upper; position > 0 && len(records) <= limit; position-- {
		if before > 0 && position >= before {
			continue
		}
		record, ok := data.records[position]
		if ok && ramLedgerSelected(record, selector.All, selector.Prefix, selected) {
			records = append(records, record)
		}
	}
	page := model.LedgerPage{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		next := encodeRamLedgerCursor(ramLedgerCursor{Version: 1, Upper: int64(upper), Before: int64(page.Records[len(page.Records)-1].Position), Selector: selectorKey})
		page.NextCursor = &next
	}
	return page, nil
}

func (s *RamStore) LedgerReadForward(space model.TenancySpace, selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error) {
	if limit < 1 || limit > model.LedgerMaxPageSize {
		return nil, model.ErrLedgerInvalidPageSize
	}
	if after < 0 || (through != nil && *through < 0) {
		return nil, model.ErrLedgerInvalidCursor
	}
	selected, _, err := normalizeRamLedgerSelector(selector)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := s.ledgers[space]
	if data == nil {
		return []model.LedgerRecord{}, nil
	}
	upper := data.nextPosition
	if through != nil && *through < upper {
		upper = *through
	}
	records := make([]model.LedgerRecord, 0, limit)
	for position := after + 1; position <= upper && len(records) < limit; position++ {
		record, ok := data.records[position]
		if ok && ramLedgerSelected(record, selector.All, selector.Prefix, selected) {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s *RamStore) LedgerDelete(space model.TenancySpace, ledgerID model.LedgerID) error {
	if !ramLedgerIDPattern.MatchString(string(ledgerID)) {
		return model.ErrLedgerInvalidID
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
