package sqlite_store

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"time"

	"github.com/vixac/bullet/store/store_interface"
)

var ledgerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type ledgerCursor struct {
	Version  int    `json:"v"`
	Upper    int64  `json:"upper"`
	Before   int64  `json:"before"`
	Selector string `json:"selector"`
}

func validateLedgerWrite(ledgerID store_interface.LedgerID, appendID store_interface.LedgerAppendID, payload string) error {
	if !ledgerIDPattern.MatchString(string(ledgerID)) {
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

func normalizeLedgerSelector(selector store_interface.LedgerSelector) ([]store_interface.LedgerID, string, error) {
	if selector.All == (len(selector.LedgerIDs) > 0) {
		return nil, "", store_interface.ErrLedgerInvalidSelector
	}
	if selector.All {
		return nil, "all", nil
	}
	if len(selector.LedgerIDs) > store_interface.LedgerMaxSelected {
		return nil, "", store_interface.ErrLedgerInvalidSelector
	}
	ids := append([]store_interface.LedgerID(nil), selector.LedgerIDs...)
	for _, id := range ids {
		if !ledgerIDPattern.MatchString(string(id)) {
			return nil, "", store_interface.ErrLedgerInvalidID
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i := 1; i < len(ids); i++ {
		if ids[i] == ids[i-1] {
			return nil, "", store_interface.ErrLedgerInvalidSelector
		}
	}
	h := sha256.New()
	for _, id := range ids {
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return ids, base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

func selectorSQL(ids []store_interface.LedgerID, all bool, args *[]any) string {
	if all {
		return ""
	}
	clause := " AND ledger_id IN (" + placeholders(len(ids)) + ")"
	for _, id := range ids {
		*args = append(*args, string(id))
	}
	return clause
}

func allocateLedgerPositions(tx *sql.Tx, space store_interface.TenancySpace, count int) (int64, error) {
	if _, err := tx.Exec(`
		INSERT INTO ledger_positions (app_id, tenancy_id, next_position)
		VALUES (?, ?, 0)
		ON CONFLICT(app_id, tenancy_id) DO NOTHING
	`, space.AppId, space.TenancyId); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`
		UPDATE ledger_positions
		SET next_position = next_position + ?
		WHERE app_id = ? AND tenancy_id = ?
	`, count, space.AppId, space.TenancyId); err != nil {
		return 0, err
	}
	var last int64
	if err := tx.QueryRow(`
		SELECT next_position FROM ledger_positions
		WHERE app_id = ? AND tenancy_id = ?
	`, space.AppId, space.TenancyId).Scan(&last); err != nil {
		return 0, err
	}
	return last - int64(count) + 1, nil
}

func scanLedgerRecord(scanner interface{ Scan(...any) error }) (store_interface.LedgerRecord, error) {
	var record store_interface.LedgerRecord
	var ledgerID, appendID string
	var position, createdAt int64
	if err := scanner.Scan(&ledgerID, &position, &appendID, &createdAt, &record.Payload); err != nil {
		return record, err
	}
	record.LedgerID = store_interface.LedgerID(ledgerID)
	record.Position = store_interface.LedgerPosition(position)
	record.AppendID = store_interface.LedgerAppendID(appendID)
	record.CreatedAt = time.Unix(0, createdAt).UTC()
	return record, nil
}

func existingLedgerRecord(tx *sql.Tx, space store_interface.TenancySpace, ledgerID store_interface.LedgerID, appendID store_interface.LedgerAppendID) (store_interface.LedgerRecord, []byte, error) {
	row := tx.QueryRow(`
		SELECT ledger_id, position, append_id, created_at_ns, payload, payload_hash
		FROM ledger
		WHERE app_id = ? AND tenancy_id = ? AND ledger_id = ? AND append_id = ?
	`, space.AppId, space.TenancyId, string(ledgerID), string(appendID))
	var record store_interface.LedgerRecord
	var id, aid string
	var position, createdAt int64
	var hash []byte
	err := row.Scan(&id, &position, &aid, &createdAt, &record.Payload, &hash)
	if err != nil {
		return record, nil, err
	}
	record.LedgerID = store_interface.LedgerID(id)
	record.Position = store_interface.LedgerPosition(position)
	record.AppendID = store_interface.LedgerAppendID(aid)
	record.CreatedAt = time.Unix(0, createdAt).UTC()
	return record, hash, nil
}

func (s *SQLiteStore) LedgerAppend(space store_interface.TenancySpace, ledgerID store_interface.LedgerID, appendID store_interface.LedgerAppendID, payload string) (store_interface.LedgerRecord, error) {
	items, err := s.LedgerAppendMany(space, ledgerID, []store_interface.LedgerAppendItem{{AppendID: appendID, Payload: payload}})
	if err != nil {
		return store_interface.LedgerRecord{}, err
	}
	return items[0], nil
}

func (s *SQLiteStore) LedgerAppendMany(space store_interface.TenancySpace, ledgerID store_interface.LedgerID, items []store_interface.LedgerAppendItem) ([]store_interface.LedgerRecord, error) {
	if !ledgerIDPattern.MatchString(string(ledgerID)) {
		return nil, store_interface.ErrLedgerInvalidID
	}
	if len(items) == 0 {
		return []store_interface.LedgerRecord{}, nil
	}
	seen := make(map[store_interface.LedgerAppendID]struct{}, len(items))
	for _, item := range items {
		if err := validateLedgerWrite(ledgerID, item.AppendID, item.Payload); err != nil {
			return nil, err
		}
		if _, exists := seen[item.AppendID]; exists {
			return nil, store_interface.ErrLedgerInvalidAppendID
		}
		seen[item.AppendID] = struct{}{}
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	first, err := allocateLedgerPositions(tx, space, len(items))
	if err != nil {
		return nil, err
	}

	existing := make([]store_interface.LedgerRecord, len(items))
	existingCount := 0
	for i, item := range items {
		record, hash, err := existingLedgerRecord(tx, space, ledgerID, item.AppendID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		existingCount++
		wanted := sha256.Sum256([]byte(item.Payload))
		if !bytes.Equal(hash, wanted[:]) || record.Payload != item.Payload {
			return nil, store_interface.ErrLedgerAppendConflict
		}
		existing[i] = record
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
		position := first + int64(i)
		hash := sha256.Sum256([]byte(item.Payload))
		if _, err := tx.Exec(`
			INSERT INTO ledger
			(app_id, tenancy_id, ledger_id, position, append_id, created_at_ns, payload, payload_hash)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, space.AppId, space.TenancyId, string(ledgerID), position, string(item.AppendID), createdAt.UnixNano(), item.Payload, hash[:]); err != nil {
			return nil, err
		}
		records[i] = store_interface.LedgerRecord{LedgerID: ledgerID, Position: store_interface.LedgerPosition(position), AppendID: item.AppendID, CreatedAt: createdAt, Payload: item.Payload}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return records, nil
}

func encodeLedgerCursor(cursor ledgerCursor) (string, error) {
	b, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodeLedgerCursor(value string) (ledgerCursor, error) {
	var cursor ledgerCursor
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor, store_interface.ErrLedgerInvalidCursor
	}
	if err := json.Unmarshal(b, &cursor); err != nil || cursor.Version != 1 || cursor.Upper < 0 || cursor.Before < 0 {
		return cursor, store_interface.ErrLedgerInvalidCursor
	}
	return cursor, nil
}

func validatePageSize(limit int) error {
	if limit < 1 || limit > store_interface.LedgerMaxPageSize {
		return store_interface.ErrLedgerInvalidPageSize
	}
	return nil
}

func (s *SQLiteStore) LedgerReadBackward(space store_interface.TenancySpace, selector store_interface.LedgerSelector, cursorValue *string, limit int) (store_interface.LedgerPage, error) {
	if err := validatePageSize(limit); err != nil {
		return store_interface.LedgerPage{}, err
	}
	ids, selectorKey, err := normalizeLedgerSelector(selector)
	if err != nil {
		return store_interface.LedgerPage{}, err
	}
	upper := int64(0)
	before := int64(0)
	if cursorValue != nil {
		cursor, err := decodeLedgerCursor(*cursorValue)
		if err != nil || cursor.Selector != selectorKey {
			return store_interface.LedgerPage{}, store_interface.ErrLedgerInvalidCursor
		}
		upper, before = cursor.Upper, cursor.Before
	} else {
		args := []any{space.AppId, space.TenancyId}
		query := `SELECT COALESCE(MAX(position), 0) FROM ledger WHERE app_id = ? AND tenancy_id = ?`
		query += selectorSQL(ids, selector.All, &args)
		if err := s.db.QueryRow(query, args...).Scan(&upper); err != nil {
			return store_interface.LedgerPage{}, err
		}
	}
	if upper == 0 {
		return store_interface.LedgerPage{Records: []store_interface.LedgerRecord{}}, nil
	}

	args := []any{space.AppId, space.TenancyId, upper}
	query := `SELECT ledger_id, position, append_id, created_at_ns, payload FROM ledger WHERE app_id = ? AND tenancy_id = ? AND position <= ?`
	if before > 0 {
		query += ` AND position < ?`
		args = append(args, before)
	}
	query += selectorSQL(ids, selector.All, &args)
	query += ` ORDER BY position DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return store_interface.LedgerPage{}, err
	}
	defer rows.Close()
	records := make([]store_interface.LedgerRecord, 0, limit+1)
	for rows.Next() {
		record, err := scanLedgerRecord(rows)
		if err != nil {
			return store_interface.LedgerPage{}, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return store_interface.LedgerPage{}, err
	}
	page := store_interface.LedgerPage{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		next, err := encodeLedgerCursor(ledgerCursor{Version: 1, Upper: upper, Before: int64(page.Records[len(page.Records)-1].Position), Selector: selectorKey})
		if err != nil {
			return store_interface.LedgerPage{}, err
		}
		page.NextCursor = &next
	}
	return page, nil
}

func (s *SQLiteStore) LedgerReadForward(space store_interface.TenancySpace, selector store_interface.LedgerSelector, after store_interface.LedgerPosition, through *store_interface.LedgerPosition, limit int) ([]store_interface.LedgerRecord, error) {
	if err := validatePageSize(limit); err != nil || after < 0 || (through != nil && *through < 0) {
		if err != nil {
			return nil, err
		}
		return nil, store_interface.ErrLedgerInvalidCursor
	}
	ids, _, err := normalizeLedgerSelector(selector)
	if err != nil {
		return nil, err
	}
	args := []any{space.AppId, space.TenancyId, int64(after)}
	query := `SELECT ledger_id, position, append_id, created_at_ns, payload FROM ledger WHERE app_id = ? AND tenancy_id = ? AND position > ?`
	if through != nil {
		query += ` AND position <= ?`
		args = append(args, int64(*through))
	}
	query += selectorSQL(ids, selector.All, &args)
	query += ` ORDER BY position ASC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]store_interface.LedgerRecord, 0, limit)
	for rows.Next() {
		record, err := scanLedgerRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *SQLiteStore) LedgerDelete(space store_interface.TenancySpace, ledgerID store_interface.LedgerID) error {
	if !ledgerIDPattern.MatchString(string(ledgerID)) {
		return store_interface.ErrLedgerInvalidID
	}
	_, err := s.db.Exec(`DELETE FROM ledger WHERE app_id = ? AND tenancy_id = ? AND ledger_id = ?`, space.AppId, space.TenancyId, string(ledgerID))
	return err
}
