package postgresql

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

	"github.com/vixac/bullet/model"
)

var postgresLedgerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type postgresLedgerCursor struct {
	Version  int    `json:"v"`
	Upper    int64  `json:"upper"`
	Before   int64  `json:"before"`
	Selector string `json:"selector"`
}

func validatePostgresLedgerWrite(ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) error {
	if !postgresLedgerIDPattern.MatchString(string(ledgerID)) {
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

func normalizePostgresLedgerSelector(selector model.LedgerSelector) ([]model.LedgerID, string, error) {
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
		if !postgresLedgerIDPattern.MatchString(selector.Prefix) {
			return nil, "", model.ErrLedgerInvalidID
		}
		return nil, "prefix:" + selector.Prefix, nil
	}
	ids := append([]model.LedgerID(nil), selector.LedgerIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	for i, id := range ids {
		if !postgresLedgerIDPattern.MatchString(string(id)) {
			return nil, "", model.ErrLedgerInvalidID
		}
		if i > 0 && id == ids[i-1] {
			return nil, "", model.ErrLedgerInvalidSelector
		}
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	return ids, base64.RawURLEncoding.EncodeToString(h.Sum(nil)), nil
}

func postgresLedgerSelectorSQL(ids []model.LedgerID, all bool, prefix string, start int, args *[]any) string {
	if all {
		return ""
	}
	if prefix != "" {
		upper := prefix[:len(prefix)-1] + string(prefix[len(prefix)-1]+1)
		*args = append(*args, prefix, upper)
		return ` AND ledger_id COLLATE "C" >= ` + placeholders(start, 1) + ` AND ledger_id COLLATE "C" < ` + placeholders(start+1, 1)
	}
	clause := " AND ledger_id IN (" + placeholders(start, len(ids)) + ")"
	for _, id := range ids {
		*args = append(*args, string(id))
	}
	return clause
}

func allocatePostgresLedgerPositions(tx *sql.Tx, space model.TenancySpace, count int) (int64, error) {
	var last int64
	err := tx.QueryRow(`
		INSERT INTO ledger_positions (app_id, tenancy_id, next_position)
		VALUES ($1, $2, $3)
		ON CONFLICT (app_id, tenancy_id)
		DO UPDATE SET next_position = ledger_positions.next_position + EXCLUDED.next_position
		RETURNING next_position
	`, space.AppId, space.TenancyId, count).Scan(&last)
	return last - int64(count) + 1, err
}

func scanPostgresLedgerRecord(scanner interface{ Scan(...any) error }) (model.LedgerRecord, error) {
	var record model.LedgerRecord
	var ledgerID, appendID string
	var position int64
	if err := scanner.Scan(&ledgerID, &position, &appendID, &record.CreatedAt, &record.Payload); err != nil {
		return record, err
	}
	record.LedgerID = model.LedgerID(ledgerID)
	record.Position = model.LedgerPosition(position)
	record.AppendID = model.LedgerAppendID(appendID)
	record.CreatedAt = record.CreatedAt.UTC()
	return record, nil
}

func existingPostgresLedgerRecord(tx *sql.Tx, space model.TenancySpace, ledgerID model.LedgerID, appendID model.LedgerAppendID) (model.LedgerRecord, []byte, error) {
	row := tx.QueryRow(`
		SELECT ledger_id, position, append_id, created_at, payload, payload_hash
		FROM ledger WHERE app_id = $1 AND tenancy_id = $2 AND ledger_id = $3 AND append_id = $4
	`, space.AppId, space.TenancyId, string(ledgerID), string(appendID))
	var record model.LedgerRecord
	var id, aid string
	var position int64
	var hash []byte
	err := row.Scan(&id, &position, &aid, &record.CreatedAt, &record.Payload, &hash)
	record.LedgerID, record.Position, record.AppendID = model.LedgerID(id), model.LedgerPosition(position), model.LedgerAppendID(aid)
	record.CreatedAt = record.CreatedAt.UTC()
	return record, hash, err
}

func (s *PostgreSQLStore) LedgerAppend(space model.TenancySpace, ledgerID model.LedgerID, appendID model.LedgerAppendID, payload string) (model.LedgerRecord, error) {
	records, err := s.LedgerAppendMany(space, ledgerID, []model.LedgerAppendItem{{AppendID: appendID, Payload: payload}})
	if err != nil {
		return model.LedgerRecord{}, err
	}
	return records[0], nil
}

func (s *PostgreSQLStore) LedgerAppendMany(space model.TenancySpace, ledgerID model.LedgerID, items []model.LedgerAppendItem) ([]model.LedgerRecord, error) {
	if !postgresLedgerIDPattern.MatchString(string(ledgerID)) {
		return nil, model.ErrLedgerInvalidID
	}
	if len(items) == 0 {
		return []model.LedgerRecord{}, nil
	}
	seen := make(map[model.LedgerAppendID]struct{}, len(items))
	for _, item := range items {
		if err := validatePostgresLedgerWrite(ledgerID, item.AppendID, item.Payload); err != nil {
			return nil, err
		}
		if _, duplicate := seen[item.AppendID]; duplicate {
			return nil, model.ErrLedgerInvalidAppendID
		}
		seen[item.AppendID] = struct{}{}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	first, err := allocatePostgresLedgerPositions(tx, space, len(items))
	if err != nil {
		return nil, err
	}
	existing := make([]model.LedgerRecord, len(items))
	existingCount := 0
	for i, item := range items {
		record, hash, err := existingPostgresLedgerRecord(tx, space, ledgerID, item.AppendID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		existingCount++
		wanted := sha256.Sum256([]byte(item.Payload))
		if !bytes.Equal(hash, wanted[:]) || record.Payload != item.Payload {
			return nil, model.ErrLedgerAppendConflict
		}
		existing[i] = record
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
		position := first + int64(i)
		hash := sha256.Sum256([]byte(item.Payload))
		if _, err := tx.Exec(`
			INSERT INTO ledger (app_id, tenancy_id, ledger_id, position, append_id, created_at, payload, payload_hash)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, space.AppId, space.TenancyId, string(ledgerID), position, string(item.AppendID), createdAt, item.Payload, hash[:]); err != nil {
			return nil, err
		}
		records[i] = model.LedgerRecord{LedgerID: ledgerID, Position: model.LedgerPosition(position), AppendID: item.AppendID, CreatedAt: createdAt, Payload: item.Payload}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return records, nil
}

func encodePostgresLedgerCursor(cursor postgresLedgerCursor) string {
	b, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodePostgresLedgerCursor(value string) (postgresLedgerCursor, error) {
	var cursor postgresLedgerCursor
	b, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || json.Unmarshal(b, &cursor) != nil || cursor.Version != 1 || cursor.Upper < 0 || cursor.Before < 0 {
		return cursor, model.ErrLedgerInvalidCursor
	}
	return cursor, nil
}

func (s *PostgreSQLStore) LedgerReadBackward(space model.TenancySpace, selector model.LedgerSelector, cursorValue *string, limit int) (model.LedgerPage, error) {
	if limit < 1 || limit > model.LedgerMaxPageSize {
		return model.LedgerPage{}, model.ErrLedgerInvalidPageSize
	}
	ids, selectorKey, err := normalizePostgresLedgerSelector(selector)
	if err != nil {
		return model.LedgerPage{}, err
	}
	var upper, before int64
	if cursorValue != nil {
		cursor, err := decodePostgresLedgerCursor(*cursorValue)
		if err != nil || cursor.Selector != selectorKey {
			return model.LedgerPage{}, model.ErrLedgerInvalidCursor
		}
		upper, before = cursor.Upper, cursor.Before
	} else {
		args := []any{space.AppId, space.TenancyId}
		query := `SELECT COALESCE(MAX(position), 0) FROM ledger WHERE app_id = $1 AND tenancy_id = $2`
		query += postgresLedgerSelectorSQL(ids, selector.All, selector.Prefix, 3, &args)
		if err := s.db.QueryRow(query, args...).Scan(&upper); err != nil {
			return model.LedgerPage{}, err
		}
	}
	if upper == 0 {
		return model.LedgerPage{Records: []model.LedgerRecord{}}, nil
	}
	args := []any{space.AppId, space.TenancyId, upper}
	query := `SELECT ledger_id, position, append_id, created_at, payload FROM ledger WHERE app_id = $1 AND tenancy_id = $2 AND position <= $3`
	nextArg := 4
	if before > 0 {
		query += ` AND position < $4`
		args = append(args, before)
		nextArg++
	}
	query += postgresLedgerSelectorSQL(ids, selector.All, selector.Prefix, nextArg, &args)
	nextArg = len(args) + 1
	query += ` ORDER BY position DESC LIMIT ` + placeholders(nextArg, 1)
	args = append(args, limit+1)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return model.LedgerPage{}, err
	}
	defer rows.Close()
	records := make([]model.LedgerRecord, 0, limit+1)
	for rows.Next() {
		record, err := scanPostgresLedgerRecord(rows)
		if err != nil {
			return model.LedgerPage{}, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return model.LedgerPage{}, err
	}
	page := model.LedgerPage{Records: records}
	if len(records) > limit {
		page.Records = records[:limit]
		next := encodePostgresLedgerCursor(postgresLedgerCursor{Version: 1, Upper: upper, Before: int64(page.Records[len(page.Records)-1].Position), Selector: selectorKey})
		page.NextCursor = &next
	}
	return page, nil
}

func (s *PostgreSQLStore) LedgerReadForward(space model.TenancySpace, selector model.LedgerSelector, after model.LedgerPosition, through *model.LedgerPosition, limit int) ([]model.LedgerRecord, error) {
	if limit < 1 || limit > model.LedgerMaxPageSize {
		return nil, model.ErrLedgerInvalidPageSize
	}
	if after < 0 || (through != nil && *through < 0) {
		return nil, model.ErrLedgerInvalidCursor
	}
	ids, _, err := normalizePostgresLedgerSelector(selector)
	if err != nil {
		return nil, err
	}
	args := []any{space.AppId, space.TenancyId, int64(after)}
	query := `SELECT ledger_id, position, append_id, created_at, payload FROM ledger WHERE app_id = $1 AND tenancy_id = $2 AND position > $3`
	nextArg := 4
	if through != nil {
		query += ` AND position <= $4`
		args = append(args, int64(*through))
		nextArg++
	}
	query += postgresLedgerSelectorSQL(ids, selector.All, selector.Prefix, nextArg, &args)
	nextArg = len(args) + 1
	query += ` ORDER BY position ASC LIMIT ` + placeholders(nextArg, 1)
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]model.LedgerRecord, 0, limit)
	for rows.Next() {
		record, err := scanPostgresLedgerRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *PostgreSQLStore) LedgerDelete(space model.TenancySpace, ledgerID model.LedgerID) error {
	if !postgresLedgerIDPattern.MatchString(string(ledgerID)) {
		return model.ErrLedgerInvalidID
	}
	_, err := s.db.Exec(`DELETE FROM ledger WHERE app_id = $1 AND tenancy_id = $2 AND ledger_id = $3`, space.AppId, space.TenancyId, string(ledgerID))
	return err
}
