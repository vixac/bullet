package sqlite_store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"

	"github.com/vixac/bullet/model"
)

const warehouseColumns = `id, put_id, content_type, value, checksum, created_at_ns`

func scanWarehouseBlob(row interface{ Scan(...any) error }) (model.Blob, error) {
	var b model.Blob
	var createdAt int64
	if err := row.Scan(&b.ID, &b.PutID, &b.ContentType, &b.Value, &b.Checksum, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Blob{}, model.ErrBlobNotFound
		}
		return model.Blob{}, err
	}
	b.CreatedAt = time.Unix(0, createdAt).UTC()
	return b, nil
}

func (s *SQLiteStore) WarehousePut(ctx context.Context, space model.TenancySpace, req model.PutBlobRequest) (model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return model.Blob{}, err
	}
	if req.PutID == "" {
		return model.Blob{}, model.ErrWarehouseInvalidPutID
	}
	// Insert first: the unique constraint arbitrates concurrent retries, including
	// writers using different store instances. Never overwrite an existing blob.
	_, err := s.db.ExecContext(ctx, `INSERT INTO warehouse
  (app_id, tenancy_id, id, put_id, content_type, value, checksum, created_at_ns)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT(app_id, tenancy_id, put_id) DO NOTHING`,
		space.AppId, space.TenancyId, rand.Text(), req.PutID, req.ContentType, req.Value, req.Checksum, time.Now().UTC().UnixNano())
	if err != nil {
		return model.Blob{}, err
	}
	// Blobs are immutable, so a separate read safely returns the winning insert.
	b, err := scanWarehouseBlob(s.db.QueryRowContext(ctx, `SELECT `+warehouseColumns+`
  FROM warehouse WHERE app_id=? AND tenancy_id=? AND put_id=?`, space.AppId, space.TenancyId, req.PutID))
	if err != nil {
		return model.Blob{}, err
	}
	if b.ContentType != req.ContentType || b.Checksum != req.Checksum || !bytes.Equal(b.Value, req.Value) {
		return model.Blob{}, model.ErrWarehousePutConflict
	}
	return b, nil
}

func (s *SQLiteStore) WarehouseGet(ctx context.Context, space model.TenancySpace, id model.BlobID) (model.Blob, error) {
	return scanWarehouseBlob(s.db.QueryRowContext(ctx, `SELECT `+warehouseColumns+`
  FROM warehouse WHERE app_id=? AND tenancy_id=? AND id=?`, space.AppId, space.TenancyId, id))
}

func (s *SQLiteStore) WarehouseGetMany(ctx context.Context, space model.TenancySpace, ids []model.BlobID) (map[model.BlobID]model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[model.BlobID]model.Blob)
	// Bound SQL parameters, not the public batch size. This also supports SQLite
	// builds with the older 999-variable limit.
	const chunkSize = 900
	for start := 0; start < len(ids); start += chunkSize {
		chunk := ids[start:min(start+chunkSize, len(ids))]
		args := []any{space.AppId, space.TenancyId}
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT `+warehouseColumns+`
   FROM warehouse WHERE app_id=? AND tenancy_id=? AND id IN (`+placeholders(len(chunk))+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			b, err := scanWarehouseBlob(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			result[b.ID] = b
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return result, nil
}

const checkpointColumns = `checkpoint_id, sequence_id, source_ledger_id,
 stream_generation, covered_through, blob_id, content_type, codec,
 codec_version, schema_version, checksum, state_checksum, status, created_at_ns`

const checkpointColumnsJoined = `c.checkpoint_id, c.sequence_id, c.source_ledger_id,
 c.stream_generation, c.covered_through, c.blob_id, c.content_type, c.codec,
 c.codec_version, c.schema_version, c.checksum, c.state_checksum, c.status, c.created_at_ns`

func validCheckpointRequest(req model.PutCheckpointRequest) bool {
	return req.WriteID != "" && req.SequenceID != "" && req.SourceLedgerID != "" &&
		req.StreamGeneration >= 0 && req.CoveredThrough >= 0 && req.Checksum != "" && req.StateChecksum != ""
}

func scanCheckpointRef(row interface{ Scan(...any) error }) (model.CheckpointRef, error) {
	var ref model.CheckpointRef
	var createdAt int64
	if err := row.Scan(&ref.ID, &ref.SequenceID, &ref.SourceLedgerID,
		&ref.StreamGeneration, &ref.CoveredThrough, &ref.BlobID, &ref.ContentType, &ref.Codec,
		&ref.CodecVersion, &ref.SchemaVersion, &ref.Checksum, &ref.StateChecksum, &ref.Status, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.CheckpointRef{}, model.ErrCheckpointNotFound
		}
		return model.CheckpointRef{}, err
	}
	ref.CreatedAt = time.Unix(0, createdAt).UTC()
	return ref, nil
}

func sameCheckpointRequest(ref model.CheckpointRef, value []byte, req model.PutCheckpointRequest) bool {
	return ref.SequenceID == req.SequenceID && ref.SourceLedgerID == req.SourceLedgerID &&
		ref.StreamGeneration == req.StreamGeneration && ref.CoveredThrough == req.CoveredThrough &&
		ref.ContentType == req.ContentType && ref.Codec == req.Codec && ref.CodecVersion == req.CodecVersion &&
		ref.SchemaVersion == req.SchemaVersion && ref.Checksum == req.Checksum &&
		ref.StateChecksum == req.StateChecksum && bytes.Equal(value, req.Value)
}

func checkpointRefAndValue(row interface{ Scan(...any) error }) (model.CheckpointRef, []byte, error) {
	var value []byte
	var ref model.CheckpointRef
	var createdAt int64
	if err := row.Scan(&ref.ID, &ref.SequenceID, &ref.SourceLedgerID,
		&ref.StreamGeneration, &ref.CoveredThrough, &ref.BlobID, &ref.ContentType, &ref.Codec,
		&ref.CodecVersion, &ref.SchemaVersion, &ref.Checksum, &ref.StateChecksum, &ref.Status, &createdAt, &value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.CheckpointRef{}, nil, model.ErrCheckpointNotFound
		}
		return model.CheckpointRef{}, nil, err
	}
	ref.CreatedAt = time.Unix(0, createdAt).UTC()
	return ref, value, nil
}

func (s *SQLiteStore) checkpointByWriteID(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, space model.TenancySpace, writeID model.CheckpointWriteID) (model.CheckpointRef, []byte, error) {
	return checkpointRefAndValue(q.QueryRowContext(ctx, `SELECT `+checkpointColumnsJoined+`, w.value
 FROM warehouse_checkpoints c JOIN warehouse w
 ON w.app_id=c.app_id AND w.tenancy_id=c.tenancy_id AND w.id=c.blob_id
 WHERE c.app_id=? AND c.tenancy_id=? AND c.write_id=?`, space.AppId, space.TenancyId, writeID))
}

func (s *SQLiteStore) WarehousePutCheckpoint(ctx context.Context, space model.TenancySpace, req model.PutCheckpointRequest) (model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return model.CheckpointRef{}, err
	}
	if !validCheckpointRequest(req) {
		return model.CheckpointRef{}, model.ErrCheckpointInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.CheckpointRef{}, err
	}
	defer tx.Rollback()
	if existing, value, err := s.checkpointByWriteID(ctx, tx, space, req.WriteID); err == nil {
		if !sameCheckpointRequest(existing, value, req) {
			return model.CheckpointRef{}, model.ErrCheckpointConflict
		}
		return existing, nil
	} else if !errors.Is(err, model.ErrCheckpointNotFound) {
		return model.CheckpointRef{}, err
	}

	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO warehouse_checkpoint_sequences
 (app_id, tenancy_id, sequence_id, source_ledger_id, stream_generation) VALUES (?, ?, ?, ?, ?)`,
		space.AppId, space.TenancyId, req.SequenceID, req.SourceLedgerID, req.StreamGeneration); err != nil {
		return model.CheckpointRef{}, err
	}
	var sourceLedgerID model.LedgerID
	var generation model.StreamGeneration
	if err := tx.QueryRowContext(ctx, `SELECT source_ledger_id, stream_generation FROM warehouse_checkpoint_sequences
 WHERE app_id=? AND tenancy_id=? AND sequence_id=?`, space.AppId, space.TenancyId, req.SequenceID).Scan(&sourceLedgerID, &generation); err != nil {
		return model.CheckpointRef{}, err
	}
	if sourceLedgerID != req.SourceLedgerID || generation != req.StreamGeneration {
		return model.CheckpointRef{}, model.ErrCheckpointConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO warehouse_checkpoint_states
 (app_id, tenancy_id, sequence_id, source_ledger_id, stream_generation, covered_through, state_checksum)
 VALUES (?, ?, ?, ?, ?, ?, ?)`, space.AppId, space.TenancyId, req.SequenceID, req.SourceLedgerID, req.StreamGeneration, req.CoveredThrough, req.StateChecksum); err != nil {
		return model.CheckpointRef{}, err
	}
	var stateChecksum string
	if err := tx.QueryRowContext(ctx, `SELECT state_checksum FROM warehouse_checkpoint_states
 WHERE app_id=? AND tenancy_id=? AND sequence_id=? AND source_ledger_id=? AND stream_generation=? AND covered_through=?`,
		space.AppId, space.TenancyId, req.SequenceID, req.SourceLedgerID, req.StreamGeneration, req.CoveredThrough).Scan(&stateChecksum); err != nil {
		return model.CheckpointRef{}, err
	}
	if stateChecksum != req.StateChecksum {
		return model.CheckpointRef{}, model.ErrCheckpointConflict
	}

	checkpointID, blobID := model.CheckpointID(rand.Text()), model.BlobID(rand.Text())
	now := time.Now().UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, `INSERT INTO warehouse
 (app_id, tenancy_id, id, put_id, content_type, value, checksum, created_at_ns)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, space.AppId, space.TenancyId, blobID,
		"checkpoint-internal:"+string(checkpointID), req.ContentType, req.Value, req.Checksum, now); err != nil {
		return model.CheckpointRef{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO warehouse_checkpoints
 (app_id, tenancy_id, checkpoint_id, write_id, sequence_id, source_ledger_id, stream_generation,
  covered_through, blob_id, content_type, codec, codec_version, schema_version, checksum,
  state_checksum, status, created_at_ns)
 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?)`,
		space.AppId, space.TenancyId, checkpointID, req.WriteID, req.SequenceID, req.SourceLedgerID,
		req.StreamGeneration, req.CoveredThrough, blobID, req.ContentType, req.Codec, req.CodecVersion,
		req.SchemaVersion, req.Checksum, req.StateChecksum, now)
	if err != nil {
		return model.CheckpointRef{}, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return model.CheckpointRef{}, err
	}
	if rows == 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM warehouse WHERE app_id=? AND tenancy_id=? AND id=?`, space.AppId, space.TenancyId, blobID); err != nil {
			return model.CheckpointRef{}, err
		}
		existing, value, err := s.checkpointByWriteID(ctx, tx, space, req.WriteID)
		if err != nil {
			return model.CheckpointRef{}, err
		}
		if !sameCheckpointRequest(existing, value, req) {
			return model.CheckpointRef{}, model.ErrCheckpointConflict
		}
		if err := tx.Commit(); err != nil {
			return model.CheckpointRef{}, err
		}
		return existing, nil
	}
	ref := model.CheckpointRef{ID: checkpointID, SequenceID: req.SequenceID, SourceLedgerID: req.SourceLedgerID,
		StreamGeneration: req.StreamGeneration, CoveredThrough: req.CoveredThrough, BlobID: blobID,
		ContentType: req.ContentType, Codec: req.Codec, CodecVersion: req.CodecVersion, SchemaVersion: req.SchemaVersion,
		Checksum: req.Checksum, StateChecksum: req.StateChecksum, Status: model.CheckpointReady, CreatedAt: time.Unix(0, now).UTC()}
	if err := tx.Commit(); err != nil {
		return model.CheckpointRef{}, err
	}
	return ref, nil
}

func (s *SQLiteStore) WarehouseGetLatestCheckpoint(ctx context.Context, space model.TenancySpace, sequenceID model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref, err := scanCheckpointRef(s.db.QueryRowContext(ctx, `SELECT `+checkpointColumns+` FROM warehouse_checkpoints
 WHERE app_id=? AND tenancy_id=? AND sequence_id=? AND status='ready'
 ORDER BY covered_through DESC, created_at_ns DESC, checkpoint_id DESC LIMIT 1`, space.AppId, space.TenancyId, sequenceID))
	if errors.Is(err, model.ErrCheckpointNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ref, nil
}

func (s *SQLiteStore) WarehouseFindCheckpoints(ctx context.Context, space model.TenancySpace, sequenceID model.CheckpointSequenceID, atOrBefore model.LedgerPosition, limit int) ([]model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if atOrBefore < 0 || limit < 0 {
		return nil, model.ErrCheckpointInvalid
	}
	query := `SELECT ` + checkpointColumns + ` FROM warehouse_checkpoints
 WHERE app_id=? AND tenancy_id=? AND sequence_id=? AND covered_through<=? AND status='ready'
 ORDER BY covered_through DESC, created_at_ns DESC, checkpoint_id DESC`
	args := []any{space.AppId, space.TenancyId, sequenceID, atOrBefore}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]model.CheckpointRef, 0)
	for rows.Next() {
		ref, err := scanCheckpointRef(rows)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return refs, nil
}

func (s *SQLiteStore) WarehouseGetCheckpoint(ctx context.Context, space model.TenancySpace, checkpointID model.CheckpointID) (model.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return model.Checkpoint{}, err
	}
	ref, value, err := checkpointRefAndValue(s.db.QueryRowContext(ctx, `SELECT `+checkpointColumnsJoined+`, w.value
 FROM warehouse_checkpoints c JOIN warehouse w ON w.app_id=c.app_id AND w.tenancy_id=c.tenancy_id AND w.id=c.blob_id
 WHERE c.app_id=? AND c.tenancy_id=? AND c.checkpoint_id=?`, space.AppId, space.TenancyId, checkpointID))
	if err != nil {
		return model.Checkpoint{}, err
	}
	if ref.Status == model.CheckpointCorrupt {
		return model.Checkpoint{}, model.ErrCheckpointCorrupt
	}
	return model.Checkpoint{Ref: ref, Value: bytes.Clone(value)}, nil
}

func (s *SQLiteStore) WarehouseMarkCheckpointCorrupt(ctx context.Context, space model.TenancySpace, checkpointID model.CheckpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE warehouse_checkpoints SET status='corrupt'
 WHERE app_id=? AND tenancy_id=? AND checkpoint_id=?`, space.AppId, space.TenancyId, checkpointID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return model.ErrCheckpointNotFound
	}
	return nil
}
