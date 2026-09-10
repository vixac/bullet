package postgresql

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

func (s *PostgreSQLStore) WarehousePut(ctx context.Context, space model.TenancySpace, req model.PutBlobRequest) (model.Blob, error) {
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
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
  ON CONFLICT(app_id, tenancy_id, put_id) DO NOTHING`,
		space.AppId, space.TenancyId, rand.Text(), req.PutID, req.ContentType, req.Value, req.Checksum, time.Now().UTC().UnixNano())
	if err != nil {
		return model.Blob{}, err
	}
	// Blobs are immutable, so a separate read safely returns the winning insert.
	b, err := scanWarehouseBlob(s.db.QueryRowContext(ctx, `SELECT `+warehouseColumns+`
  FROM warehouse WHERE app_id=$1 AND tenancy_id=$2 AND put_id=$3`, space.AppId, space.TenancyId, req.PutID))
	if err != nil {
		return model.Blob{}, err
	}
	if b.ContentType != req.ContentType || b.Checksum != req.Checksum || !bytes.Equal(b.Value, req.Value) {
		return model.Blob{}, model.ErrWarehousePutConflict
	}
	return b, nil
}

func (s *PostgreSQLStore) WarehouseGet(ctx context.Context, space model.TenancySpace, id model.BlobID) (model.Blob, error) {
	return scanWarehouseBlob(s.db.QueryRowContext(ctx, `SELECT `+warehouseColumns+`
  FROM warehouse WHERE app_id=$1 AND tenancy_id=$2 AND id=$3`, space.AppId, space.TenancyId, id))
}

func (s *PostgreSQLStore) WarehouseGetMany(ctx context.Context, space model.TenancySpace, ids []model.BlobID) (map[model.BlobID]model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[model.BlobID]model.Blob)
	// Bound the number of SQL parameters without limiting the public batch size.
	const chunkSize = 1000
	for start := 0; start < len(ids); start += chunkSize {
		chunk := ids[start:min(start+chunkSize, len(ids))]
		args := []any{space.AppId, space.TenancyId}
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := s.db.QueryContext(ctx, `SELECT `+warehouseColumns+`
   FROM warehouse WHERE app_id=$1 AND tenancy_id=$2 AND id IN (`+placeholders(3, len(chunk))+`)`, args...)
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
