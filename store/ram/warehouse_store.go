package ram

import (
	"bytes"
	"context"
	"crypto/rand"
	"time"

	"github.com/vixac/bullet/model"
)

type warehouseSpace struct {
	blobs map[model.BlobID]model.Blob
	puts  map[model.PutID]model.BlobID
}

func cloneBlob(b model.Blob) model.Blob {
	b.Value = bytes.Clone(b.Value)
	return b
}

func (s *RamStore) WarehousePut(ctx context.Context, space model.TenancySpace, req model.PutBlobRequest) (model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return model.Blob{}, err
	}
	if req.PutID == "" {
		return model.Blob{}, model.ErrWarehouseInvalidPutID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.Blob{}, err
	}
	if s.warehouse == nil {
		s.warehouse = make(map[model.TenancySpace]*warehouseSpace)
	}
	data := s.warehouse[space]
	if data == nil {
		data = &warehouseSpace{blobs: make(map[model.BlobID]model.Blob), puts: make(map[model.PutID]model.BlobID)}
		s.warehouse[space] = data
	}
	if id, ok := data.puts[req.PutID]; ok {
		b := data.blobs[id]
		if b.ContentType != req.ContentType || b.Checksum != req.Checksum || !bytes.Equal(b.Value, req.Value) {
			return model.Blob{}, model.ErrWarehousePutConflict
		}
		return cloneBlob(b), nil
	}
	id := model.BlobID(rand.Text())
	for {
		if _, exists := data.blobs[id]; !exists {
			break
		}
		id = model.BlobID(rand.Text())
	}
	b := model.Blob{ID: id, PutID: req.PutID, ContentType: req.ContentType, Value: bytes.Clone(req.Value), Checksum: req.Checksum, CreatedAt: time.Now().UTC()}
	data.blobs[id] = b
	data.puts[req.PutID] = id
	return cloneBlob(b), nil
}

func (s *RamStore) WarehouseGet(ctx context.Context, space model.TenancySpace, id model.BlobID) (model.Blob, error) {
	found, err := s.WarehouseGetMany(ctx, space, []model.BlobID{id})
	if err != nil {
		return model.Blob{}, err
	}
	b, ok := found[id]
	if !ok {
		return model.Blob{}, model.ErrBlobNotFound
	}
	return b, nil
}

func (s *RamStore) WarehouseGetMany(ctx context.Context, space model.TenancySpace, ids []model.BlobID) (map[model.BlobID]model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[model.BlobID]model.Blob)
	data := s.warehouse[space]
	if data == nil {
		return result, nil
	}
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if b, ok := data.blobs[id]; ok {
			result[id] = cloneBlob(b)
		}
	}
	return result, nil
}
