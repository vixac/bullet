package ram

import (
	"bytes"
	"context"
	"crypto/rand"
	si "github.com/vixac/bullet/store/store_interface"
	"time"
)

type warehouseSpace struct {
	blobs map[si.BlobID]si.Blob
	puts  map[si.PutID]si.BlobID
}

func cloneBlob(b si.Blob) si.Blob {
	b.Value = bytes.Clone(b.Value)
	return b
}

func (s *RamStore) WarehousePut(ctx context.Context, space si.TenancySpace, req si.PutBlobRequest) (si.Blob, error) {
	if err := ctx.Err(); err != nil {
		return si.Blob{}, err
	}
	if req.PutID == "" {
		return si.Blob{}, si.ErrWarehouseInvalidPutID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return si.Blob{}, err
	}
	if s.warehouse == nil {
		s.warehouse = make(map[si.TenancySpace]*warehouseSpace)
	}
	data := s.warehouse[space]
	if data == nil {
		data = &warehouseSpace{blobs: make(map[si.BlobID]si.Blob), puts: make(map[si.PutID]si.BlobID)}
		s.warehouse[space] = data
	}
	if id, ok := data.puts[req.PutID]; ok {
		b := data.blobs[id]
		if b.ContentType != req.ContentType || b.Checksum != req.Checksum || !bytes.Equal(b.Value, req.Value) {
			return si.Blob{}, si.ErrWarehousePutConflict
		}
		return cloneBlob(b), nil
	}
	id := si.BlobID(rand.Text())
	for {
		if _, exists := data.blobs[id]; !exists {
			break
		}
		id = si.BlobID(rand.Text())
	}
	b := si.Blob{ID: id, PutID: req.PutID, ContentType: req.ContentType, Value: bytes.Clone(req.Value), Checksum: req.Checksum, CreatedAt: time.Now().UTC()}
	data.blobs[id] = b
	data.puts[req.PutID] = id
	return cloneBlob(b), nil
}

func (s *RamStore) WarehouseGet(ctx context.Context, space si.TenancySpace, id si.BlobID) (si.Blob, error) {
	found, err := s.WarehouseGetMany(ctx, space, []si.BlobID{id})
	if err != nil {
		return si.Blob{}, err
	}
	b, ok := found[id]
	if !ok {
		return si.Blob{}, si.ErrBlobNotFound
	}
	return b, nil
}

func (s *RamStore) WarehouseGetMany(ctx context.Context, space si.TenancySpace, ids []si.BlobID) (map[si.BlobID]si.Blob, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[si.BlobID]si.Blob)
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
