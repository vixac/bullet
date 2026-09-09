package store_interface

import (
	"context"
	"errors"
	"time"
)

type BlobID string
type PutID string

// PutBlobRequest identifies an immutable write within a tenancy space.
// Checksum is opaque metadata; it is not computed or validated yet.
type PutBlobRequest struct {
	PutID       PutID  `json:"put_id"`
	ContentType string `json:"content_type"`
	Value       []byte `json:"value"`
	Checksum    string `json:"checksum"`
}

type Blob struct {
	ID          BlobID    `json:"id"`
	PutID       PutID     `json:"put_id"`
	ContentType string    `json:"content_type"`
	Value       []byte    `json:"value"`
	Checksum    string    `json:"checksum"`
	CreatedAt   time.Time `json:"created_at"`
}

// WarehouseStore stores immutable blobs. Retrying a PutID with identical bytes
// and metadata returns the original blob; different bytes or metadata conflict.
// Get returns ErrBlobNotFound for missing IDs. GetMany omits missing IDs and
// returns a non-nil map, including for an empty request. Values are caller-owned.
type WarehouseStore interface {
	WarehousePut(context.Context, TenancySpace, PutBlobRequest) (Blob, error)
	WarehouseGet(context.Context, TenancySpace, BlobID) (Blob, error)
	WarehouseGetMany(context.Context, TenancySpace, []BlobID) (map[BlobID]Blob, error)
}

var (
	ErrWarehouseUnsupported  = errors.New("warehouse is not supported by this store")
	ErrWarehouseInvalidPutID = errors.New("warehouse put ID must not be empty")
	ErrWarehousePutConflict  = errors.New("warehouse put ID already exists with different content or metadata")
	ErrBlobNotFound          = errors.New("blob not found")
)
