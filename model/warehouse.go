package model

import (
	"errors"
	"time"
)

type BlobID string
type PutID string

// PutBlobRequest identifies an immutable write within a tenancy space.
// Checksum is opaque metadata; it is not computed or validated yet.
type PutBlobRequest struct {
	PutID       PutID
	ContentType string
	Value       []byte
	Checksum    string
}

type Blob struct {
	ID          BlobID
	PutID       PutID
	ContentType string
	Value       []byte
	Checksum    string
	CreatedAt   time.Time
}

var (
	ErrWarehouseUnsupported  = errors.New("warehouse is not supported by this store")
	ErrWarehouseInvalidPutID = errors.New("warehouse put ID must not be empty")
	ErrWarehousePutConflict  = errors.New("warehouse put ID already exists with different content or metadata")
	ErrBlobNotFound          = errors.New("blob not found")
)
