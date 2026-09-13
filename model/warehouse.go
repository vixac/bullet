package model

import (
	"errors"
	"time"
)

type BlobID string
type PutID string

// CheckpointID identifies one immutable snapshot representation.
type CheckpointID string

// CheckpointWriteID makes publishing a checkpoint idempotent.
type CheckpointWriteID string

// CheckpointSequenceID identifies the logical state lineage being snapshotted,
// such as a workspace. A sequence has one source-ledger lineage at a time.
type CheckpointSequenceID string

// StreamGeneration distinguishes a recreated source ledger from an earlier,
// destructively reset ledger with the same ID.
type StreamGeneration int64

type CheckpointStatus string

const (
	CheckpointReady   CheckpointStatus = "ready"
	CheckpointCorrupt CheckpointStatus = "corrupt"
)

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

// PutCheckpointRequest atomically stores an immutable state representation and
// publishes its source-ledger boundary. Checksum describes the stored bytes;
// StateChecksum identifies the uncompressed logical state.
type PutCheckpointRequest struct {
	WriteID          CheckpointWriteID
	SequenceID       CheckpointSequenceID
	SourceLedgerID   LedgerID
	StreamGeneration StreamGeneration
	CoveredThrough   LedgerPosition

	ContentType   string
	Codec         string
	CodecVersion  string
	SchemaVersion string
	Value         []byte
	Checksum      string
	StateChecksum string
}

// CheckpointRef is the metadata needed to hydrate state and replay its ledger
// tail. It intentionally includes the source ledger and covered position so a
// caller need not query Ledger before locating a checkpoint.
type CheckpointRef struct {
	ID               CheckpointID
	SequenceID       CheckpointSequenceID
	SourceLedgerID   LedgerID
	StreamGeneration StreamGeneration
	CoveredThrough   LedgerPosition
	BlobID           BlobID
	ContentType      string
	Codec            string
	CodecVersion     string
	SchemaVersion    string
	Checksum         string
	StateChecksum    string
	Status           CheckpointStatus
	CreatedAt        time.Time
}

// Checkpoint combines a checkpoint reference with its immutable bytes.
type Checkpoint struct {
	Ref   CheckpointRef
	Value []byte
}

var (
	ErrWarehouseUnsupported  = errors.New("warehouse is not supported by this store")
	ErrWarehouseInvalidPutID = errors.New("warehouse put ID must not be empty")
	ErrWarehousePutConflict  = errors.New("warehouse put ID already exists with different content or metadata")
	ErrBlobNotFound          = errors.New("blob not found")
	ErrCheckpointUnsupported = errors.New("checkpoints are not supported by this store")
	ErrCheckpointInvalid     = errors.New("invalid checkpoint")
	ErrCheckpointConflict    = errors.New("checkpoint write conflicts with existing checkpoint")
	ErrCheckpointNotFound    = errors.New("checkpoint not found")
	ErrCheckpointCorrupt     = errors.New("checkpoint is marked corrupt")
)
