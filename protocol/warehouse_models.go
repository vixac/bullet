package protocol

import (
	"fmt"
	"strconv"
	"time"

	"github.com/vixac/bullet/model"
)

// PutBlobRequest is the JSON body for POST /warehouse/blobs.
type PutBlobRequest struct {
	PutID       model.PutID `json:"put_id"`
	ContentType string      `json:"content_type"`
	Value       []byte      `json:"value"`
	Checksum    string      `json:"checksum"`
}

type Blob struct {
	ID          model.BlobID `json:"id"`
	PutID       model.PutID  `json:"put_id"`
	ContentType string       `json:"content_type"`
	Value       []byte       `json:"value"`
	Checksum    string       `json:"checksum"`
	CreatedAt   time.Time    `json:"created_at"`
}

type WarehouseGetManyRequest struct {
	IDs []model.BlobID `json:"ids"`
}
type WarehouseGetManyResponse map[model.BlobID]Blob

// PutCheckpointRequest is the JSON body for POST /warehouse/checkpoints.
// CoveredThrough is a decimal string to retain the established ledger-position
// encoding without losing int64 precision in JSON consumers.
type PutCheckpointRequest struct {
	WriteID          model.CheckpointWriteID    `json:"write_id"`
	SequenceID       model.CheckpointSequenceID `json:"sequence_id"`
	SourceLedgerID   model.LedgerID             `json:"source_ledger_id"`
	StreamGeneration model.StreamGeneration     `json:"stream_generation"`
	CoveredThrough   string                     `json:"covered_through"`
	ContentType      string                     `json:"content_type"`
	Codec            string                     `json:"codec"`
	CodecVersion     string                     `json:"codec_version"`
	SchemaVersion    string                     `json:"schema_version"`
	Value            []byte                     `json:"value"`
	Checksum         string                     `json:"checksum"`
	StateChecksum    string                     `json:"state_checksum"`
}

type CheckpointRef struct {
	ID               model.CheckpointID         `json:"id"`
	SequenceID       model.CheckpointSequenceID `json:"sequence_id"`
	SourceLedgerID   model.LedgerID             `json:"source_ledger_id"`
	StreamGeneration model.StreamGeneration     `json:"stream_generation"`
	CoveredThrough   string                     `json:"covered_through"`
	BlobID           model.BlobID               `json:"blob_id"`
	ContentType      string                     `json:"content_type"`
	Codec            string                     `json:"codec"`
	CodecVersion     string                     `json:"codec_version"`
	SchemaVersion    string                     `json:"schema_version"`
	Checksum         string                     `json:"checksum"`
	StateChecksum    string                     `json:"state_checksum"`
	Status           model.CheckpointStatus     `json:"status"`
	CreatedAt        time.Time                  `json:"created_at"`
}

type Checkpoint struct {
	Ref   CheckpointRef `json:"ref"`
	Value []byte        `json:"value"`
}

type LatestCheckpointResponse struct {
	Checkpoint *CheckpointRef `json:"checkpoint"`
}

type CheckpointCandidatesResponse struct {
	Checkpoints []CheckpointRef `json:"checkpoints"`
}

func (r PutBlobRequest) Model() model.PutBlobRequest {
	return model.PutBlobRequest{PutID: r.PutID, ContentType: r.ContentType, Value: r.Value, Checksum: r.Checksum}
}
func PutBlobRequestFromModel(r model.PutBlobRequest) PutBlobRequest {
	return PutBlobRequest{PutID: r.PutID, ContentType: r.ContentType, Value: r.Value, Checksum: r.Checksum}
}
func (b Blob) Model() model.Blob {
	return model.Blob{ID: b.ID, PutID: b.PutID, ContentType: b.ContentType, Value: b.Value, Checksum: b.Checksum, CreatedAt: b.CreatedAt}
}
func BlobFromModel(b model.Blob) Blob {
	return Blob{ID: b.ID, PutID: b.PutID, ContentType: b.ContentType, Value: b.Value, Checksum: b.Checksum, CreatedAt: b.CreatedAt}
}
func BlobsFromModel(blobs map[model.BlobID]model.Blob) WarehouseGetManyResponse {
	if blobs == nil {
		return nil
	}
	result := make(WarehouseGetManyResponse, len(blobs))
	for id, blob := range blobs {
		result[id] = BlobFromModel(blob)
	}
	return result
}

func parseCheckpointPosition(value string) (model.LedgerPosition, error) {
	position, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid checkpoint ledger position %q: %w", value, err)
	}
	return model.LedgerPosition(position), nil
}

func (r PutCheckpointRequest) Model() (model.PutCheckpointRequest, error) {
	coveredThrough, err := parseCheckpointPosition(r.CoveredThrough)
	if err != nil {
		return model.PutCheckpointRequest{}, fmt.Errorf("%w: %v", model.ErrCheckpointInvalid, err)
	}
	return model.PutCheckpointRequest{
		WriteID: r.WriteID, SequenceID: r.SequenceID, SourceLedgerID: r.SourceLedgerID,
		StreamGeneration: r.StreamGeneration, CoveredThrough: coveredThrough,
		ContentType: r.ContentType, Codec: r.Codec, CodecVersion: r.CodecVersion,
		SchemaVersion: r.SchemaVersion, Value: r.Value, Checksum: r.Checksum,
		StateChecksum: r.StateChecksum,
	}, nil
}

func PutCheckpointRequestFromModel(r model.PutCheckpointRequest) PutCheckpointRequest {
	return PutCheckpointRequest{
		WriteID: r.WriteID, SequenceID: r.SequenceID, SourceLedgerID: r.SourceLedgerID,
		StreamGeneration: r.StreamGeneration, CoveredThrough: strconv.FormatInt(int64(r.CoveredThrough), 10),
		ContentType: r.ContentType, Codec: r.Codec, CodecVersion: r.CodecVersion,
		SchemaVersion: r.SchemaVersion, Value: r.Value, Checksum: r.Checksum,
		StateChecksum: r.StateChecksum,
	}
}

func (r CheckpointRef) Model() (model.CheckpointRef, error) {
	coveredThrough, err := parseCheckpointPosition(r.CoveredThrough)
	if err != nil {
		return model.CheckpointRef{}, err
	}
	return model.CheckpointRef{
		ID: r.ID, SequenceID: r.SequenceID, SourceLedgerID: r.SourceLedgerID,
		StreamGeneration: r.StreamGeneration, CoveredThrough: coveredThrough, BlobID: r.BlobID,
		ContentType: r.ContentType, Codec: r.Codec, CodecVersion: r.CodecVersion,
		SchemaVersion: r.SchemaVersion, Checksum: r.Checksum, StateChecksum: r.StateChecksum,
		Status: r.Status, CreatedAt: r.CreatedAt,
	}, nil
}

func CheckpointRefFromModel(r model.CheckpointRef) CheckpointRef {
	return CheckpointRef{
		ID: r.ID, SequenceID: r.SequenceID, SourceLedgerID: r.SourceLedgerID,
		StreamGeneration: r.StreamGeneration, CoveredThrough: strconv.FormatInt(int64(r.CoveredThrough), 10), BlobID: r.BlobID,
		ContentType: r.ContentType, Codec: r.Codec, CodecVersion: r.CodecVersion,
		SchemaVersion: r.SchemaVersion, Checksum: r.Checksum, StateChecksum: r.StateChecksum,
		Status: r.Status, CreatedAt: r.CreatedAt,
	}
}

func (c Checkpoint) Model() (model.Checkpoint, error) {
	ref, err := c.Ref.Model()
	if err != nil {
		return model.Checkpoint{}, err
	}
	return model.Checkpoint{Ref: ref, Value: c.Value}, nil
}

func CheckpointFromModel(c model.Checkpoint) Checkpoint {
	return Checkpoint{Ref: CheckpointRefFromModel(c.Ref), Value: c.Value}
}

func CheckpointRefsFromModel(refs []model.CheckpointRef) []CheckpointRef {
	result := make([]CheckpointRef, len(refs))
	for i, ref := range refs {
		result[i] = CheckpointRefFromModel(ref)
	}
	return result
}

func (r CheckpointCandidatesResponse) Models() ([]model.CheckpointRef, error) {
	result := make([]model.CheckpointRef, len(r.Checkpoints))
	for i, ref := range r.Checkpoints {
		converted, err := ref.Model()
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}
	return result, nil
}
