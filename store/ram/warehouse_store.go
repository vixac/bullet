package ram

import (
	"bytes"
	"context"
	"crypto/rand"
	"sort"
	"time"

	"github.com/vixac/bullet/model"
)

type warehouseSpace struct {
	blobs            map[model.BlobID]model.Blob
	puts             map[model.PutID]model.BlobID
	checkpoints      map[model.CheckpointID]checkpointData
	checkpointWrites map[model.CheckpointWriteID]model.CheckpointID
}

type checkpointData struct {
	req model.PutCheckpointRequest
	ref model.CheckpointRef
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
		data = newWarehouseSpace()
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

func newWarehouseSpace() *warehouseSpace {
	return &warehouseSpace{
		blobs:            make(map[model.BlobID]model.Blob),
		puts:             make(map[model.PutID]model.BlobID),
		checkpoints:      make(map[model.CheckpointID]checkpointData),
		checkpointWrites: make(map[model.CheckpointWriteID]model.CheckpointID),
	}
}

func cloneCheckpointRef(ref model.CheckpointRef) model.CheckpointRef { return ref }

func cloneCheckpointRequest(req model.PutCheckpointRequest) model.PutCheckpointRequest {
	req.Value = bytes.Clone(req.Value)
	return req
}

func sameCheckpointRequest(a, b model.PutCheckpointRequest) bool {
	return a.WriteID == b.WriteID &&
		a.SequenceID == b.SequenceID &&
		a.SourceLedgerID == b.SourceLedgerID &&
		a.StreamGeneration == b.StreamGeneration &&
		a.CoveredThrough == b.CoveredThrough &&
		a.ContentType == b.ContentType &&
		a.Codec == b.Codec &&
		a.CodecVersion == b.CodecVersion &&
		a.SchemaVersion == b.SchemaVersion &&
		a.Checksum == b.Checksum &&
		a.StateChecksum == b.StateChecksum &&
		bytes.Equal(a.Value, b.Value)
}

func validCheckpointRequest(req model.PutCheckpointRequest) bool {
	return req.WriteID != "" && req.SequenceID != "" && req.SourceLedgerID != "" &&
		req.StreamGeneration >= 0 && req.CoveredThrough >= 0 &&
		req.Checksum != "" && req.StateChecksum != ""
}

// WarehousePutCheckpoint atomically makes checkpoint bytes and their replay
// boundary visible. RAM uses one lock for the same atomicity guarantee offered
// by a transactional backend.
func (s *RamStore) WarehousePutCheckpoint(ctx context.Context, space model.TenancySpace, req model.PutCheckpointRequest) (model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return model.CheckpointRef{}, err
	}
	if !validCheckpointRequest(req) {
		return model.CheckpointRef{}, model.ErrCheckpointInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return model.CheckpointRef{}, err
	}
	if s.warehouse == nil {
		s.warehouse = make(map[model.TenancySpace]*warehouseSpace)
	}
	data := s.warehouse[space]
	if data == nil {
		data = newWarehouseSpace()
		s.warehouse[space] = data
	}
	if checkpointID, ok := data.checkpointWrites[req.WriteID]; ok {
		existing := data.checkpoints[checkpointID]
		if !sameCheckpointRequest(existing.req, req) {
			return model.CheckpointRef{}, model.ErrCheckpointConflict
		}
		return cloneCheckpointRef(existing.ref), nil
	}

	// A source boundary must deterministically identify one logical state. New
	// codec representations are fine; a different state checksum is not.
	for _, existing := range data.checkpoints {
		if existing.ref.SequenceID == req.SequenceID &&
			(existing.ref.SourceLedgerID != req.SourceLedgerID || existing.ref.StreamGeneration != req.StreamGeneration) {
			return model.CheckpointRef{}, model.ErrCheckpointConflict
		}
		if existing.ref.SequenceID == req.SequenceID &&
			existing.ref.SourceLedgerID == req.SourceLedgerID &&
			existing.ref.StreamGeneration == req.StreamGeneration &&
			existing.ref.CoveredThrough == req.CoveredThrough &&
			existing.ref.StateChecksum != req.StateChecksum {
			return model.CheckpointRef{}, model.ErrCheckpointConflict
		}
	}

	blobID := model.BlobID(rand.Text())
	for {
		if _, exists := data.blobs[blobID]; !exists {
			break
		}
		blobID = model.BlobID(rand.Text())
	}
	checkpointID := model.CheckpointID(rand.Text())
	for {
		if _, exists := data.checkpoints[checkpointID]; !exists {
			break
		}
		checkpointID = model.CheckpointID(rand.Text())
	}
	now := time.Now().UTC()
	data.blobs[blobID] = model.Blob{
		ID: blobID, PutID: model.PutID("checkpoint:" + string(req.WriteID)),
		ContentType: req.ContentType, Value: bytes.Clone(req.Value), Checksum: req.Checksum, CreatedAt: now,
	}
	ref := model.CheckpointRef{
		ID: checkpointID, SequenceID: req.SequenceID, SourceLedgerID: req.SourceLedgerID,
		StreamGeneration: req.StreamGeneration, CoveredThrough: req.CoveredThrough, BlobID: blobID,
		ContentType: req.ContentType, Codec: req.Codec, CodecVersion: req.CodecVersion,
		SchemaVersion: req.SchemaVersion, Checksum: req.Checksum, StateChecksum: req.StateChecksum,
		Status: model.CheckpointReady, CreatedAt: now,
	}
	data.checkpoints[checkpointID] = checkpointData{req: cloneCheckpointRequest(req), ref: ref}
	data.checkpointWrites[req.WriteID] = checkpointID
	return cloneCheckpointRef(ref), nil
}

func (s *RamStore) WarehouseGetLatestCheckpoint(ctx context.Context, space model.TenancySpace, sequenceID model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := s.warehouse[space]
	if data == nil {
		return nil, nil
	}
	var latest *model.CheckpointRef
	for _, checkpoint := range data.checkpoints {
		ref := checkpoint.ref
		if ref.SequenceID != sequenceID || ref.Status != model.CheckpointReady {
			continue
		}
		if latest == nil || ref.CoveredThrough > latest.CoveredThrough ||
			(ref.CoveredThrough == latest.CoveredThrough && (ref.CreatedAt.After(latest.CreatedAt) ||
				(ref.CreatedAt.Equal(latest.CreatedAt) && ref.ID > latest.ID))) {
			copy := cloneCheckpointRef(ref)
			latest = &copy
		}
	}
	return latest, nil
}

func (s *RamStore) WarehouseFindCheckpoints(ctx context.Context, space model.TenancySpace, sequenceID model.CheckpointSequenceID, atOrBefore model.LedgerPosition, limit int) ([]model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if atOrBefore < 0 || limit < 0 {
		return nil, model.ErrCheckpointInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := s.warehouse[space]
	if data == nil {
		return []model.CheckpointRef{}, nil
	}
	refs := make([]model.CheckpointRef, 0)
	for _, checkpoint := range data.checkpoints {
		ref := checkpoint.ref
		if ref.SequenceID == sequenceID && ref.Status == model.CheckpointReady && ref.CoveredThrough <= atOrBefore {
			refs = append(refs, cloneCheckpointRef(ref))
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].CoveredThrough != refs[j].CoveredThrough {
			return refs[i].CoveredThrough > refs[j].CoveredThrough
		}
		if !refs[i].CreatedAt.Equal(refs[j].CreatedAt) {
			return refs[i].CreatedAt.After(refs[j].CreatedAt)
		}
		return refs[i].ID > refs[j].ID
	})
	if limit > 0 && len(refs) > limit {
		refs = refs[:limit]
	}
	return refs, nil
}

func (s *RamStore) WarehouseGetCheckpoint(ctx context.Context, space model.TenancySpace, checkpointID model.CheckpointID) (model.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return model.Checkpoint{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return model.Checkpoint{}, err
	}
	data := s.warehouse[space]
	if data == nil {
		return model.Checkpoint{}, model.ErrCheckpointNotFound
	}
	checkpoint, ok := data.checkpoints[checkpointID]
	if !ok {
		return model.Checkpoint{}, model.ErrCheckpointNotFound
	}
	if checkpoint.ref.Status == model.CheckpointCorrupt {
		return model.Checkpoint{}, model.ErrCheckpointCorrupt
	}
	blob, ok := data.blobs[checkpoint.ref.BlobID]
	if !ok {
		return model.Checkpoint{}, model.ErrCheckpointNotFound
	}
	return model.Checkpoint{Ref: cloneCheckpointRef(checkpoint.ref), Value: bytes.Clone(blob.Value)}, nil
}

func (s *RamStore) WarehouseMarkCheckpointCorrupt(ctx context.Context, space model.TenancySpace, checkpointID model.CheckpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	data := s.warehouse[space]
	if data == nil {
		return model.ErrCheckpointNotFound
	}
	checkpoint, ok := data.checkpoints[checkpointID]
	if !ok {
		return model.ErrCheckpointNotFound
	}
	checkpoint.ref.Status = model.CheckpointCorrupt
	data.checkpoints[checkpointID] = checkpoint
	return nil
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
