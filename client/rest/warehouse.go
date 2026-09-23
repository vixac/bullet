package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
)

func (c *Client) WarehousePut(ctx context.Context, req model.PutBlobRequest) (model.Blob, error) {
	var r protocol.Blob
	err := c.do(ctx, http.MethodPost, "/warehouse/blobs", protocol.PutBlobRequestFromModel(req), &r, http.StatusOK)
	if err != nil {
		return model.Blob{}, err
	}
	return r.Model(), nil
}
func (c *Client) WarehouseGet(ctx context.Context, id model.BlobID) (model.Blob, error) {
	if err := ctx.Err(); err != nil {
		return model.Blob{}, err
	}
	if id == "" {
		return model.Blob{}, model.ErrBlobNotFound
	}
	var r protocol.Blob
	err := c.do(ctx, http.MethodGet, "/warehouse/blobs/"+url.PathEscape(string(id)), nil, &r, http.StatusOK)
	if err != nil {
		return model.Blob{}, err
	}
	return r.Model(), nil
}
func (c *Client) WarehouseGetMany(ctx context.Context, ids []model.BlobID) (map[model.BlobID]model.Blob, error) {
	var r protocol.WarehouseGetManyResponse
	if err := c.do(ctx, http.MethodPost, "/warehouse/blobs/batch-get", protocol.WarehouseGetManyRequest{IDs: ids}, &r, http.StatusOK); err != nil {
		return nil, err
	}
	result := make(map[model.BlobID]model.Blob, len(r))
	for id, b := range r {
		result[id] = b.Model()
	}
	return result, nil
}

func (c *Client) WarehousePutCheckpoint(ctx context.Context, req model.PutCheckpointRequest) (model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return model.CheckpointRef{}, err
	}
	var response protocol.CheckpointRef
	if err := c.do(ctx, http.MethodPost, "/warehouse/checkpoints", protocol.PutCheckpointRequestFromModel(req), &response, http.StatusOK); err != nil {
		return model.CheckpointRef{}, err
	}
	ref, err := response.Model()
	if err != nil {
		return model.CheckpointRef{}, fmt.Errorf("decode checkpoint reference: %w", err)
	}
	return ref, nil
}

func checkpointSequencePath(id model.CheckpointSequenceID) string {
	return "/warehouse/checkpoint-sequences/" + checkpointPathEscape(string(id)) + "/checkpoints"
}

func checkpointPath(id model.CheckpointID) string {
	return "/warehouse/checkpoints/" + checkpointPathEscape(string(id))
}

// Gin unescapes raw path parameters with QueryUnescape, under which a literal
// plus denotes a space. PathEscape intentionally leaves plus signs untouched,
// so encode them explicitly to preserve path identifiers exactly.
func checkpointPathEscape(value string) string {
	return strings.ReplaceAll(url.PathEscape(value), "+", "%2B")
}

func (c *Client) WarehouseGetLatestCheckpoint(ctx context.Context, sequenceID model.CheckpointSequenceID) (*model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequenceID == "" {
		return nil, model.ErrCheckpointInvalid
	}
	var response protocol.LatestCheckpointResponse
	if err := c.do(ctx, http.MethodGet, checkpointSequencePath(sequenceID)+"/latest", nil, &response, http.StatusOK); err != nil {
		return nil, err
	}
	if response.Checkpoint == nil {
		return nil, nil
	}
	ref, err := response.Checkpoint.Model()
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint reference: %w", err)
	}
	return &ref, nil
}

func (c *Client) WarehouseFindCheckpoints(ctx context.Context, sequenceID model.CheckpointSequenceID, atOrBefore model.LedgerPosition, limit int) ([]model.CheckpointRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequenceID == "" || atOrBefore < 0 || limit < 0 {
		return nil, model.ErrCheckpointInvalid
	}
	query := url.Values{}
	query.Set("at_or_before", strconv.FormatInt(int64(atOrBefore), 10))
	query.Set("limit", strconv.Itoa(limit))
	var response protocol.CheckpointCandidatesResponse
	if err := c.do(ctx, http.MethodGet, checkpointSequencePath(sequenceID)+"?"+query.Encode(), nil, &response, http.StatusOK); err != nil {
		return nil, err
	}
	refs, err := response.Models()
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint candidates: %w", err)
	}
	return refs, nil
}

func (c *Client) WarehouseGetCheckpoint(ctx context.Context, checkpointID model.CheckpointID) (model.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return model.Checkpoint{}, err
	}
	if checkpointID == "" {
		return model.Checkpoint{}, model.ErrCheckpointNotFound
	}
	var response protocol.Checkpoint
	if err := c.do(ctx, http.MethodGet, checkpointPath(checkpointID), nil, &response, http.StatusOK); err != nil {
		return model.Checkpoint{}, err
	}
	checkpoint, err := response.Model()
	if err != nil {
		return model.Checkpoint{}, fmt.Errorf("decode checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (c *Client) WarehouseMarkCheckpointCorrupt(ctx context.Context, checkpointID model.CheckpointID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if checkpointID == "" {
		return model.ErrCheckpointNotFound
	}
	return c.do(ctx, http.MethodPost, checkpointPath(checkpointID)+"/corrupt", nil, nil, http.StatusNoContent)
}
