package protocol

import (
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
