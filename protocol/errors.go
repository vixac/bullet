package protocol

import (
	"errors"

	"github.com/vixac/bullet/model"
)

// ErrorResponseFrom encodes known domain error identities without relying on message text.
func ErrorResponseFrom(err error) ErrorResponse {
	result := ErrorResponse{Error: err.Error()}
	for _, item := range domainErrors {
		if errors.Is(err, item.err) {
			result.Code = item.code
			break
		}
	}
	return result
}

// DomainError returns the shared sentinel for a wire code, or nil for unknown codes.
func DomainError(code string) error {
	for _, item := range domainErrors {
		if item.code == code {
			return item.err
		}
	}
	return nil
}

var domainErrors = []struct {
	code string
	err  error
}{
	{"node_not_found", model.ErrNodeNotFound},
	{"node_already_exists", model.ErrNodeAlreadyExists},
	{"cycle_detected", model.ErrCycleDetected},
	{"mutation_conflict", model.ErrMutationConflict},
	{"track_key_already_exists", model.ErrTrackKeyAlreadyExists},
	{"invalid_position", model.ErrInvalidPosition},
	{"invalid_filter", model.ErrInvalidFilter},
	{"warehouse_unsupported", model.ErrWarehouseUnsupported},
	{"warehouse_invalid_put_id", model.ErrWarehouseInvalidPutID},
	{"warehouse_put_conflict", model.ErrWarehousePutConflict},
	{"blob_not_found", model.ErrBlobNotFound},
	{"checkpoint_unsupported", model.ErrCheckpointUnsupported},
	{"checkpoint_invalid", model.ErrCheckpointInvalid},
	{"checkpoint_conflict", model.ErrCheckpointConflict},
	{"checkpoint_not_found", model.ErrCheckpointNotFound},
	{"checkpoint_corrupt", model.ErrCheckpointCorrupt},
	{"ledger_unsupported", model.ErrLedgerUnsupported},
	{"ledger_invalid_id", model.ErrLedgerInvalidID},
	{"ledger_invalid_append_id", model.ErrLedgerInvalidAppendID},
	{"ledger_payload_too_large", model.ErrLedgerPayloadTooLarge},
	{"ledger_append_conflict", model.ErrLedgerAppendConflict},
	{"ledger_batch_conflict", model.ErrLedgerBatchConflict},
	{"ledger_invalid_selector", model.ErrLedgerInvalidSelector},
	{"ledger_invalid_page_size", model.ErrLedgerInvalidPageSize},
	{"ledger_invalid_cursor", model.ErrLedgerInvalidCursor},
	{"track_mutation_unsupported", model.ErrTrackMutationUnsupported},
}
