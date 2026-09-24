package model

import (
	"errors"
)

type TrackKey struct {
	BucketID int32
	Key      string
}

type TrackPut struct {
	BucketID int32
	Key      string
	Value    TrackValue
	// IfAbsent creates this key only when it does not already exist. Within a
	// TrackMutation, a conflict prevents the entire mutation from committing.
	// False preserves the normal upsert behavior.
	IfAbsent bool
}

type TrackMutation struct {
	MutationID MutationID
	Puts       []TrackPut
	Deletes    []TrackKey
}

type TrackMutationResult struct {
	Applied bool
}

var (
	ErrTrackMutationUnsupported = errors.New("track mutations are not supported by this store")
	ErrTrackKeyAlreadyExists    = errors.New("track key already exists")
	ErrTrackPayloadTooLarge     = errors.New("track payload is too large")
)

const TrackMaxPayloadBytes = 64 * 1024

func ValidateTrackValue(value TrackValue) error {
	if len(value.Payload) > TrackMaxPayloadBytes {
		return ErrTrackPayloadTooLarge
	}
	return nil
}

// TrackReadOptions controls optional data returned by point and explicit-key
// reads. Prefix queries intentionally never return payloads.
type TrackReadOptions struct {
	IncludePayload bool
}

// TrackKeyValueItem associates a key with a stored-value projection. Point and
// explicit-key reads may include Payload; prefix reads intentionally omit it.
type TrackKeyValueItem struct {
	Key   string
	Value TrackValue
}

// TrackValue is the value and optional metadata stored for a key.
type TrackValue struct {
	Value  int64
	Tag    *int64
	Metric *float64
	// Payload is nil when no payload is stored. A non-nil empty slice represents
	// a stored zero-byte payload.
	Payload []byte
}
