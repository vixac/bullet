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
	Value    int64
	Tag      *int64
	Metric   *float64
}

type TrackMutation struct {
	MutationID MutationID
	Puts       []TrackPut
	Deletes    []TrackKey
}

type TrackMutationResult struct {
	Applied bool
}

var ErrTrackMutationUnsupported = errors.New("track mutations are not supported by this store")

// TrackKeyValueItem associates a key with its complete stored value.
type TrackKeyValueItem struct {
	Key   string
	Value TrackValue
}

// TrackValue is the value and optional metadata stored for a key.
type TrackValue struct {
	Value  int64
	Tag    *int64
	Metric *float64
}
