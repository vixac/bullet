package model

import (
	"errors"
	"time"
)

const (
	LedgerMaxPayloadBytes = 256 * 1024
	LedgerMaxPageSize     = 1000
	LedgerMaxSelected     = 100
)

type LedgerID string
type LedgerAppendID string
type LedgerPosition int64

type LedgerAppendItem struct {
	AppendID LedgerAppendID
	Payload  string
}

type LedgerRecord struct {
	LedgerID  LedgerID
	Position  LedgerPosition
	AppendID  LedgerAppendID
	CreatedAt time.Time
	Payload   string
}

// LedgerSelector selects all ledgers, explicit IDs, or IDs with a literal,
// case-sensitive prefix within a tenancy space. Exactly one of All, nonempty
// LedgerIDs, and nonempty Prefix must be supplied. Prefix follows ledger ID syntax.
type LedgerSelector struct {
	All       bool
	LedgerIDs []LedgerID
	Prefix    string
}

type LedgerPage struct {
	Records    []LedgerRecord
	NextCursor *string
}

var (
	ErrLedgerUnsupported     = errors.New("ledger is not supported by this store")
	ErrLedgerInvalidID       = errors.New("invalid ledger id")
	ErrLedgerInvalidAppendID = errors.New("invalid ledger append id")
	ErrLedgerPayloadTooLarge = errors.New("ledger payload is too large")
	ErrLedgerAppendConflict  = errors.New("ledger append id already exists with a different payload")
	ErrLedgerBatchConflict   = errors.New("ledger batch mixes existing and new append ids")
	ErrLedgerInvalidSelector = errors.New("invalid ledger selector")
	ErrLedgerInvalidPageSize = errors.New("invalid ledger page size")
	ErrLedgerInvalidCursor   = errors.New("invalid ledger cursor")
)
