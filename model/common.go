package model

// TenancySpace identifies the application and tenancy for a store operation.
type TenancySpace struct {
	AppId     int32
	TenancyId int64
}

// MutationID identifies an idempotent mutation. Its scope is defined by the operation.
type MutationID string
