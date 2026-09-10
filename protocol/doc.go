// Package protocol defines Bullet's HTTP request and response representations.
// It can be imported by clients without importing Gin or any store driver.
//
// Tenancy is carried by AppIDHeader and TenancyIDHeader, never by individual
// mutation items. Route parameters and query parameters remain part of the
// endpoint contract and are not automatically encoded by marshaling a body.
//
// These types preserve the existing wire format, including Ledger's decimal
// string positions and Track's uppercase Value, Tag, and Metric fields inside
// nested values. Use package model for transport-independent operations.
// Conversion helpers do not transfer ownership of pointer or slice contents;
// callers must not mutate inputs while an operation is in progress.
package protocol
