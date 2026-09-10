// Package client defines Bullet's tenant-scoped, transport-independent client
// interfaces. Each interface mirrors its store/store_interface counterpart,
// with exactly the TenancySpace argument removed. Domain arguments, results,
// method names, and error identities are shared through package model.
//
// Implementations bind a model.TenancySpace once during construction. A local
// adapter passes that space to every store call; a REST adapter sends the same
// space in protocol.AppIDHeader and protocol.TenancyIDHeader on every request.
// The configured space should remain immutable for the lifetime of the client.
// TrackMutate follows the same rule: all its puts and deletes use that space.
//
// This package defines contracts only; it does not construct adapters, own
// store connections, or close resources. Applications own their store and HTTP
// client lifecycles. Scope selection is not an authorization mechanism.
package client
