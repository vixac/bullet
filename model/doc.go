// Package model defines Bullet's transport-independent domain values, IDs,
// options, limits, and errors. Both backend stores and tenant-scoped clients use
// these types. Domain values do not define HTTP or database serialization;
// use package protocol for HTTP request and response bodies.
package model
