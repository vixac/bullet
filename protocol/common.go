package protocol

const (
	AppIDHeader     = "X-App-Id"
	TenancyIDHeader = "X-Tenancy-Id"
)

// ErrorResponse preserves the existing HTTP error body. Stable error codes
// and transport-independent error reconstruction are a separate API change.
type ErrorResponse struct {
	Error string `json:"error"`
}
