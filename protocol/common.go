package protocol

const (
	AppIDHeader     = "X-App-Id"
	TenancyIDHeader = "X-Tenancy-Id"
)

// ErrorResponse carries a readable message and an optional stable domain code.
type ErrorResponse struct {
	Code  string `json:"code,omitempty"`
	Error string `json:"error"`
}
