package blitzdb

import (
	"fmt"
	"strings"
)

// Typed SDK error kinds (see PROTOCOL.md "Errors").
const (
	ErrTransport = "Transport"
	ErrTimeout   = "Timeout"
	ErrAuth      = "Auth"
	ErrRetryable = "Retryable"
	ErrNotFound  = "NotFound"
	ErrInvalid   = "Invalid"
	ErrServer    = "Server"
	ErrClosed    = "Closed"
)

// SdkError is a typed client error. Mapping is by documented prefix;
// unknown strings surface as Server verbatim.
type SdkError struct {
	Kind   string
	Detail string
}

func (e *SdkError) Error() string { return fmt.Sprintf("%s: %s", strings.ToLower(e.Kind), e.Detail) }

func newErr(kind, msg string) *SdkError { return &SdkError{Kind: kind, Detail: msg} }

// MapServerError maps a server error payload to a typed error.
func MapServerError(msg string) *SdkError {
	switch {
	case strings.HasPrefix(msg, "unauthorized") || strings.HasPrefix(msg, "forbidden"):
		return newErr(ErrAuth, msg)
	case strings.HasPrefix(msg, "WAL backpressure") ||
		strings.HasPrefix(msg, "group full") ||
		strings.HasPrefix(msg, "rotation in progress"):
		return newErr(ErrRetryable, msg)
	case strings.Contains(msg, "not found") || msg == "not found":
		return newErr(ErrNotFound, msg)
	case strings.Contains(msg, "requires values") ||
		strings.Contains(msg, "requires row_id") ||
		strings.Contains(msg, "too large") ||
		strings.Contains(msg, "DuplicateKey") ||
		strings.Contains(msg, "duplicate value") ||
		strings.Contains(msg, "not supported") ||
		strings.Contains(msg, "not unique") ||
		strings.Contains(msg, "validation") ||
		strings.Contains(msg, "schema error") ||
		strings.Contains(msg, "type error"):
		return newErr(ErrInvalid, msg)
	default:
		return newErr(ErrServer, msg)
	}
}
