// Package observability provides the shared logger, Prometheus metric
// registry, and trace-context helpers used by every layer of the service.
package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ServiceName is stamped on every log line and on every event envelope.
const ServiceName = "moderation_service"

// NewLogger builds the structured JSON production logger. Every line carries
// the service name so logs stay attributable after aggregation.
func NewLogger(level zapcore.Level) (*zap.Logger, error) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.MessageKey = "msg"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.Lock(zapcore.AddSync(os.Stderr)),
		level,
	)
	return zap.New(core).With(zap.String("service_name", ServiceName)), nil
}

// traceKey is the private context key for the propagated trace id.
type traceKey struct{}

// requestKey is the private context key for the per-request id.
type requestKey struct{}

// WithTraceID stores the W3C trace id in the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, traceID)
}

// TraceID returns the trace id carried by ctx, or "" when absent.
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(traceKey{}).(string); ok {
		return id
	}
	return ""
}

// WithRequestID stores the per-request id in the context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	if requestID == "" {
		return ctx
	}
	return context.WithValue(ctx, requestKey{}, requestID)
}

// RequestID returns the per-request id carried by ctx, or "" when absent.
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(requestKey{}).(string); ok {
		return id
	}
	return ""
}

// MintTraceID returns a fresh 32 lowercase hex trace id. Scheduled jobs have no
// originating request, so they mint one at the edge of their flow.
func MintTraceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand never fails on supported platforms; a deterministic
		// fallback keeps trace_id non-empty rather than propagating "".
		return strings.Repeat("0", 31) + "1"
	}
	return hex.EncodeToString(buf[:])
}

// ParseTraceparent extracts the trace id from a W3C traceparent header
// ("00-<32 hex>-<16 hex>-<flags>"). Malformed values yield "".
func ParseTraceparent(header string) string {
	parts := strings.Split(strings.TrimSpace(header), "-")
	if len(parts) < 4 {
		return ""
	}
	if len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return ""
	}
	return normalizeTraceID(parts[1])
}

// ResolveTraceID picks the trace id to use for a request: the traceparent
// trace id when valid, otherwise X-Trace-Id, otherwise a freshly minted id.
func ResolveTraceID(traceparent, xTraceID string) string {
	if id := ParseTraceparent(traceparent); id != "" {
		return id
	}
	if id := normalizeTraceID(strings.TrimSpace(xTraceID)); id != "" {
		return id
	}
	return MintTraceID()
}

// normalizeTraceID lowercases a trace id and rejects anything that is not 1-32
// hex characters, so an untrusted header cannot bloat logs or event envelopes.
func normalizeTraceID(raw string) string {
	if raw == "" || len(raw) > 32 {
		return ""
	}
	lowered := strings.ToLower(raw)
	for _, r := range lowered {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return lowered
}

// FormatTraceparent renders a W3C traceparent header for an outbound call.
func FormatTraceparent(traceID, spanID string) string {
	if traceID == "" {
		return ""
	}
	if len(spanID) != 16 {
		spanID = MintTraceID()[:16]
	}
	return "00-" + traceID + "-" + spanID + "-01"
}

// LoggerWithTrace returns the logger annotated with the trace and request ids
// carried by ctx, so every line is correlatable.
func LoggerWithTrace(ctx context.Context, logger *zap.Logger) *zap.Logger {
	if logger == nil {
		return zap.NewNop()
	}
	fields := make([]zap.Field, 0, 2)
	if traceID := TraceID(ctx); traceID != "" {
		fields = append(fields, zap.String("trace_id", traceID))
	}
	if requestID := RequestID(ctx); requestID != "" {
		fields = append(fields, zap.String("request_id", requestID))
	}
	if len(fields) == 0 {
		return logger
	}
	return logger.With(fields...)
}
