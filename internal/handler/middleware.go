package handler

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"moderation_service/internal/authz"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// Context keys used by the middleware chain.
const (
	// loggerContextKey stores the request-scoped logger.
	loggerContextKey = "moderation.logger"
	// identityContextKey stores the verified caller identity.
	identityContextKey = "moderation.identity"
)

// Principal is the authenticated caller of a request.
type Principal struct {
	// UserID is the token subject.
	UserID string
	// SessionID is the identity_service session id.
	SessionID string
}

// actorID returns the authenticated caller id. It is only safe to call on
// routes behind the auth middleware.
func actorID(c *gin.Context) string {
	if value, exists := c.Get(identityContextKey); exists {
		if principal, ok := value.(Principal); ok {
			return principal.UserID
		}
	}
	return ""
}

// traceMiddleware accepts traceparent / X-Trace-Id, mints one when absent, and
// publishes the trace id on the request context and the response headers.
func traceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := observability.ResolveTraceID(c.GetHeader("traceparent"), c.GetHeader("X-Trace-Id"))
		requestID := models.NewUUID()

		ctx := observability.WithTraceID(c.Request.Context(), traceID)
		ctx = observability.WithRequestID(ctx, requestID)
		c.Request = c.Request.WithContext(ctx)

		c.Header("X-Trace-Id", traceID)
		c.Header("X-Request-Id", requestID)
		c.Next()
	}
}

// loggingMiddleware attaches the request-scoped logger and emits one line per
// request carrying the trace id and the resulting status.
func loggingMiddleware(logger *zap.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = zap.NewNop()
	}
	return func(c *gin.Context) {
		requestLogger := observability.LoggerWithTrace(c.Request.Context(), logger)
		c.Set(loggerContextKey, requestLogger)

		started := time.Now()
		c.Next()

		requestLogger.Info("http request",
			zap.String("method", c.Request.Method),
			zap.String("route", routeName(c)),
			zap.Int("status", c.Writer.Status()),
			zap.Int64("duration_ms", time.Since(started).Milliseconds()),
			zap.String("operation", routeName(c)),
		)
	}
}

// metricsMiddleware records the RED metrics of every route. The route label is
// the registered route pattern, never the raw path, so cardinality stays bounded.
func metricsMiddleware(metrics *observability.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		if metrics == nil {
			c.Next()
			return
		}
		started := time.Now()
		c.Next()

		route := routeName(c)
		method := c.Request.Method
		metrics.HTTPRequests.WithLabelValues(route, method, statusClass(c.Writer.Status())).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(route, method).Observe(time.Since(started).Seconds())
	}
}

// authMiddleware verifies the bearer access token locally (no call to
// identity_service) and stores the caller identity for the handlers.
func authMiddleware(verifier *authz.Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok || verifier == nil {
			respondError(c, models.ErrUnauthorized)
			return
		}
		identity, err := verifier.Verify(token)
		if err != nil {
			respondError(c, models.ErrUnauthorized)
			return
		}
		c.Set(identityContextKey, Principal{UserID: identity.UserID, SessionID: identity.SessionID})
		c.Next()
	}
}

// recoveryMiddleware converts a panic in a handler into a 500 in the error
// shape instead of dropping the connection.
func recoveryMiddleware(logger *zap.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = zap.NewNop()
	}
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		observability.LoggerWithTrace(c.Request.Context(), logger).Error("panic recovered",
			zap.Any("panic", recovered),
			zap.String("operation", routeName(c)),
			zap.String("status", "error"),
		)
		respondError(c, models.ErrInternal)
	})
}

// bearerToken extracts a bearer token from the Authorization header.
func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}

// routeName returns the matched route pattern, or "unmatched" for unknown paths.
func routeName(c *gin.Context) string {
	if path := c.FullPath(); path != "" {
		return path
	}
	return "unmatched"
}

// statusClass buckets a status code for the low-cardinality status_class label.
func statusClass(status int) string {
	switch {
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}

// ShutdownState tracks whether the process has begun a graceful shutdown so
// /readyz can fail immediately and the load balancer stops routing.
type ShutdownState struct {
	begun chan struct{}
}

// NewShutdownState returns a state that is open for traffic.
func NewShutdownState() *ShutdownState { return &ShutdownState{begun: make(chan struct{})} }

// Begin flips readiness to failing. It is safe to call more than once.
func (s *ShutdownState) Begin() {
	select {
	case <-s.begun:
	default:
		close(s.begun)
	}
}

// ShuttingDown reports whether shutdown has begun.
func (s *ShutdownState) ShuttingDown() bool {
	select {
	case <-s.begun:
		return true
	default:
		return false
	}
}

// Ready implements ReadinessProbe from two cheap dependency checks.
type Ready struct {
	// Database is the pgx pool (Ping verifies connectivity).
	Database interface {
		Ping(ctx context.Context) error
	}
	// Producer is the Kafka producer (Healthy verifies broker reachability).
	Producer interface {
		Healthy(ctx context.Context) bool
	}
	// Shutdown short-circuits readiness once draining starts.
	Shutdown *ShutdownState
}

// Ready reports whether the replica may serve traffic. Transient dependency
// hiccups fail readiness only; /healthz stays green so no restart is triggered.
func (r *Ready) Ready(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.Shutdown != nil && r.Shutdown.ShuttingDown() {
		return errNotReady.WithDetail("The service is shutting down.")
	}
	if r.Database != nil {
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := r.Database.Ping(checkCtx); err != nil {
			return errNotReady.WithDetail("The database is unavailable.")
		}
	}
	if r.Producer != nil && !r.Producer.Healthy(ctx) {
		return errNotReady.WithDetail("The event producer is unavailable.")
	}
	return nil
}

// statusText renders a status code for logging without importing net/http names.
func statusText(c *gin.Context) string {
	return strconv.Itoa(c.Writer.Status()) + " " + http.StatusText(c.Writer.Status())
}
