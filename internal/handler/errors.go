// Package handler contains the thin gin layer: it binds requests, delegates to
// the domain services, and maps domain errors to HTTP exactly once.
package handler

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// maxIdempotencyKeyLength is the documented bound of the header value.
const maxIdempotencyKeyLength = 128

// errNotReady is returned by /readyz when a dependency or the shutdown state
// makes the replica unable to serve traffic.
var errNotReady = &models.Error{Status: http.StatusServiceUnavailable, Code: "not_ready", Detail: "The service is not ready to serve traffic."}

// errNotFound is returned for unknown routes so no endpoint answers with an
// empty body.
var errNotFound = &models.Error{Status: http.StatusNotFound, Code: "not_found", Detail: "The requested resource does not exist."}

// errMethodNotAllowed is returned for a known path with an unsupported method.
var errMethodNotAllowed = &models.Error{Status: http.StatusMethodNotAllowed, Code: "method_not_allowed", Detail: "The method is not supported for this resource."}

// respondError writes the platform error shape from any domain error. This is
// the single place an error becomes an HTTP status.
func respondError(c *gin.Context, err error) {
	domainErr := models.AsError(err)
	if domainErr == nil {
		domainErr = models.ErrInternal
	}
	if domainErr.Status >= http.StatusInternalServerError {
		observability.LoggerWithTrace(c.Request.Context(), loggerFrom(c)).Error("request failed",
			zap.String("operation", c.FullPath()),
			zap.String("status", http.StatusText(domainErr.Status)),
			zap.String("error", domainErr.Code),
			zap.Error(err),
		)
	}
	c.AbortWithStatusJSON(domainErr.Status, models.ErrorResponse{Error: domainErr.Code, Detail: domainErr.Detail})
}

// bindJSON decodes the request body. A body is optional for endpoints whose
// requestBody is not required (the lift endpoint).
func bindJSON(c *gin.Context, out any, optional bool) error {
	if err := c.ShouldBindJSON(out); err != nil {
		if optional && errors.Is(err, io.EOF) {
			return nil
		}
		return models.ErrInvalidRequest.WithDetail("The request body is not valid.")
	}
	return nil
}

// idempotencyKey returns the validated Idempotency-Key header, or "" when the
// client did not send one.
func idempotencyKey(c *gin.Context) (string, error) {
	key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if len(key) > maxIdempotencyKeyLength {
		return "", models.ErrInvalidRequest.WithDetailf("Idempotency-Key must be at most %d characters.", maxIdempotencyKeyLength)
	}
	return key, nil
}

// pageParam resolves the cursor pagination query parameters.
func pageParam(c *gin.Context) (models.Pagination, error) {
	return models.NewPagination(c.Query("limit"), c.Query("cursor"))
}

// optionalQuery returns a pointer to a non-empty query parameter, or nil.
func optionalQuery(c *gin.Context, name string) *string {
	value := strings.TrimSpace(c.Query(name))
	if value == "" {
		return nil
	}
	return &value
}

// loggerFrom returns the request-scoped logger installed by the middleware.
func loggerFrom(c *gin.Context) *zap.Logger {
	if value, exists := c.Get(loggerContextKey); exists {
		if logger, ok := value.(*zap.Logger); ok {
			return logger
		}
	}
	return zap.NewNop()
}
