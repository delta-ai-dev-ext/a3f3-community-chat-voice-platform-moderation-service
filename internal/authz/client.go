package authz

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"

	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// clientTimeout is the per-attempt API-001 budget from the inter-service
// contract (2 s, at most one retry on 5xx/timeout).
const clientTimeout = 2 * time.Second

// maxResponseBytes bounds how much of an API-001 response is read.
const maxResponseBytes = 1 << 20

// Error classes reported in external_call_errors_total. Kept as a closed set so
// the label stays low-cardinality.
const (
	errorClassTimeout     = "timeout"
	errorClassTransport   = "transport"
	errorClassServerError = "server_error"
	errorClassDecode      = "decode"
	errorClassClientError = "client_error"
)

// CheckRequest is the API-001 request body.
type CheckRequest struct {
	// ActorUserID is the user performing the moderator action.
	ActorUserID string `json:"actor_user_id"`
	// Permission is the community permission the action requires.
	Permission models.Permission `json:"permission"`
	// TargetUserID is the member the action applies to; nil for non-targeted
	// actions such as automod configuration or audit-log reads.
	TargetUserID *string `json:"target_user_id"`
}

// TargetInfo is the API-001 view of the action's target member.
type TargetInfo struct {
	UserID           string     `json:"user_id"`
	IsMember         bool       `json:"is_member"`
	IsBanned         bool       `json:"is_banned"`
	IsOwner          bool       `json:"is_owner"`
	OutrankedByActor bool       `json:"outranked_by_actor"`
	MutedUntil       *time.Time `json:"muted_until"`
}

// Decision is the normalized API-001 answer. A denial is not a transport
// failure: it is a definitive "not allowed" with a machine-readable reason.
type Decision struct {
	// Allowed reports whether the actor may perform the action right now.
	Allowed bool
	// DenialReason explains a denial and maps to a 403 error code.
	DenialReason string
	// ActorIsMember reports whether the actor is an active community member.
	ActorIsMember bool
	// ActorIsOwner reports whether the actor owns the community.
	ActorIsOwner bool
	// ActorPermissions is the actor's effective permission set.
	ActorPermissions []models.Permission
	// Target is the target member view, nil for non-targeted actions.
	Target *TargetInfo
}

// checkResponse is the API-001 success body.
type checkResponse struct {
	CommunityID      string              `json:"community_id"`
	ActorUserID      string              `json:"actor_user_id"`
	Permission       models.Permission   `json:"permission"`
	Allowed          bool                `json:"allowed"`
	DenialReason     *string             `json:"denial_reason"`
	ActorIsMember    bool                `json:"actor_is_member"`
	ActorIsOwner     bool                `json:"actor_is_owner"`
	ActorPermissions []models.Permission `json:"actor_permissions"`
	Target           *TargetInfo         `json:"target"`
}

// Checker performs the real-time authorization check that gates every moderator
// action. Implementations must fail closed: if the answer is unknown, the error
// must prevent the action rather than allow it.
type Checker interface {
	Check(ctx context.Context, communityID string, request CheckRequest) (Decision, error)
}

// Client is the HTTP implementation of Checker. It has no cache and no
// fallback: a revoked moderator can never act on a stale decision (ADR-001).
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	metrics    *observability.Metrics
	logger     *zap.Logger
	// RequestTimeout bounds one attempt; zero means the contract's 2 s.
	RequestTimeout time.Duration
}

var _ Checker = (*Client)(nil)

// NewClient builds the API-001 client. httpClient may be nil, in which case a
// client with the contract timeout is used.
func NewClient(baseURL, token string, metrics *observability.Metrics, logger *zap.Logger, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: clientTimeout}
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		token:      token,
		httpClient: httpClient,
		metrics:    metrics,
		logger:     logger,
	}
}

// Check asks community_service whether the actor may perform the permission in
// the community. A transport failure, timeout, or 5xx after one retry yields
// ErrAuthorizationUnavailable so the caller fails closed with 503.
func (c *Client) Check(ctx context.Context, communityID string, request CheckRequest) (Decision, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return Decision{}, fmt.Errorf("encode authorization check: %w", err)
	}
	endpoint := c.baseURL + "/internal/communities/" + url.PathEscape(communityID) + "/authorization-checks"

	const maxAttempts = 2
	var (
		lastErr   error
		lastClass string
	)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		decision, errorClass, retryable, attemptErr := c.attempt(ctx, endpoint, body)
		if attemptErr == nil {
			return decision, nil
		}
		lastErr, lastClass = attemptErr, errorClass
		if !retryable || attempt == maxAttempts || ctx.Err() != nil {
			break
		}
		c.logger.Warn("retrying authorization check",
			zap.String("trace_id", observability.TraceID(ctx)),
			zap.String("operation", observability.OperationAuthorizationChecks),
			zap.Int("attempt", attempt),
		)
	}
	c.metrics.RecordExternalCallError(lastClass)
	if errors.Is(lastErr, models.ErrCommunityNotFound) {
		return Decision{}, lastErr
	}
	return Decision{}, models.ErrAuthorizationUnavailable
}

// attempt performs one HTTP call and classifies the outcome. It reports the
// low-cardinality error class for metrics and whether a retry is safe.
func (c *Client) attempt(ctx context.Context, endpoint string, body []byte) (Decision, string, bool, error) {
	timeout := c.RequestTimeout
	if timeout <= 0 {
		timeout = clientTimeout
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	httpRequest, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return Decision{}, errorClassClientError, false, fmt.Errorf("build authorization request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+c.token)
	propagateTrace(attemptCtx, httpRequest.Header)

	started := time.Now()
	response, err := c.httpClient.Do(httpRequest)
	c.metrics.ObserveExternalCall(time.Since(started).Seconds())
	if err != nil {
		errorClass := errorClassTransport
		var netErr net.Error
		if (errors.As(err, &netErr) && netErr.Timeout()) || errors.Is(err, context.DeadlineExceeded) {
			errorClass = errorClassTimeout
		}
		return Decision{}, errorClass, true, fmt.Errorf("authorization check: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))
		_ = response.Body.Close()
	}()

	switch {
	case response.StatusCode == http.StatusOK:
		return decodeDecision(response.Body)
	case response.StatusCode == http.StatusNotFound:
		return Decision{}, errorClassClientError, false, models.ErrCommunityNotFound
	case response.StatusCode >= 500:
		return Decision{}, errorClassServerError, true, fmt.Errorf("authorization check returned status %d", response.StatusCode)
	default:
		return Decision{}, errorClassClientError, false, fmt.Errorf("authorization check returned status %d", response.StatusCode)
	}
}

// decodeDecision turns an API-001 success body into a Decision. A malformed
// body is a provider malfunction, so the action fails closed.
func decodeDecision(body io.Reader) (Decision, string, bool, error) {
	var payload checkResponse
	decoder := json.NewDecoder(io.LimitReader(body, maxResponseBytes))
	if err := decoder.Decode(&payload); err != nil {
		return Decision{}, errorClassDecode, false, fmt.Errorf("decode authorization response: %w", err)
	}
	decision := Decision{
		Allowed:          payload.Allowed,
		ActorIsMember:    payload.ActorIsMember,
		ActorIsOwner:     payload.ActorIsOwner,
		ActorPermissions: payload.ActorPermissions,
		Target:           payload.Target,
	}
	if payload.DenialReason != nil {
		decision.DenialReason = *payload.DenialReason
	}
	return decision, "", false, nil
}

// propagateTrace copies the trace context onto the outbound API-001 request.
func propagateTrace(ctx context.Context, header http.Header) {
	traceID := observability.TraceID(ctx)
	if traceID == "" {
		return
	}
	header.Set("X-Trace-Id", traceID)
	header.Set("traceparent", observability.FormatTraceparent(traceID, ""))
}
