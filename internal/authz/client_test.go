package authz_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/authz"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// checkEndpoint is the API-001 path used by every test.
const checkEndpoint = "http://community.test/internal/communities/" + communityID + "/authorization-checks"

// communityID and actorID are fixed ids so assertions can name them.
const (
	communityID = "11111111-1111-4111-8111-111111111111"
	actorID     = "22222222-2222-4222-8222-222222222222"
	targetID    = "33333333-3333-4333-8333-333333333333"
)

// newTestClient wires a Client to an isolated mock transport so tests can run in
// parallel without touching the global http.DefaultTransport.
func newTestClient(t *testing.T) (*authz.Client, *httpmock.MockTransport, *observability.Metrics) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	require.NoError(t, err)

	transport := httpmock.NewMockTransport()
	client := authz.NewClient("http://community.test", "service-token", metrics, nil, &http.Client{Transport: transport})
	client.RequestTimeout = 50 * time.Millisecond
	return client, transport, metrics
}

// allowedBody is a well-formed API-001 allow response.
func allowedBody() map[string]any {
	return map[string]any{
		"community_id":      communityID,
		"actor_user_id":     actorID,
		"permission":        "ban_members",
		"allowed":           true,
		"denial_reason":     nil,
		"actor_is_member":   true,
		"actor_is_owner":    false,
		"actor_permissions": []string{"ban_members"},
		"target": map[string]any{
			"user_id":            targetID,
			"is_member":          true,
			"is_banned":          false,
			"is_owner":           false,
			"outranked_by_actor": true,
			"muted_until":        nil,
		},
	}
}

// TestCheckFailsClosed pins the fail-closed contract of ADR-001: any transport
// failure, timeout, or 5xx results in authorization_unavailable after exactly
// one retry, never in an allow.
func TestCheckFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		responder    httpmock.Responder
		expectCalls  int
		expectErr    error
		expectClass  string
		expectDenied bool
	}{
		{
			name:        "server error on both attempts yields authorization_unavailable",
			responder:   httpmock.NewStringResponder(http.StatusServiceUnavailable, `{"error":"down"}`),
			expectCalls: 2,
			expectErr:   models.ErrAuthorizationUnavailable,
			expectClass: "server_error",
		},
		{
			name:        "internal server error on both attempts yields authorization_unavailable",
			responder:   httpmock.NewStringResponder(http.StatusInternalServerError, `{"error":"boom"}`),
			expectCalls: 2,
			expectErr:   models.ErrAuthorizationUnavailable,
			expectClass: "server_error",
		},
		{
			name: "timeouts on both attempts yield authorization_unavailable",
			responder: func(*http.Request) (*http.Response, error) {
				time.Sleep(200 * time.Millisecond)
				return httpmock.NewStringResponse(http.StatusOK, "{}"), nil
			},
			expectCalls: 2,
			expectErr:   models.ErrAuthorizationUnavailable,
			expectClass: "timeout",
		},
		{
			name:        "unknown community is a 404 not an authorization outage",
			responder:   httpmock.NewStringResponder(http.StatusNotFound, `{"error":"community_not_found"}`),
			expectCalls: 1,
			expectErr:   models.ErrCommunityNotFound,
			expectClass: "client_error",
		},
		{
			name:        "unexpected 400 is not retried and still fails closed",
			responder:   httpmock.NewStringResponder(http.StatusBadRequest, `{"error":"invalid"}`),
			expectCalls: 1,
			expectErr:   models.ErrAuthorizationUnavailable,
			expectClass: "client_error",
		},
		{
			name:        "undecodable success body fails closed",
			responder:   httpmock.NewStringResponder(http.StatusOK, `not-json`),
			expectCalls: 1,
			expectErr:   models.ErrAuthorizationUnavailable,
			expectClass: "decode",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client, transport, metrics := newTestClient(t)
			transport.RegisterResponder(http.MethodPost, checkEndpoint, test.responder)

			decision, err := client.Check(context.Background(), communityID, authz.CheckRequest{
				ActorUserID:  actorID,
				Permission:   models.PermissionBanMembers,
				TargetUserID: ptr(targetID),
			})

			require.Error(t, err)
			assert.True(t, errors.Is(err, test.expectErr), "expected %v, got %v", test.expectErr, err)
			assert.False(t, decision.Allowed, "a failed check must never allow the action")
			assert.Equal(t, test.expectCalls, transport.GetCallCountInfo()["POST "+checkEndpoint])
			assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ExternalCallErrors.WithLabelValues(
				observability.ProviderCommunityService, observability.OperationAuthorizationChecks, test.expectClass,
			)))
		})
	}
}

// TestCheckRetriesOnceThenSucceeds proves the single retry on a 5xx is honoured
// and that the retried check's answer is what the service acts on.
func TestCheckRetriesOnceThenSucceeds(t *testing.T) {
	t.Parallel()

	client, transport, _ := newTestClient(t)
	calls := 0
	transport.RegisterResponder(http.MethodPost, checkEndpoint, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return httpmock.NewStringResponse(http.StatusBadGateway, `{"error":"upstream"}`), nil
		}
		return httpmock.NewJsonResponse(http.StatusOK, allowedBody())
	})

	decision, err := client.Check(context.Background(), communityID, authz.CheckRequest{
		ActorUserID:  actorID,
		Permission:   models.PermissionBanMembers,
		TargetUserID: ptr(targetID),
	})

	require.NoError(t, err)
	assert.True(t, decision.Allowed)
	assert.Equal(t, 2, calls)
	require.NotNil(t, decision.Target)
	assert.True(t, decision.Target.OutrankedByActor)
}

// TestCheckMapsDenial verifies a denial is a definitive answer with a reason,
// not a transport error, so the handler can return the derived 403.
func TestCheckMapsDenial(t *testing.T) {
	t.Parallel()

	body := allowedBody()
	body["allowed"] = false
	body["denial_reason"] = "target_outranks_actor"
	body["target"] = map[string]any{
		"user_id":            targetID,
		"is_member":          true,
		"is_banned":          false,
		"is_owner":           false,
		"outranked_by_actor": false,
		"muted_until":        nil,
	}

	client, transport, _ := newTestClient(t)
	transport.RegisterResponder(http.MethodPost, checkEndpoint, httpmock.NewJsonResponderOrPanic(http.StatusOK, body))

	decision, err := client.Check(context.Background(), communityID, authz.CheckRequest{
		ActorUserID:  actorID,
		Permission:   models.PermissionBanMembers,
		TargetUserID: ptr(targetID),
	})

	require.NoError(t, err)
	assert.False(t, decision.Allowed)
	assert.Equal(t, "target_outranks_actor", decision.DenialReason)
	assert.Equal(t, models.ErrTargetOutranksActor, models.DenialError(decision.DenialReason))
}

// TestCheckPropagatesTraceAndIdentity asserts the outbound call carries the
// service token, both trace headers, and the documented request body.
func TestCheckPropagatesTraceAndIdentity(t *testing.T) {
	t.Parallel()

	client, transport, _ := newTestClient(t)
	var captured *http.Request
	var capturedBody authz.CheckRequest
	transport.RegisterResponder(http.MethodPost, checkEndpoint, func(req *http.Request) (*http.Response, error) {
		captured = req
		raw, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &capturedBody))
		return httpmock.NewJsonResponse(http.StatusOK, allowedBody())
	})

	ctx := observability.WithTraceID(context.Background(), "4bf92f3577b34da6a3ce929d0e0e4736")
	_, err := client.Check(ctx, communityID, authz.CheckRequest{
		ActorUserID:  actorID,
		Permission:   models.PermissionBanMembers,
		TargetUserID: ptr(targetID),
	})
	require.NoError(t, err)

	require.NotNil(t, captured)
	assert.Equal(t, "Bearer service-token", captured.Header.Get("Authorization"))
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", captured.Header.Get("X-Trace-Id"))
	assert.Contains(t, captured.Header.Get("traceparent"), "4bf92f3577b34da6a3ce929d0e0e4736")
	assert.Equal(t, actorID, capturedBody.ActorUserID)
	assert.Equal(t, models.PermissionBanMembers, capturedBody.Permission)
	require.NotNil(t, capturedBody.TargetUserID)
	assert.Equal(t, targetID, *capturedBody.TargetUserID)
}

// ptr returns a pointer to value.
func ptr(value string) *string { return &value }
