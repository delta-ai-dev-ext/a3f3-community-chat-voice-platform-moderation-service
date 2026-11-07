package handler_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jarcoal/httpmock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/authz"
	"moderation_service/internal/events"
	"moderation_service/internal/handler"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/internal/service"
	"moderation_service/tests/mocks"
)

// Fixed identifiers and claims used across the HTTP tests.
const (
	communityID   = "11111111-1111-4111-8111-111111111111"
	moderatorID   = "22222222-2222-4222-8222-222222222222"
	targetID      = "33333333-3333-4333-8333-333333333333"
	sanctionID    = "44444444-4444-4444-8444-444444444444"
	appealID      = "55555555-5555-4555-8555-555555555555"
	reviewerID    = "66666666-6666-4666-8666-666666666666"
	tokenIssuer   = "identity_service"
	tokenAudience = "moderation_service"
	checkEndpoint = "http://community.test/internal/communities/" + communityID + "/authorization-checks"
)

// routerFixture is a fully wired HTTP stack over in-memory fakes: real router,
// real services, real JWT verification, and a mocked API-001 transport.
type routerFixture struct {
	engine       *gin.Engine
	store        *mocks.Store
	metrics      *observability.Metrics
	transport    *httpmock.MockTransport
	shutdown     *handler.ShutdownState
	key          *rsa.PrivateKey
	readyChecker *stubReadiness
}

// stubReadiness lets a test force the /readyz answer.
type stubReadiness struct {
	err error
}

// Ready returns the configured readiness error.
func (s *stubReadiness) Ready(context.Context) error { return s.err }

// newRouterFixture builds the HTTP stack. api001 registers the API-001 responder.
func newRouterFixture(t *testing.T, api001 httpmock.Responder) *routerFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	verifier := newVerifier(t, key)

	metricsRegistry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(metricsRegistry)
	require.NoError(t, err)

	store := mocks.NewStore()
	transport := httpmock.NewMockTransport()
	if api001 != nil {
		transport.RegisterResponder(http.MethodPost, checkEndpoint, api001)
	}

	authorizer := authz.NewClient("http://community.test", "service-token", metrics, nil, &http.Client{Transport: transport})
	deps := service.Deps{
		Store:      store,
		Authorizer: authorizer,
		Events:     events.NewBuilder(),
		Topic:      "moderation_events",
		Metrics:    metrics,
		Now:        func() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) },
	}

	shutdown := handler.NewShutdownState()
	ready := &stubReadiness{}

	engine := handler.NewRouter(handler.RouterConfig{
		Metrics:  metrics,
		Verifier: verifier,
		Registry: metricsRegistry,
		Handlers: &handler.Handlers{
			Sanctions: service.NewSanctionService(deps),
			Appeals:   service.NewAppealService(deps),
			Automod:   service.NewAutomodService(deps),
			Audit:     service.NewAuditService(deps),
			Readiness: ready,
		},
	})

	return &routerFixture{
		engine:       engine,
		store:        store,
		metrics:      metrics,
		transport:    transport,
		shutdown:     shutdown,
		key:          key,
		readyChecker: ready,
	}
}

// newVerifier builds a verifier whose configuration advertises key.
func newVerifier(t *testing.T, key *rsa.PrivateKey) *authz.Verifier {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	configuration, err := json.Marshal([]map[string]string{{"kid": "test-key", "alg": "RS256", "pem": string(pemBytes)}})
	require.NoError(t, err)
	verifier, err := authz.NewVerifier(string(configuration), tokenIssuer, tokenAudience)
	require.NoError(t, err)
	return verifier
}

// token mints an access token for the given subject.
func (f *routerFixture) token(t *testing.T, subject string, mutate func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": subject,
		"sid": "99999999-9999-4999-8999-999999999999",
		"iss": tokenIssuer,
		"aud": []string{tokenAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	signed := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed.Header["kid"] = "test-key"
	raw, err := signed.SignedString(f.key)
	require.NoError(t, err)
	return raw
}

// do performs one request as subject and returns the recorder.
func (f *routerFixture) do(t *testing.T, method, path, body, subject string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	return f.doWithClaims(t, method, path, body, func(jwt.MapClaims) {
		if subject == "" {
			return
		}
	}, subject, headers)
}

// doWithClaims performs one request with a token whose claims mutate controls
// the verification outcome.
func (f *routerFixture) doWithClaims(t *testing.T, method, path, body string, mutate func(jwt.MapClaims), subject string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if subject != "" {
		request.Header.Set("Authorization", "Bearer "+f.token(t, subject, mutate))
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	f.engine.ServeHTTP(recorder, request)
	return recorder
}

// doRaw performs one request with a literal Authorization header.
func (f *routerFixture) doRaw(t *testing.T, method, path, body, authorization string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	f.engine.ServeHTTP(recorder, request)
	return recorder
}

// stubDatabase is a readiness dependency double.
type stubDatabase struct {
	err error
}

// Ping returns the configured error.
func (s *stubDatabase) Ping(context.Context) error { return s.err }

// assertErrorBody asserts the {error,detail} shape and the status code.
func assertErrorBody(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, recorder.Code, "body: %s", recorder.Body.String())
	var payload models.ErrorResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
	assert.Equal(t, code, payload.Error)
	assert.NotEmpty(t, payload.Detail)
}

// allowResponder is a successful API-001 answer.
func allowResponder() httpmock.Responder {
	return httpmock.NewJsonResponderOrPanic(http.StatusOK, map[string]any{
		"community_id":      communityID,
		"actor_user_id":     moderatorID,
		"permission":        "ban_members",
		"allowed":           true,
		"denial_reason":     nil,
		"actor_is_member":   true,
		"actor_is_owner":    false,
		"actor_permissions": []string{"ban_members"},
		"target":            nil,
	})
}

// denyResponder is a definitive API-001 denial.
func denyResponder(reason string) httpmock.Responder {
	return httpmock.NewJsonResponderOrPanic(http.StatusOK, map[string]any{
		"community_id":      communityID,
		"actor_user_id":     moderatorID,
		"permission":        "ban_members",
		"allowed":           false,
		"denial_reason":     reason,
		"actor_is_member":   true,
		"actor_is_owner":    false,
		"actor_permissions": []string{},
		"target":            nil,
	})
}

// activeBan returns a stored active ban.
func activeBan() models.Sanction {
	return models.Sanction{
		ID:           sanctionID,
		CommunityID:  communityID,
		TargetUserID: targetID,
		ActorUserID:  ptr(moderatorID),
		Type:         models.SanctionTypeBan,
		Source:       models.SourceModerator,
		IssuedAt:     time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
	}
}

// ptr returns a pointer to value.
func ptr(value string) *string { return &value }

// nowTime is the deterministic request clock used by the fixture.
func nowTime() time.Time { return time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC) }
