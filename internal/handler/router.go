package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"moderation_service/internal/authz"
	"moderation_service/internal/observability"
	"moderation_service/internal/service"
)

// ReadinessProbe reports whether this replica may serve traffic.
type ReadinessProbe interface {
	// Ready returns nil when the replica is ready, or a domain error otherwise.
	Ready(ctx context.Context) error
}

// Handlers bundles the domain services the HTTP layer delegates to. Handlers
// stay thin: bind, delegate, map errors.
type Handlers struct {
	// Sanctions serves the sanction endpoints and the member status view.
	Sanctions *service.SanctionService
	// Appeals serves appeal submission, review, and the queues.
	Appeals *service.AppealService
	// Automod serves the versioned automatic filter configuration.
	Automod *service.AutomodService
	// Audit serves the append-only audit log read.
	Audit *service.AuditService
	// Readiness backs /readyz.
	Readiness ReadinessProbe
}

// RouterConfig configures the HTTP surface.
type RouterConfig struct {
	// Logger is the structured JSON logger.
	Logger *zap.Logger
	// Metrics records RED metrics on every route.
	Metrics *observability.Metrics
	// Verifier verifies identity_service access tokens locally.
	Verifier *authz.Verifier
	// Handlers holds the domain services.
	Handlers *Handlers
	// Registry exposes the Prometheus metrics of this service.
	Registry prometheus.Gatherer
}

// NewRouter builds the gin engine with every path of docs/openapi_spec.yaml.
func NewRouter(config RouterConfig) *gin.Engine {
	engine := gin.New()
	engine.HandleMethodNotAllowed = true
	engine.Use(
		recoveryMiddleware(config.Logger),
		traceMiddleware(),
		loggingMiddleware(config.Logger),
		metricsMiddleware(config.Metrics),
	)

	handlers := config.Handlers
	if handlers == nil {
		handlers = &Handlers{}
	}

	// Probes are unauthenticated and dependency-free (liveness) or dependency
	// only (readiness), so a rolling deploy never fails readiness on auth.
	engine.GET("/healthz", handlers.liveness)
	engine.GET("/readyz", handlers.readiness)
	if config.Registry != nil {
		engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(config.Registry, promhttp.HandlerOpts{})))
	}

	engine.NoRoute(func(c *gin.Context) { respondError(c, errNotFound) })
	engine.NoMethod(func(c *gin.Context) { respondError(c, errMethodNotAllowed) })

	authenticated := engine.Group("")
	authenticated.Use(authMiddleware(config.Verifier))

	authenticated.POST("/communities/:community_id/sanctions", handlers.createSanction)
	authenticated.GET("/communities/:community_id/sanctions", handlers.listSanctions)
	authenticated.POST("/communities/:community_id/sanctions/:sanction_id/lift", handlers.liftSanction)
	authenticated.POST("/sanctions/:sanction_id/appeals", handlers.createAppeal)
	authenticated.GET("/me/sanctions", handlers.listMySanctions)
	authenticated.GET("/me/appeals", handlers.listMyAppeals)
	authenticated.GET("/communities/:community_id/appeals", handlers.listCommunityAppeals)
	authenticated.POST("/communities/:community_id/appeals/:appeal_id/decision", handlers.decideAppeal)
	authenticated.GET("/communities/:community_id/automod", handlers.getAutomod)
	authenticated.PUT("/communities/:community_id/automod", handlers.putAutomod)
	authenticated.GET("/communities/:community_id/audit-log", handlers.listAuditLog)

	return engine
}

// newServer builds the HTTP server with the timeouts a stateless container
// needs so a slow client cannot pin a connection indefinitely.
func newServer(addr string, engine *gin.Engine) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           engine,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
