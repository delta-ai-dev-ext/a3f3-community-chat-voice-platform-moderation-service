// Command server is the moderation_service entrypoint. It loads the typed
// configuration, wires the HTTP stack and the in-process background loops, and
// drains all of them on SIGTERM/SIGINT within the platform's shutdown budget
// (docs/engineering_guide.md, "Container Lifecycle & Probes").
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"moderation_service/internal/authz"
	"moderation_service/internal/config"
	"moderation_service/internal/events"
	"moderation_service/internal/handler"
	"moderation_service/internal/jobs"
	"moderation_service/internal/observability"
	"moderation_service/internal/repository"
	"moderation_service/internal/service"
)

// startupTimeout bounds the pool set-up so a wedged DNS lookup or TCP connect
// cannot hold the container in "starting" forever.
const startupTimeout = 10 * time.Second

// HTTP server timeouts. They are deliberately explicit: a stateless replica
// must never let one slow client pin a connection indefinitely.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
)

func main() {
	if err := run(); err != nil {
		fatal(err)
	}
}

// fatal reports a startup or shutdown failure in the structured log shape and
// exits non-zero. Configuration problems happen before the configured logger
// exists, so a bootstrap logger at the default level guarantees the reason is
// still machine-readable.
func fatal(err error) {
	logger, loggerErr := observability.NewLogger(zapcore.InfoLevel)
	if loggerErr != nil {
		fmt.Fprintf(os.Stderr, "moderation_service: fatal: %v\n", err)
	} else {
		logger.Error("moderation_service failed",
			zap.String("operation", "startup"),
			zap.String("status", "error"),
			zap.Error(err),
		)
		_ = logger.Sync()
	}
	os.Exit(1)
}

// run assembles the process and returns once it has fully drained. A nil return
// means the replica shut down on a signal with everything closed.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger, err := observability.NewLogger(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	// The service owns its registry so /metrics exports exactly this process's
	// metrics and nothing inherited from the global default registry.
	registry := prometheus.NewRegistry()
	metrics, err := observability.NewMetrics(registry)
	if err != nil {
		return fmt.Errorf("register metrics: %w", err)
	}

	// Identity tokens are verified locally, so broken key configuration must
	// stop startup rather than reject every request at runtime.
	verifier, err := authz.NewVerifier(cfg.IdentityJWKSKeys, cfg.IdentityTokenIssuer, cfg.IdentityTokenAudience)
	if err != nil {
		return fmt.Errorf("build identity verifier: %w", err)
	}

	// Migrations run before the pool and the listener: a replica must never
	// report ready against a schema the binary does not match.
	if err := repository.Migrate(cfg.DatabaseURL); err != nil {
		return err
	}

	poolCtx, cancelPool := context.WithTimeout(context.Background(), startupTimeout)
	defer cancelPool()
	pool, err := repository.NewPool(poolCtx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: the producer and the loops that use it
	// are already gone by the time the pool closes.
	defer pool.Close()

	producer, err := events.NewKafkaProducer(cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("connect kafka producer: %w", err)
	}
	defer func() {
		if closeErr := producer.Close(); closeErr != nil {
			logger.Warn("kafka producer close failed",
				zap.String("operation", "shutdown"),
				zap.Error(closeErr),
			)
		}
	}()

	store := repository.NewPgxStore(pool)
	builder := events.NewBuilder()
	shutdown := handler.NewShutdownState()

	deps := service.Deps{
		Store:      store,
		Authorizer: authz.NewClient(cfg.CommunityServiceURL, cfg.CommunityServiceToken, metrics, logger, nil),
		Events:     builder,
		Topic:      cfg.KafkaTopicModerationEvents,
		Logger:     logger,
		Metrics:    metrics,
	}

	// gin's built-in request logging is replaced by the middleware chain, which
	// emits one structured line per request carrying the trace id.
	gin.SetMode(gin.ReleaseMode)
	engine := handler.NewRouter(handler.RouterConfig{
		Logger:   logger,
		Metrics:  metrics,
		Verifier: verifier,
		Registry: registry,
		Handlers: &handler.Handlers{
			Sanctions: service.NewSanctionService(deps),
			Appeals:   service.NewAppealService(deps),
			Automod:   service.NewAutomodService(deps),
			Audit:     service.NewAuditService(deps),
			// Readiness pings PostgreSQL and the producer, and fails as soon
			// as shutdown starts so the load balancer stops routing here.
			Readiness: &handler.Ready{Database: pool, Producer: producer, Shutdown: shutdown},
		},
	})

	loopsCtx, stopLoops := context.WithCancel(context.Background())
	defer stopLoops()
	loops := startBackgroundLoops(loopsCtx, logger, store, producer, builder, metrics, cfg.KafkaTopicModerationEvents)

	server := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.ServicePort),
		Handler:           engine,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()

	// Serve on a goroutine so a bind failure and a signal are both handled by
	// the same shutdown path instead of os.Exit racing the drain.
	serveErr := make(chan error, 1)
	go func() { serveErr <- serve(server) }()

	logger.Info("http listener started",
		zap.Int("port", cfg.ServicePort),
		zap.String("operation", "http_listen"),
		zap.String("status", "accepted"),
	)

	var shutdownErr error
	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received",
			zap.String("operation", "shutdown"),
			zap.String("status", "accepted"),
		)
	case shutdownErr = <-serveErr:
		if shutdownErr == nil {
			// The listener stopped without a signal or an error, which is only
			// possible if something else closed it; report it rather than
			// pretending the process drained cleanly.
			shutdownErr = errors.New("http listener stopped unexpectedly")
		}
	}

	// (1) Fail readiness first so no new traffic is routed to this replica.
	shutdown.Begin()

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownTimeoutSeconds)*time.Second)
	defer cancelDrain()

	// (2) and (3): stop accepting connections, then drain in-flight requests
	// inside the shared budget.
	if err := server.Shutdown(drainCtx); err != nil {
		logger.Warn("http drain did not complete",
			zap.String("operation", "shutdown"),
			zap.Error(err),
		)
		if shutdownErr == nil {
			shutdownErr = fmt.Errorf("drain http server: %w", err)
		}
	}

	// (3) The loops observe cancellation and finish the batch they are in.
	stopLoops()
	if !waitLoops(drainCtx, loops) {
		logger.Warn("background loops did not stop before the shutdown deadline",
			zap.String("operation", "shutdown"),
		)
		if shutdownErr == nil {
			shutdownErr = errors.New("background loops did not stop before the shutdown deadline")
		}
	}

	// (4) The deferred producer and pool closes run when run returns.
	logger.Info("shutdown complete",
		zap.String("operation", "shutdown"),
		zap.String("status", "accepted"),
	)
	return shutdownErr
}

// serve runs the listener until it is closed. ErrServerClosed is the expected
// outcome of the graceful path, so it is not treated as a failure.
func serve(server *http.Server) error {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve http: %w", err)
	}
	return nil
}

// startBackgroundLoops launches the outbox relay, the mute-expiry sweep and the
// audit retention purge. Every loop returns when ctx is cancelled, so one
// cancel plus waitLoops drains them inside the shutdown budget.
func startBackgroundLoops(
	ctx context.Context,
	logger *zap.Logger,
	store repository.Store,
	producer events.Producer,
	builder *events.Builder,
	metrics *observability.Metrics,
	topic string,
) *sync.WaitGroup {
	dispatcher := events.NewDispatcher(events.DispatcherConfig{
		Store:    store,
		Producer: producer,
		Logger:   logger,
		Metrics:  metrics,
	})
	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:   store,
		Events:  builder,
		Topic:   topic,
		Metrics: metrics,
		Logger:  logger,
	})
	purger := jobs.NewAuditRetentionPurger(jobs.RetentionConfig{
		Store:   store,
		Metrics: metrics,
		Logger:  logger,
	})

	var loops sync.WaitGroup
	for _, runLoop := range []func(context.Context){dispatcher.Run, sweeper.Run, purger.Run} {
		loops.Add(1)
		go func(runLoop func(context.Context)) {
			defer loops.Done()
			runLoop(ctx)
		}(runLoop)
	}
	return &loops
}

// waitLoops blocks until every loop returned, or the shutdown deadline passed.
// It reports whether the drain finished, so a stuck loop cannot hold the
// process past the platform's drain budget.
func waitLoops(ctx context.Context, loops *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		loops.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
