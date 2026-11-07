# moderation_service — Engineering Guide

Persistent technical memory for implementing and evolving `moderation_service` (Go). Enforced portable stack: PostgreSQL, Kafka (producer), stateless containers. No S3-compatible storage and no external third-party providers are required for this service.

## Runtime & Toolchain

- **Language / runtime:** Go 1.26 (module name `moderation_service`; initialize with `go mod init moderation_service` — no VCS host prefix).
- **Package manager:** Go modules (`go.mod` / `go.sum`). Never edit them by hand.
- **Build/test entrypoints (`Makefile` targets):**
  - `make install` — `go mod download` (always first on a cold workspace).
  - `make test` — `go test ./... -race -count=1 -parallel 4`.
  - `make build` — Docker image build (developer/CI only; not in the agent runtime).
  - `make run` — `go run ./cmd/server`.
  - `make lint` — `golangci-lint run ./...`.
- **Dockerfile:** multi-stage `golang:1.26-alpine` builder → `distroless/static-debian12` runtime, `CGO_ENABLED=0`, non-root `USER nonroot:nonroot`.

## Libraries & External Provider SDKs

| Concern | Package / Library | Source | Usage Notes |
|---|---|---|---|
| REST API / HTTP framework | `github.com/gin-gonic/gin` | language-skill | Handlers stay thin; bind/validate request bodies at the boundary; return the platform error shape `{"error", "detail"}`. |
| JWT verification (identity tokens) | `github.com/golang-jwt/jwt/v5` | tech-lead-research | Verify locally with RSA/EdDSA public keys from config (`IDENTITY_JWKS_KEYS`); validate `exp`, `iss`, `aud`; claims `sub` (user id), `sid`. Do not call `identity_service` at request time. |
| PostgreSQL driver | `github.com/jackc/pgx/v5` | language-skill | Use `pgxpool` with parameterized queries; `SELECT ... FOR UPDATE SKIP LOCKED` for the expiry sweep; never build SQL by string concatenation. |
| Migrations | `github.com/golang-migrate/migrate/v4` | language-skill | SQL migrations applied before serving traffic; readiness fails until applied. |
| Kafka client (producer) | `github.com/IBM/sarama` | language-skill | Idempotent producer; topic `moderation_events` partitioned by `community_id`; envelope mirrored into `trace_id`/`event_type` message headers; configure via `KAFKA_BROKERS`. |
| Logging | `go.uber.org/zap` | language-skill | Structured JSON production logging; `trace_id` on every line. |
| Metrics | `github.com/prometheus/client_golang/prometheus` | language-skill | Register business + RED metrics (see architecture §7); low-cardinality labels only. |
| HTTP client for API-001 | `net/http` (`http.Client`) | tech-lead-research | Plain `http.Client` with 2 s timeout, one retry on `5xx`/timeout, `traceparent`/`X-Trace-Id` propagation; no SDK exists for this internal contract. |
| Configuration | `github.com/joho/godotenv` | language-skill | Local dev only; production reads env vars directly. |
| Testing framework | standard `testing` + `github.com/stretchr/testify` | language-skill | Table-driven tests, `t.Parallel()` everywhere. |
| Mocking | `github.com/stretchr/testify/mock` | language-skill | Mock repositories, the authorization client, and the Kafka producer. |
| HTTP mocking | `github.com/jarcoal/httpmock` | language-skill | Stub API-001 responses and failure modes in tests. |

Resolve each version with `go get <module>@latest` (or per `Pinned Decisions`) and `go mod tidy`; verify the API with `technical_research_agent` before adding a dependency. **There are no external third-party provider SDKs** for this service — its only outbound dependency is the internal `community_service` API-001 call.

## Container Lifecycle & Probes

- **Statelessness:** no session or cache state may live in process memory across requests; every endpoint and job is safe to run with `replicas > 1` concurrently.
- **Signals:** trap `SIGTERM` and `SIGINT` via `signal.NotifyContext`. On signal: (1) flip readiness to failing so the load balancer stops routing; (2) stop accepting new HTTP requests; (3) drain in-flight requests and in-flight outbox dispatch batches within a bounded 20 s shutdown timeout; (4) close the pgx pool and the Kafka producer; (5) exit 0.
- **`GET /healthz` (liveness):** responds 200 while the process is responsive; MUST NOT check PostgreSQL/Kafka so transient dependency hiccups never restart the container.
- **`GET /readyz` (readiness):** checks pgx pool ping and Kafka producer availability; returns 503 when either is unavailable or once shutdown has started.
- **Scheduled jobs** run inside the server process on background goroutines (expiry sweep every 30 s; retention purge daily). Both must respect the shutdown timeout and stop cleanly.

## Testing Strategy

- **Structure:** unit tests beside the code (`*_test.go`); integration-level tests under `tests/integration/`; shared fakes/mocks under `tests/mocks/` or `tests/fixtures/`.
- **Determinism & parallelism:** all tests run with `-race -parallel 4`; no shared mutable state; fixed clock injection for expiry and retention tests; no real network calls, ever.
- **Database:** repository tests run against in-memory fakes or a `pgx` mock that reproduces real errors/constraints; SQL correctness (migrations, partial indexes, `SKIP LOCKED` behavior) is verified in developer/CI environments against a real PostgreSQL — that part is deferred, not agent-executed.
- **Kafka:** the producer is behind an interface; tests assert the outbox rows and envelope structure produced, using a mock producer. Envelope assertions: all nine fields present, `trace_id` non-empty, payload has no envelope field.
- **Authorization client:** `httpmock` stubs for allowed / denied / timeout / 5xx responses, verifying fail-closed behavior (`503`, no writes).
- **Coverage:** ≥ 80 % on critical paths — sanction issue/lift/expiry, appeal submit/decide (including the overturn cascade emitting both events), automod versioning, audit append/purge, idempotency replay.

## Coding Conventions & Project Structure

```
cmd/server/main.go          # entrypoint: config, wiring, graceful shutdown
internal/config/config.go   # typed Config struct loaded + validated from env at startup
internal/handler/           # gin handlers (thin) — bind, delegate, map errors to HTTP
internal/service/           # business logic: sanction, appeal, automod, audit services
internal/repository/        # pgx-based data access (sanction, appeal, automod, audit, outbox)
internal/authz/             # API-001 authorization client (fail-closed)
internal/events/            # envelope construction + outbox dispatcher (Kafka relay)
internal/jobs/              # mute-expiry sweeper, audit retention purge
internal/models/            # domain types, request/response structs
tests/unit/ tests/integration/ tests/mocks/
```

- Layering is one-directional: `handler → service → repository`; services never touch `gin` contexts; repositories never contain business rules.
- Errors: define sentinel domain errors; handlers map them to status codes once (no per-handler ad-hoc mapping). Return errors — never `panic` in non-init code.
- All exported types and functions carry Go doc comments; comments explain *why*, not *what*; no TODOs, no dead code.
- Table-driven tests with `t.Parallel()`; test names read as specifications.
- Prepared statements / parameterized queries only — `fmt.Sprintf` in SQL is forbidden.

## Configuration & Secrets Management

12-factor: all configuration from environment variables, validated at startup (fail fast with a structured error log). Required variables:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (portable form). |
| `KAFKA_BROKERS` | Comma-separated broker list for the `moderation_events` producer. |
| `KAFKA_TOPIC_MODERATION_EVENTS` | Topic name (default `moderation_events`). |
| `COMMUNITY_SERVICE_URL` | Base URL of `community_service` for API-001. |
| `COMMUNITY_SERVICE_TOKEN` | Per-edge service token sent as `Authorization: Bearer` to API-001. |
| `IDENTITY_JWKS_KEYS` | identity_service public verification keys (current + next, JSON) for local token verification. |
| `IDENTITY_TOKEN_ISSUER` / `IDENTITY_TOKEN_AUDIENCE` | Expected `iss` / `aud` claims. |
| `SERVICE_PORT` | HTTP listen port (default 8080). |
| `LOG_LEVEL` | `debug` \| `info` \| `warn` \| `error` (default `info`). |

Secrets (`COMMUNITY_SERVICE_TOKEN`, database credentials) come from deployment secrets/env injection only — never `.env` files in production, never committed, never logged.

## Observability & Logging Conventions

- **Structured JSON logging:** every line has `timestamp`, `level`, `service_name: "moderation_service"`, `trace_id`, `request_id`/`event_id`, `operation`, `status`. Levels: DEBUG (dev), INFO, WARN (degraded), ERROR. No free-form messages, no secrets, no user-provided content logged verbatim.
- **Trace propagation:** accept `traceparent` / `X-Trace-Id` on every HTTP request; propagate to the API-001 call; copy `trace_id`/`correlation_id` into every published event envelope. Scheduled jobs mint a fresh 32-hex `trace_id` at the edge of the flow.
- **Mandatory event envelope (producer):** every Kafka message is `{"envelope": {...}, "payload": {...}}` with exactly two top-level keys. The envelope carries `event_id`, `event_type`, `event_version`, `occurred_at`, `producer` (`moderation_service`), `trace_id`, `correlation_id`, `causation_id` (nullable only for the flow's first event), `schema_ref` (`EVENT_CONTRACTS.md#<event_type>-v<version>`). `trace_id` is **never empty** — propagated from the originating request or minted at the edge of the flow. A payload MUST NOT carry an envelope field. Model the envelope as a single reusable struct — never construct maps ad hoc at publish sites. The envelope is mirrored into Kafka headers (`trace_id`, `event_type`).
- **Metric registration:** register once at startup via the Prometheus registry; business metrics per architecture §7; keep labels low-cardinality.

## Pinned Decisions

| Dependency / Area | Pin / Constraint | Rationale | Added By | Date |
|---|---|---|---|---|
| Go runtime | `1.26` | Platform-wide Go standard from the system design; distroless multi-stage build. | tech-lead | 2026-01-14 |
| Kafka client | `github.com/IBM/sarama` | Approved library; idempotent producer; envelope headers mirrored. | tech-lead | 2026-01-14 |
| PostgreSQL driver | `github.com/jackc/pgx/v5` (no ORM) | Explicit SQL needed for partial unique indexes and `SKIP LOCKED` sweep queries. | tech-lead | 2026-01-14 |
| Event delivery | Transactional outbox → Kafka relay | Guarantees at-least-once event publication even if the process dies between commit and publish; consumers dedupe on `event_id` (ADR-003). | tech-lead | 2026-01-14 |
| Authorization | Real-time API-001 checks only — no cache, fail closed `503` | Permission/hierarchy must be correct at the moment of action (ADR-001). | tech-lead | 2026-01-14 |
