# moderation_service

Sanctions (mute / kick / ban), sanction lifecycle, member appeals, per-community
automatic filter configuration, and the one-year audit log for the Community
Chat & Voice Platform.

Every moderator action is authorized in real time against `community_service`
(API-001, fail closed) and published on the `moderation_events` Kafka topic
through a transactional outbox.

## Layout

```
cmd/server/main.go          entrypoint: config, wiring, graceful shutdown
internal/config             typed configuration loaded from the environment
internal/handler            gin handlers, middleware, error mapping
internal/service            business rules (sanctions, appeals, automod, audit)
internal/repository         pgx data access + SQL migrations
internal/authz              identity token verification + API-001 client
internal/events             event envelope, outbox relay, Kafka producer
internal/jobs               mute-expiry sweep, audit retention purge
internal/models             domain types, requests, responses, domain errors
internal/observability      structured logging, metrics, trace propagation
```

## Running

```bash
make install   # go mod download
make run       # go run ./cmd/server
make test      # go test ./... -race -count=1 -parallel 4
make lint      # golangci-lint run ./...
make build     # docker image (golang:1.26-alpine -> distroless/static)
```

Configuration is 12-factor; see `internal/config/config.go` for the accepted
environment variables and their defaults. Migrations are embedded in the binary
and applied before the HTTP listener starts.
