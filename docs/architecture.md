# moderation_service — Architecture

Service-level architecture for the moderation domain of the Community Chat & Voice Platform. This document is the source of truth for service boundaries, request/event flows, inter-service contracts, the PostgreSQL schema, the stateless container operational model, and observability metrics. Implementation detail (libraries, folder layout, tooling) lives in `docs/engineering_guide.md`.

## 1. Service Responsibilities & Component Boundaries

`moderation_service` owns, exclusively:

- **Sanctions** — mute (timed or indefinite), kick (immediate removal), ban (permanent until lifted) issued by moderators.
- **Sanction lifecycle** — lifting by moderators, automatic expiry of timed mutes, lifts caused by appeal overturns.
- **Member-facing sanction status** — a sanctioned user reads their own active sanctions, including for communities they are no longer a member of (bans).
- **Appeals** — one open appeal per sanction by the affected member; review (uphold / overturn) by moderators with `review_appeals`; an overturn lifts the sanction.
- **Automatic filter configuration (automod)** — per-community rule sets (spam thresholds, banned words, block/remove actions, exempt roles), versioned, published to `messaging_service` which enforces them.
- **Audit log** — an immutable, append-only record of every moderator action (sanction issued/lifted/expired, appeal decided, automod configuration changed), retained for exactly one year, then purged by a daily job.

`moderation_service` does NOT: enforce mutes on the message hot path or in voice channels (`messaging_service` / `voice_service` do, from events), record messages, apply bans to community membership (`community_service` does from events), or notify the affected member (`notification_service` does, from events).

### Internal components

| Component | Responsibility |
|---|---|
| HTTP API | Public endpoints (sanctions, appeals, automod, audit log, probes). Thin; delegates to domain services. |
| Token verifier | Verifies `identity_service`-issued access tokens **locally** (asymmetric signature, current + next public key from configuration). No runtime call to `identity_service`. |
| Authorization client | Calls `community_service` API-001 for permission + hierarchy checks before every moderator action. **Fails closed.** |
| Sanction service | Issue, lift, expire sanctions; enforces type rules (kick has no duration, only mutes expire, lifts only apply to mute/ban). |
| Appeal service | Submission (one open per sanction), review, overturn cascade (lift sanction + emit both events). |
| Automod service | Validate and version rule sets; publish snapshots. |
| Audit service | Append-only writes; filtered reads limited to the last 365 days. |
| Event publisher | Writes the mandatory event envelope for every state change onto the outbox and relays it to Kafka. |
| Scheduled jobs | (1) Mute-expiry sweep: lifts due timed mutes, emitting `unmute` with `source = expiry`; (2) Retention purge: daily deletion of audit entries older than 365 days. |

## 2. Request Flows

### 2.1 Sanction issue (mute / kick / ban)

1. Client sends `POST /communities/{community_id}/sanctions` with a bearer access token (verified locally) and an optional `Idempotency-Key`.
2. The service calls `community_service` API-001: `actor_user_id`, `permission` (mute → `mute_members`, kick → `kick_members`, ban → `ban_members`), `target_user_id`.
3. If the call fails (timeout after one retry, `5xx`), the request fails closed with `503 authorization_unavailable` — no state change, no event, no audit entry.
4. If the response is `allowed: false`, the request fails with the `403` error code derived from `denial_reason` (`missing_permission` → `insufficient_permission`, `target_outranks_actor` → `target_outranks_actor`, `target_is_owner` → `target_is_owner`, `target_is_self` → `target_is_self`).
5. On success the sanction is persisted and — in the same database transaction — the `member_sanction_changed` event (EVT-004) and the `sanction_issued` audit entry are appended (outbox pattern).
6. The dispatcher relays the event to Kafka topic `moderation_events`, partitioned by `community_id`.

### 2.2 Sanction lift (unmute / unban)

Same authorization flow with `permission` matching the sanction type (`mute_members` for mutes, `ban_members` for bans), target = the sanction's original target. Kicks cannot be lifted. The stored sanction is marked lifted (`lift_source = moderator`), an `unmute`/`unban` EVT-004 with `source = moderator` and a `sanction_lifted` audit entry are committed.

### 2.3 Timed mute expiry (scheduled)

A sweep runs on a short interval in every replica. Due active mutes are selected with row-level locking that prevents double processing across replicas, lifted with `lift_source = expiry`, and an `unmute` EVT-004 with `source = expiry`, `actor_user_id = null` and a minted `trace_id` (no originating HTTP request) is committed with a `sanction_expired` audit entry.

### 2.4 Member status (`/me/sanctions`, `/me/appeals`)

Authenticated caller reads their own active sanctions (type, reason, expiry, `appealable`, open appeal id) across all communities — including bans where the caller is no longer a member — and their own appeals with outcomes. No authorization call to API-001 (the caller only ever sees their own rows).

### 2.5 Appeals

- **Submit:** the caller must own the sanction; the sanction must be an active mute or ban; at most one open (`submitted`) appeal per sanction is enforced by a partial unique index. Emits EVT-006 `submitted`.
- **Decide:** a moderator with `review_appeals` upholds or overturns. `upheld` keeps the sanction active. `overturned` lifts the sanction with `lift_source = appeal` and emits EVT-004 (`unmute`/`unban`, `source = appeal`) **and** EVT-006 (`overturned`) in the same transaction. Either way a `sanction_lifted` audit entry (on overturn) plus an `appeal_decided` audit entry are appended.

### 2.6 Automod configuration

Reads and writes require `manage_automod`. A `PUT` validates the full rule set, stores it as `max(rule_set_version) + 1` for the community, marks it current, emits EVT-005 (full snapshot) and appends an `automod_config_changed` audit entry. Invalid configurations are rejected with `422` and leave the previous version current.

### 2.7 Audit log read

Requires `view_audit_log`. Reads are filtered to `created_at >= now() - 365 days`, with optional `action_type` and `actor` filters and cursor pagination.

## 3. Event Flows (producer only)

`moderation_service` consumes **no** Kafka events. It produces three event types on the topic `moderation_events`, all partitioned by `community_id`, wrapped in the mandatory envelope (`envelope` / `payload` — see the envelope table under Inter-Service Contracts below).

| Contract | Event type | Emitted when |
|---|---|---|
| EVT-004 | `member_sanction_changed` | Mute, unmute, kick, ban, unban (sources: `moderator`, `expiry`, `appeal`) |
| EVT-005 | `automod_rules_changed` | Automod rule set version stored (full snapshot) |
| EVT-006 | `appeal_status_changed` | Appeal submitted / upheld / overturned |

Downstream effects (owned by consumers): `community_service` applies member state and re-emits `community_member_changed`; `messaging_service` replaces its rule-set projection; `notification_service` sends the moderation notice and appeal outcome.

## Inter-Service Contracts

This service's slice of the project-level `API_CONTRACTS.md` / `EVENT_CONTRACTS.md`, copied verbatim.

### Sync — consumer of API-001 (community_service)

`moderation_service` is the **caller**. Before every moderator action (sanction issue/lift, appeal review, automod configuration change, audit-log read) it confirms the actor holds the required permission and, when the action has a target, that the actor outranks the target. Banned users who are no longer members are still reported through `target.is_banned` so that unbans and appeal reviews work.

| Field | Value |
|---|---|
| Provider | `community_service` |
| Consumers | `moderation_service` |
| Kind | REST |
| Operation | `POST /internal/communities/{community_id}/authorization-checks` |
| Auth | service token, `Authorization: Bearer` |
| Idempotency | not required (read-only check, safe to retry) |

Path parameters: `community_id` (uuid).

**Request** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["actor_user_id", "permission"],
  "properties": {
    "actor_user_id": {"type": "string", "format": "uuid"},
    "permission": {
      "type": "string",
      "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
    },
    "target_user_id": {
      "type": ["string", "null"],
      "format": "uuid",
      "description": "Member the action applies to; null or omitted for non-targeted actions (automod config, audit-log read)."
    }
  }
}
```

**Response 200** (`application/json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["community_id", "actor_user_id", "permission", "allowed", "denial_reason", "actor_is_member", "actor_is_owner", "actor_permissions", "target"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "actor_user_id": {"type": "string", "format": "uuid"},
    "permission": {
      "type": "string",
      "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
    },
    "allowed": {"type": "boolean", "description": "True only when the actor is an active member, holds the permission, and (if a target is given) outranks the target."},
    "denial_reason": {
      "type": ["string", "null"],
      "enum": ["actor_not_member", "missing_permission", "target_outranks_actor", "target_is_owner", "target_is_self", null]
    },
    "actor_is_member": {"type": "boolean"},
    "actor_is_owner": {"type": "boolean"},
    "actor_permissions": {
      "type": "array",
      "uniqueItems": true,
      "items": {
        "type": "string",
        "enum": ["manage_community", "manage_channels", "manage_roles", "create_invites", "mute_members", "kick_members", "ban_members", "manage_automod", "view_audit_log", "review_appeals"]
      }
    },
    "target": {
      "type": ["object", "null"],
      "description": "Null when the request had no target_user_id.",
      "required": ["user_id", "is_member", "is_banned", "is_owner", "outranked_by_actor", "muted_until"],
      "properties": {
        "user_id": {"type": "string", "format": "uuid"},
        "is_member": {"type": "boolean"},
        "is_banned": {"type": "boolean"},
        "is_owner": {"type": "boolean"},
        "outranked_by_actor": {"type": "boolean"},
        "muted_until": {"type": ["string", "null"], "format": "date-time"}
      }
    }
  }
}
```

**Errors** — `400` (malformed body / unknown permission), `401` (missing or invalid service token), `404` (`community_not_found`), `500`, `503`, all `application/problem+json` referencing `shared:ErrorResponse`. A denied check is **not** an error: it returns `200` with `allowed: false` and a `denial_reason`.

Caller rules: 2 s timeout, at most one retry on `5xx`/timeout (read-only check, safe to retry); `traceparent` / `X-Trace-Id` propagated.

### Async — producer of EVT-004 / EVT-005 / EVT-006

Topic `moderation_events`, key `community_id`, at-least-once delivery. Payload schemas, verbatim:

**EVT-004 — `member_sanction_changed` v1** (consumers: `community_service`, `notification_service`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["sanction_id", "community_id", "target_user_id", "actor_user_id", "action", "source", "reason", "expires_at", "appealable"],
  "properties": {
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "target_user_id": {"type": "string", "format": "uuid"},
    "actor_user_id": {"type": ["string", "null"], "format": "uuid", "description": "Null when source is expiry."},
    "action": {"type": "string", "enum": ["mute", "unmute", "kick", "ban", "unban"]},
    "source": {"type": "string", "enum": ["moderator", "expiry", "appeal"]},
    "reason": {"type": ["string", "null"], "maxLength": 1000},
    "expires_at": {"type": ["string", "null"], "format": "date-time", "description": "Mute end time; null for permanent bans, kicks, and lift actions."},
    "appealable": {"type": "boolean", "description": "True for active mute and ban sanctions the member can appeal."}
  }
}
```

**EVT-005 — `automod_rules_changed` v1** (consumer: `messaging_service`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["community_id", "rule_set_version", "spam", "banned_words", "exempt_role_ids"],
  "properties": {
    "community_id": {"type": "string", "format": "uuid"},
    "rule_set_version": {"type": "integer", "minimum": 1},
    "spam": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "max_messages_per_window", "window_seconds", "max_duplicate_messages", "max_mentions_per_message", "max_links_per_message"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "max_messages_per_window": {"type": "integer", "minimum": 1},
        "window_seconds": {"type": "integer", "minimum": 1, "maximum": 3600},
        "max_duplicate_messages": {"type": "integer", "minimum": 1},
        "max_mentions_per_message": {"type": "integer", "minimum": 1},
        "max_links_per_message": {"type": "integer", "minimum": 0}
      }
    },
    "banned_words": {
      "type": "object",
      "additionalProperties": false,
      "required": ["enabled", "action", "match_mode", "words"],
      "properties": {
        "enabled": {"type": "boolean"},
        "action": {"type": "string", "enum": ["block", "remove"]},
        "match_mode": {"type": "string", "enum": ["whole_word", "substring"]},
        "words": {"type": "array", "items": {"type": "string", "minLength": 1, "maxLength": 100}, "maxItems": 1000, "uniqueItems": true}
      }
    },
    "exempt_role_ids": {"type": "array", "items": {"type": "string", "format": "uuid"}, "uniqueItems": true}
  }
}
```

**EVT-006 — `appeal_status_changed` v1** (consumer: `notification_service`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["appeal_id", "sanction_id", "community_id", "appellant_user_id", "sanction_type", "status", "reviewer_user_id", "decision_note"],
  "properties": {
    "appeal_id": {"type": "string", "format": "uuid"},
    "sanction_id": {"type": "string", "format": "uuid"},
    "community_id": {"type": "string", "format": "uuid"},
    "appellant_user_id": {"type": "string", "format": "uuid"},
    "sanction_type": {"type": "string", "enum": ["mute", "ban"]},
    "status": {"type": "string", "enum": ["submitted", "upheld", "overturned"]},
    "reviewer_user_id": {"type": ["string", "null"], "format": "uuid"},
    "decision_note": {"type": ["string", "null"], "maxLength": 2000}
  }
}
```

**Envelope (mandatory, every event).** Every message is a JSON object with exactly two top-level keys, `envelope` and `payload`:

| Field | Type | Required | Rule |
|---|---|---|---|
| `event_id` | string (uuid) | yes | Unique per event. Consumer deduplication key. |
| `event_type` | string (snake_case) | yes | Must match a registered contract. |
| `event_version` | integer | yes | >= 1. |
| `occurred_at` | string (RFC3339 UTC) | yes | Producer clock at emit time. |
| `producer` | string (snake_case) | yes | `moderation_service`. |
| `trace_id` | string (32 lowercase hex) | yes | **Never empty.** Propagated from the originating request; minted at the edge for scheduled jobs. |
| `correlation_id` | string (uuid) | yes | Groups every event of one business flow. |
| `causation_id` | string (uuid) or null | yes | `event_id` of the causing event; `null` for the flow's first event. |
| `schema_ref` | string | yes | `EVENT_CONTRACTS.md#<event_type>-v<version>`. |

The envelope is also mirrored into Kafka message headers (`trace_id`, `event_type`). A payload MUST NOT declare an envelope field name at its top level.

## 5. PostgreSQL Schema

Owned and defined by this service. All timestamps are `timestamptz`. All UUIDs are v4.

```sql
-- Sanctions: the authority for every moderation decision.
CREATE TABLE sanction (
    id              uuid PRIMARY KEY,
    community_id    uuid NOT NULL,
    target_user_id  uuid NOT NULL,
    actor_user_id   uuid,                          -- NULL when source = expiry
    type            text NOT NULL CHECK (type IN ('mute','kick','ban')),
    reason          text CHECK (char_length(reason) <= 1000),
    source          text NOT NULL CHECK (source IN ('moderator','expiry','appeal')),
    issued_at       timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz,                   -- timed mutes only; NULL = indefinite
    lifted_at       timestamptz,                   -- NULL while active
    lift_source     text CHECK (lift_source IN ('moderator','expiry','appeal')),
    lift_reason     text CHECK (char_length(lift_reason) <= 1000),
    CONSTRAINT sanction_type_duration CHECK (
        (type = 'mute') OR (expires_at IS NULL)
    ),
    CONSTRAINT sanction_lift_consistency CHECK (
        (lifted_at IS NULL) OR (lift_source IS NOT NULL)
    )
);
-- Active sanctions per member (the hot path for /me/sanctions and moderation lists).
CREATE INDEX idx_sanction_active ON sanction (community_id, target_user_id)
    WHERE lifted_at IS NULL;
-- Due timed mutes for the expiry sweep.
CREATE INDEX idx_sanction_expiry_due ON sanction (expires_at)
    WHERE type = 'mute' AND lifted_at IS NULL AND expires_at IS NOT NULL;
-- History listings.
CREATE INDEX idx_sanction_history ON sanction (community_id, issued_at DESC);

-- Appeals: one open appeal per sanction, decided at most once.
CREATE TABLE appeal (
    id               uuid PRIMARY KEY,
    sanction_id      uuid NOT NULL REFERENCES sanction(id),
    community_id     uuid NOT NULL,
    statement        text NOT NULL CHECK (char_length(statement) BETWEEN 1 AND 2000),
    status           text NOT NULL CHECK (status IN ('submitted','upheld','overturned')),
    reviewer_user_id uuid,
    decision_note    text CHECK (char_length(decision_note) <= 2000),
    created_at       timestamptz NOT NULL DEFAULT now(),
    decided_at       timestamptz,
    CONSTRAINT appeal_decision_consistency CHECK (
        (status = 'submitted' AND decided_at IS NULL)
        OR (status IN ('upheld','overturned') AND reviewer_user_id IS NOT NULL AND decided_at IS NOT NULL)
    )
);
CREATE UNIQUE INDEX uq_appeal_open ON appeal (sanction_id) WHERE status = 'submitted';
CREATE INDEX idx_appeal_community_status ON appeal (community_id, status, created_at);
CREATE INDEX idx_appeal_appellant ON appeal (community_id, appellant_user_id, created_at);

-- Automod rule sets: versioned, append-only; one current version per community.
CREATE TABLE automod_rule_set (
    community_id     uuid NOT NULL,
    rule_set_version integer NOT NULL CHECK (rule_set_version >= 1),
    spam             jsonb NOT NULL,               -- AutomodSpamRules shape, validated in the service
    banned_words     jsonb NOT NULL,               -- AutomodBannedWords shape, validated in the service
    exempt_role_ids  uuid[] NOT NULL DEFAULT '{}',
    is_current       boolean NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (community_id, rule_set_version)
);
CREATE UNIQUE INDEX uq_automod_current ON automod_rule_set (community_id) WHERE is_current;

-- Audit log: append-only (INSERT only; DELETE reserved for the retention purge).
CREATE TABLE audit_log_entry (
    id             uuid PRIMARY KEY,
    community_id   uuid NOT NULL,
    actor_user_id  uuid,                           -- NULL for automatic actions (expiry sweep)
    target_user_id uuid,
    action         text NOT NULL CHECK (action IN
                     ('sanction_issued','sanction_lifted','sanction_expired','appeal_decided','automod_config_changed')),
    reason         text CHECK (char_length(reason) <= 1000),
    detail         jsonb,                          -- e.g. {"sanction_id": "...", "source": "appeal"} / {"rule_set_version": 4}
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_log_read ON audit_log_entry (community_id, created_at DESC);
CREATE INDEX idx_audit_log_actor ON audit_log_entry (community_id, actor_user_id, created_at DESC);

-- Transactional outbox: events committed with the state change they describe,
-- relayed to Kafka by a background dispatcher (see ADR-003).
CREATE TABLE outbox_event (
    id           uuid PRIMARY KEY,                 -- equals envelope.event_id
    topic        text NOT NULL,                    -- always 'moderation_events'
    message_key  text NOT NULL,                    -- community_id
    envelope     jsonb NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX idx_outbox_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;
```

Relationships: `appeal.sanction_id → sanction.id` (one open appeal per sanction, decision history preserved); `automod_rule_set` is versioned per community with exactly one `is_current` row; `audit_log_entry` is standalone, written in the same transaction as the action it records; `outbox_event` is the relay buffer for all three event contracts.

## 6. Stateless Container Operational Model, Failure Modes & Resilience

- **Stateless:** no local disk or in-memory state survives a request; every replica is interchangeable; all state lives in PostgreSQL, Kafka, or the request itself. Multiple replicas run concurrently against shared PostgreSQL.
- **Probes:** `GET /healthz` is liveness-only (process responsive; must not fail on transient dependency hiccups). `GET /readyz` verifies PostgreSQL connectivity and Kafka producer availability and returns `503` once shutdown begins.
- **Graceful shutdown:** on `SIGTERM`/`SIGINT` — flip readiness to failing, stop accepting HTTP requests, finish in-flight requests and in-flight outbox dispatch batches within a bounded timeout, close the database pool and Kafka producer, exit 0.
- **Authorization failure (fail closed):** API-001 uses a 2 s timeout with at most one retry on `5xx`/timeout. If the check still fails, the action returns `503 authorization_unavailable`; no cache or fallback exists (ADR-001).
- **Event publication:** events are appended to the outbox in the same transaction as the state change and relayed asynchronously (ADR-003). The dispatcher retries with backoff; a crash after commit but before relay is healed because unpublished outbox rows are picked up by any replica. Consumers deduplicate on `envelope.event_id`, so re-relayed duplicates are safe.
- **Expiry sweep concurrency:** due mutes are selected with `FOR UPDATE SKIP LOCKED` and processed in batches, so concurrent replicas never lift the same mute twice.
- **Retention purge:** batched deletes with a row limit per iteration to keep the daily job short; the last-success timestamp is exported as a metric.
- **Idempotency:** `Idempotency-Key` on creation endpoints a client may retry (sanction issue, appeal submission, appeal decision, automod PUT) — the first accepted result is returned on retry instead of duplicating.
- **Kafka delivery:** producer with idempotence enabled; topic partitioned by `community_id` to preserve per-community ordering.

## 7. Observability

Business metrics (low-cardinality labels only — never user, community, or sanction ids):

- `moderation_sanctions_total{type,source}` — `type ∈ {mute,kick,ban}`, `source ∈ {moderator,expiry,appeal}`.
- `moderation_appeals_total{status}` — `status ∈ {submitted,upheld,overturned}`.
- `moderation_appeal_decision_seconds` — histogram from appeal submission to decision.
- `moderation_automod_config_updates_total`.
- `moderation_audit_entries_purged_total` and `moderation_retention_job_last_success_timestamp`.

Technical metrics:

- RED metrics on all routes: `http_requests_total{route,method,status_class}`, `http_request_duration_seconds{route,method}`.
- API-001 client calls: `external_call_duration_seconds{provider="community_service",operation="authorization-checks"}` and `external_call_errors_total{provider="community_service",operation="authorization-checks",error_class}`.
- `kafka_producer_errors_total{topic}` on the relay.

SLOs: sanction endpoint p95 < 250 ms (including API-001); 5xx rate < 0.1 %; API-001 call p95 < 50 ms; retention purge succeeds daily.

Tracing: `traceparent` / `X-Trace-Id` accepted on every HTTP request and propagated to the API-001 call; the `trace_id` travels in the event envelope for every published event; scheduled jobs mint a fresh `trace_id` at the edge of the flow. Structured JSON logging carries the `trace_id` on every log line.

