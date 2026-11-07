# ADR-003: Transactional outbox for moderation_events publication

## Status
Accepted

## Date
2026-01-14

## Context
A sanctioned member must actually stop being able to post: `community_service` applies `member_sanction_changed` (EVT-004) to member state, `messaging_service` reads mute state and automod rules from events, and `notification_service` sends the moderation notice. If a sanction is committed to PostgreSQL but the Kafka publish fails (process crash between commit and publish, broker unavailable), the mute would silently not be enforced — the worst possible failure mode for a moderation system.

Options considered:
1. **Publish directly to Kafka after the DB commit** — simple, but a crash in the window between commit and publish loses the event permanently.
2. **Publish before the commit** — could notify downstream consumers of a sanction that is then rolled back.
3. **Transactional outbox** — the event row is written in the same transaction as the state change; a background dispatcher relays unpublished rows to Kafka with retries.

## Decision
Every event (`member_sanction_changed`, `automod_rules_changed`, `appeal_status_changed`) is written to an `outbox_event` table in the same database transaction as the state change (and the audit entry). A background dispatcher in each replica polls unpublished rows in `created_at` order, publishes them to the `moderation_events` topic (key = `community_id`), and marks them published. Delivery is effectively at-least-once: dispatcher retries and multi-replica polling can produce duplicate sends, which is safe because consumers deduplicate on `envelope.event_id` per the platform delivery semantics.

## Consequences
- Event publication is delayed by at most the dispatcher poll interval instead of being lost.
- One extra table and a dispatcher loop per replica; outbox rows are pruned after successful publication plus a grace window.
- Envelope construction is centralized in the dispatcher/publisher so the mandatory envelope (including a non-empty `trace_id`) cannot be skipped at any publish site.
