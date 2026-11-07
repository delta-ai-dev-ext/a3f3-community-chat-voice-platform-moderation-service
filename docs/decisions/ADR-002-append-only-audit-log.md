# ADR-002: Append-only audit log with one-year retention and a daily purge job

## Status
Accepted

## Date
2026-01-14

## Context
The platform commits to a one-year audit log: every moderator action (sanction issued/lifted/expired, appeal decided, automod configuration changed) must be recorded immutably with actor, target, action, reason, and timestamp, visible to members with `view_audit_log`, and deleted after one year.

Options considered:
1. **Mutable table with soft-delete flags** — undermines the immutability guarantee; an entry could be silently altered.
2. **Append-only table + daily batched DELETE job** — simple, satisfies immutability (no UPDATE path) and retention without extra infrastructure.
3. **Event-sourced log on Kafka with a compaction topic** — Kafka is not the right store for a queryable, permission-filtered, cursor-paginated audit history, and retention semantics would be implicit.

## Decision
Audit entries live in a dedicated `audit_log_entry` PostgreSQL table. The application layer only ever INSERTs; UPDATE and DELETE rights are withheld from the service database role, except the retention purge job. Reads are filtered to `created_at >= now() - 365 days` so the retention window is correct even before the job runs. A daily scheduled job deletes in bounded batches and exports `moderation_audit_entries_purged_total` and `moderation_retention_job_last_success_timestamp`.

## Consequences
- Entries older than a year are unrecoverable by design — this is the stated data-retention commitment.
- Every moderator action must write its audit entry in the same database transaction as the state change it records, so an accepted action can never lack its audit entry.
- No audit entry is emitted when the action itself failed (authorization denial, validation failure).
