# moderation_service — User Stories

Behavioral stories for the moderation domain (sanctions, appeals, automod, audit log), derived from the system architecture. Every story is independently testable.

---

## US-001 — Issue a mute

**As a** community moderator holding `mute_members`
**I want** to mute a member for a duration or indefinitely
**So that** the member stops disrupting the community without losing access entirely.

**Acceptance criteria**
- `POST /communities/{community_id}/sanctions` with `type = mute`, optional `duration_seconds` and `reason` returns `201` with the sanction (`active`, `expires_at` set only when a duration was given).
- Before the write, an authorization check confirms the actor holds `mute_members` and outranks the target.
- On success a `member_sanction_changed` event (`action = mute`) and a `sanction_issued` audit entry are committed in the same transaction as the sanction.
- Indefinite mutes have `expires_at = null`.

## US-002 — Issue a kick

**As a** community moderator holding `kick_members`
**I want** to remove a member immediately
**So that** severe disruption is ended at once.

**Acceptance criteria**
- `type = kick` returns `201` with a sanction that has `expires_at = null`.
- If `duration_seconds` is provided the request fails with `422 kick_no_duration`.
- On success a `member_sanction_changed` event (`action = kick`) and a `sanction_issued` audit entry are committed.

## US-003 — Issue a ban

**As a** community moderator holding `ban_members`
**I want** to permanently ban a member
**So that** they can no longer participate in the community.

**Acceptance criteria**
- `type = ban` returns `201` with a sanction that has `expires_at = null` (ban duration is ignored with `422 ban_no_duration` if provided).
- The event carries `action = ban` with `source = moderator` and the stored reason.

## US-004 — Authorization denials

**As a** community owner
**I want** moderators to be blocked from sanctioning me, the community owner, or themselves
**So that** the role hierarchy is never bypassed.

**Acceptance criteria**
- If the actor lacks the required permission → `403 insufficient_permission`.
- If the target outranks the actor → `403 target_outranks_actor`.
- If the target is the community owner → `403 target_is_owner`.
- If the target is the actor → `403 target_is_self`.
- No sanction, event, or audit entry is written on denial.

## US-005 — Fail closed when authorization is unavailable

**As a** platform operator
**I want** moderation actions to fail safely when the authorization service is unreachable
**So that** no unauthorized sanction can ever be issued.

**Acceptance criteria**
- When the API-001 call times out (2 s, one retry) or returns `5xx`, the endpoint returns `503` with `error = authorization_unavailable`.
- No state change, event, or audit entry occurs.
- `/readyz` is unaffected by the authorization check (it covers PostgreSQL and Kafka only).

## US-006 — Lift a sanction (unmute / unban)

**As a** community moderator with the permission matching the sanction type
**I want** to lift an active mute or ban with a reason
**So that** a member can rejoin the conversation early or re-enter the community.

**Acceptance criteria**
- `POST .../sanctions/{sanction_id}/lift` marks the sanction lifted with `lift_source = moderator` and the optional reason; returns the updated sanction.
- A `member_sanction_changed` event (`unmute`/`unban`, `source = moderator`) and a `sanction_lifted` audit entry are committed.
- Lifting a kick fails with `409 kick_not_liftable`.
- Lifting an already lifted/expired sanction fails with `409 sanction_not_active`.
- The actor must hold `mute_members` (mutes) or `ban_members` (bans) and outrank the target.

## US-007 — Timed mutes expire automatically

**As a** muted member
**I want** my mute to end exactly when its duration elapses
**So that** I can speak again without moderator intervention.

**Acceptance criteria**
- A scheduled sweep lifts mutes whose `expires_at` has passed with `lift_source = expiry`, emitting an `unmute` event with `source = expiry`, `actor_user_id = null`, and a `sanction_expired` audit entry.
- Concurrent replicas never lift the same mute twice (row-level locking in the sweep).
- Sanctions listed as `expired` reflect the computed view; a swept mute is `lifted` with `lift_source = expiry`.

## US-008 — Member sees own sanctions

**As a** sanctioned member
**I want** to see my active sanctions with their reason and end time
**So that** I understand why I cannot post or participate.

**Acceptance criteria**
- `GET /me/sanctions` returns the caller's active sanctions across all communities, including bans in communities the caller no longer belongs to.
- Each item includes `type`, `reason`, `issued_at`, `expires_at`, `appealable`, and `open_appeal_id` when an appeal is open.
- Only the caller's own sanctions are ever returned; no authorization call is needed.

## US-009 — Submit an appeal

**As a** muted or banned member
**I want** to submit one appeal for my active sanction with a written statement
**So that** a moderator can review my case.

**Acceptance criteria**
- `POST /sanctions/{sanction_id}/appeals` with `statement` (1–2000 chars) returns `201` with the appeal (`status = submitted`) and emits `appeal_status_changed` (`submitted`).
- Only the sanction's target may appeal (`403 not_sanction_target` otherwise).
- Only active mutes and bans are appealable (`409 sanction_not_active` for lifts/expiry, `422 sanction_not_appealable` for kicks).
- A second open appeal for the same sanction fails with `409 appeal_already_open`.

## US-010 — Decide an appeal

**As a** community moderator holding `review_appeals`
**I want** to uphold or overturn an open appeal with a decision note
**So that** wrongly applied sanctions are corrected.

**Acceptance criteria**
- `POST .../appeals/{appeal_id}/decision` with `decision` (`upheld`/`overturned`) returns the decided appeal (`reviewer_user_id`, `decided_at` set).
- `upheld` keeps the sanction active.
- `overturned` lifts the sanction with `lift_source = appeal` and emits both the `member_sanction_changed` (`unmute`/`unban`, `source = appeal`) and `appeal_status_changed` (`overturned`) events in the same transaction.
- Deciding an already decided appeal fails with `409 appeal_already_decided`.
- An `appeal_decided` audit entry (plus `sanction_lifted` on overturn) is appended.

## US-011 — Moderators review the appeal queue

**As a** community moderator holding `review_appeals`
**I want** to list the community's appeals filtered by status
**So that** I can work through pending reviews.

**Acceptance criteria**
- `GET /communities/{community_id}/appeals?status=submitted` returns appeals newest-first with cursor pagination.
- Requires `review_appeals`; callers without it receive `403 insufficient_permission`.

## US-012 — Configure community automod rules

**As a** community admin holding `manage_automod`
**I want** to set spam thresholds, banned words, their actions, and exempt roles
**So that** obvious spam and banned language is handled automatically.

**Acceptance criteria**
- `GET .../automod` returns the current rule set (`404` when none is configured yet); `PUT` validates the full configuration and stores it as `max(rule_set_version) + 1`, returning the new version.
- The PUT emits an `automod_rules_changed` full-snapshot event and appends an `automod_config_changed` audit entry.
- An invalid configuration returns `422` and leaves the previous version current and current.
- The caller must hold `manage_automod` for both operations.

## US-013 — Read the community audit log

**As a** community moderator holding `view_audit_log`
**I want** to browse who did what, when, and why over the past year
**So that** moderation decisions remain transparent and accountable.

**Acceptance criteria**
- `GET .../audit-log` returns entries newest-first with cursor pagination, filtered to the last 365 days.
- Optional filters: `action_type` and `actor`.
- Automatic actions (mute expiry) appear with `actor_user_id = null`.

## US-014 — Audit retention purges after one year

**As a** platform operator
**I want** audit entries older than one year to be permanently deleted daily
**So that** the data-retention commitment is met without manual work.

**Acceptance criteria**
- A daily job deletes audit entries older than 365 days in bounded batches and exports `moderation_audit_entries_purged_total` and `moderation_retention_job_last_success_timestamp`.
- Audit-log reads never return entries older than 365 days, even if the purge has not yet run.
- The audit log is never updated — only inserted and purged.

## US-015 — Retried writes do not duplicate sanctions or appeals

**As a** client of the API
**I want** an `Idempotency-Key` on creation endpoints
**So that** a network retry cannot issue the same sanction or appeal twice.

**Acceptance criteria**
- Sanction issue, appeal submission, appeal decision, and automod PUT honor `Idempotency-Key`: replaying the same key returns the first accepted result instead of creating a duplicate.
- Keys may be reused across different communities/users.
