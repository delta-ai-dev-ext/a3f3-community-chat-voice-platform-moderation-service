# ADR-001: Authorize every moderator action in real time via community_service, failing closed

## Status
Accepted

## Date
2026-01-14

## Context
`moderation_service` issues sanctions with real consequences: muted members lose their voice, banned members lose the community. The authorization model (community-level role permissions plus a role-position hierarchy that decides whether an actor may act on a target) is owned by `community_service`. Moderation endpoints are low-rate compared to the platform's hot paths, so a synchronous authorization call per action is affordable.

Options considered:
1. **Copy role/permission data into moderation and evaluate locally** — risks drift the moment a role changes (the very moment an authorization is revoked), duplicates the hierarchy logic.
2. **Cache authorization results with a TTL** — a revoked moderator could act for up to the TTL; unacceptable for punitive actions.
3. **Real-time check per action, failing closed when the provider is unavailable** — always correct at the moment of action.

## Decision
Every moderator action (sanction issue/lift, appeal decision, automod configuration read/write, audit-log read) performs a real-time API-001 call to `community_service` before any state change. On provider unavailability (timeout after one retry, `5xx`), the action fails with `503 authorization_unavailable` — no cache, no fallback, no degraded mode. Member-facing `/me/*` endpoints require no authorization call because they only ever expose the caller's own rows.

## Consequences
- Sanction issuance depends on `community_service` availability; during an outage moderation is blocked rather than unsafe (explicit product trade-off: no unauthorized sanctions).
- The sanction endpoint latency includes the API-001 round trip (p95 target < 250 ms).
- A denied check is not an error — API-001 returns `200 allowed: false` with a `denial_reason`, which the service maps to `403` error codes.
