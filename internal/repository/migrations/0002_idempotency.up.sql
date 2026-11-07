-- Idempotency-Key replay storage (US-015). architecture.md §5 has no table for
-- it, and a stateless replica cannot remember keys in memory, so the first
-- accepted response of a keyed write is stored here in the same transaction as
-- the write itself.
CREATE TABLE idempotency_key (
    scope           text NOT NULL,                 -- endpoint + resource scope
    idempotency_key text NOT NULL,
    request_hash    text NOT NULL,                 -- sha256 of the normalized request
    response_status integer NOT NULL,
    response_body   jsonb NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, idempotency_key)
);
CREATE INDEX idx_idempotency_created_at ON idempotency_key (created_at);
