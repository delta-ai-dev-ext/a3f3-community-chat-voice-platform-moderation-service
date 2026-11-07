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
    -- architecture.md §5 declares idx_appeal_appellant (community_id,
    -- appellant_user_id, created_at) below but omits the column itself; the
    -- appellant is always the sanction target and the API returns it.
    appellant_user_id uuid NOT NULL,
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
