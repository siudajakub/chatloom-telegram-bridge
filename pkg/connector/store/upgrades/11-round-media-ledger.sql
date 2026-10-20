-- v10 -> v11 (compatible with v2+): Durable round media operations and playback state

CREATE TABLE telegram_round_operation (
    bridge_id TEXT NOT NULL,
    login_id TEXT NOT NULL,
    portal_id TEXT NOT NULL,
    portal_receiver TEXT NOT NULL,
    action TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    media_identity TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    random_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    result TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT '',
    claim_token TEXT NOT NULL DEFAULT '',
    lease_until_ms BIGINT NOT NULL DEFAULT 0,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (bridge_id, login_id, portal_id, portal_receiver, action, request_id),
    UNIQUE (bridge_id, login_id, action, request_id),
    UNIQUE (bridge_id, login_id, action, event_id)
);

CREATE TABLE telegram_round_media_state (
    bridge_id TEXT NOT NULL,
    login_id TEXT NOT NULL,
    portal_id TEXT NOT NULL,
    portal_receiver TEXT NOT NULL,
    event_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    media_unread BOOLEAN,
    played_at_ms BIGINT NOT NULL DEFAULT 0,
    expired BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (bridge_id, login_id, portal_id, portal_receiver, event_id)
);
