-- v11 -> v12 (compatible with v2+): Preserve contents-read updates before message mapping

CREATE TABLE IF NOT EXISTS telegram_round_contents_read (
    bridge_id TEXT NOT NULL,
    login_id TEXT NOT NULL,
    remote_message_id TEXT NOT NULL,
    played_at_ms BIGINT NOT NULL,
    PRIMARY KEY (bridge_id, login_id, remote_message_id)
);
