package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// RememberContentsRead preserves an authenticated Telegram update even when
// its preceding message has not finished being bridged yet.
func (q *RoundLedger) RememberContentsRead(ctx context.Context, bridgeID, loginID, remoteID string) error {
	_, err := q.DB.Exec(ctx, `INSERT INTO telegram_round_contents_read (bridge_id,login_id,remote_message_id,played_at_ms) VALUES ($1,$2,$3,$4) ON CONFLICT (bridge_id,login_id,remote_message_id) DO NOTHING`, bridgeID, loginID, remoteID, time.Now().UnixMilli())
	return err
}

func (q *RoundLedger) WasContentsRead(ctx context.Context, bridgeID, loginID, remoteID string) (bool, error) {
	var stamp int64
	err := q.DB.QueryRow(ctx, `SELECT played_at_ms FROM telegram_round_contents_read WHERE bridge_id=$1 AND login_id=$2 AND remote_message_id=$3`, bridgeID, loginID, remoteID).Scan(&stamp)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return stamp > 0, err
}
