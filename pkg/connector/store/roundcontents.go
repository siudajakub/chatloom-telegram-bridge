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

// RoundLedgerRetention is how long finished round operations and remembered
// contents-read IDs are kept (wave 5 F11). Older rows can no longer influence
// a send retry, a control or a history import in a useful way.
const RoundLedgerRetention = 30 * 24 * time.Hour

// PruneFinished deletes contents-read rows and finished (accepted or
// permanent) operations older than the cutoff. Prepared, retry, uncertain and
// dispatching operations are always kept, and so is media state. It runs once
// at start-up; there is no ticker.
func (q *RoundLedger) PruneFinished(ctx context.Context, cutoff time.Time) (contents, operations int64, err error) {
	cutoffMS := cutoff.UnixMilli()
	res, err := q.DB.Exec(ctx, `DELETE FROM telegram_round_contents_read WHERE played_at_ms < $1`, cutoffMS)
	if err != nil {
		return 0, 0, err
	}
	contents, _ = res.RowsAffected()
	res, err = q.DB.Exec(ctx, `DELETE FROM telegram_round_operation WHERE status IN ('accepted','permanent') AND updated_at_ms < $1`, cutoffMS)
	if err != nil {
		return contents, 0, err
	}
	operations, _ = res.RowsAffected()
	return contents, operations, nil
}
