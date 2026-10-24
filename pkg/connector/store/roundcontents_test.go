package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContentsReadSurvivesMissingMappingRestartAndAccountSeparation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "contents.db")
	q, raw := openRoundLedgerTestDB(t, path)
	require.NoError(t, q.RememberContentsRead(ctx, "bridge", "login-a", "555.42"))
	require.NoError(t, q.RememberContentsRead(ctx, "bridge", "login-a", "555.42"))
	require.NoError(t, raw.Close())
	q, raw = openRoundLedgerTestDB(t, path)
	defer raw.Close()
	read, err := q.WasContentsRead(ctx, "bridge", "login-a", "555.42")
	require.NoError(t, err)
	require.True(t, read)
	for _, key := range []struct{ bridge, login, message string }{{"bridge", "login-b", "555.42"}, {"other", "login-a", "555.42"}, {"bridge", "login-a", "666.42"}} {
		read, err = q.WasContentsRead(ctx, key.bridge, key.login, key.message)
		require.NoError(t, err)
		require.False(t, read)
	}
}

func TestPruneFinishedKeepsOpenOperations(t *testing.T) {
	ctx := context.Background()
	q, raw := openRoundLedgerTestDB(t, filepath.Join(t.TempDir(), "prune.db"))
	defer raw.Close()
	old := time.Now().Add(-RoundLedgerRetention - time.Hour).UnixMilli()
	require.NoError(t, q.RememberContentsRead(ctx, "bridge", "login", "old"))
	require.NoError(t, q.RememberContentsRead(ctx, "bridge", "login", "new"))
	_, err := raw.Exec(`UPDATE telegram_round_contents_read SET played_at_ms=? WHERE remote_message_id='old'`, old)
	require.NoError(t, err)
	for i, status := range []string{"accepted", "permanent", "uncertain", "dispatching", "retry", "prepared", "accepted"} {
		updated := old
		if i == 6 {
			updated = time.Now().UnixMilli()
		}
		_, err = raw.Exec(`INSERT INTO telegram_round_operation (bridge_id,login_id,portal_id,portal_receiver,action,request_id,event_id,media_identity,payload_hash,random_id,status,updated_at_ms) VALUES ('bridge','login','portal','', 'round_send', ?, ?, 'm', 'h', 1, ?, ?)`, fmt.Sprintf("r%d", i), fmt.Sprintf("$e%d", i), status, updated)
		require.NoError(t, err)
	}
	contents, operations, err := q.PruneFinished(ctx, time.Now().Add(-RoundLedgerRetention))
	require.NoError(t, err)
	require.EqualValues(t, 1, contents)
	require.EqualValues(t, 2, operations)
	read, err := q.WasContentsRead(ctx, "bridge", "login", "new")
	require.NoError(t, err)
	require.True(t, read)
	var left int
	require.NoError(t, raw.QueryRow(`SELECT COUNT(*) FROM telegram_round_operation`).Scan(&left))
	require.Equal(t, 5, left)
}
