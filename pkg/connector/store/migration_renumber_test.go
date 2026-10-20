package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
)

func openRenumberTestDB(t *testing.T, path string) (*Container, *sql.DB) {
	t.Helper()
	raw, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	require.NoError(t, err)
	return NewStore(db, dbutil.NoopLogger), raw
}

func messageIDs(t *testing.T, raw *sql.DB) map[string]string {
	t.Helper()
	rows, err := raw.Query("SELECT id, room_id FROM message")
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var msgID, roomID string
		require.NoError(t, rows.Scan(&msgID, &roomID))
		out[msgID] = roomID
	}
	require.NoError(t, rows.Err())
	return out
}

func storeVersion(t *testing.T, raw *sql.DB) int {
	t.Helper()
	var version int
	require.NoError(t, raw.QueryRow("SELECT version FROM telegram_version").Scan(&version))
	return version
}

// A database migrated by the pre-v26.10 round fork is at v11 with the round
// tables present but without upstream v10 applied.
func TestRoundForkV11DatabaseUpgradesAfterRenumbering(t *testing.T) {
	ctx := context.Background()
	c, raw := openRenumberTestDB(t, filepath.Join(t.TempDir(), "bridge.db"))
	_, err := raw.Exec("CREATE TABLE message (id TEXT NOT NULL, room_id TEXT NOT NULL)")
	require.NoError(t, err)
	require.NoError(t, c.Upgrade(ctx))
	require.Equal(t, 13, storeVersion(t, raw))

	ledger := NewRoundLedger(c.Database)
	require.NoError(t, ledger.RememberContentsRead(ctx, "bridge", "login", "5.10"))

	// Rewind to the old fork's state.
	_, err = raw.Exec("UPDATE telegram_version SET version=11")
	require.NoError(t, err)
	_, err = raw.Exec(`INSERT INTO message (id, room_id) VALUES
		('100', 'channel:5'), ('5.101', 'channel:5'), ('102', 'user:7'),
		('migrated:103:1', 'channel:5'), ('104', 'chat:9')`)
	require.NoError(t, err)

	require.NoError(t, c.Upgrade(ctx))
	require.Equal(t, 13, storeVersion(t, raw))

	ids := messageIDs(t, raw)
	require.Len(t, ids, 5)
	require.Contains(t, ids, "5.101")
	require.Contains(t, ids, "102")
	require.Contains(t, ids, "104")
	require.Contains(t, ids, "migrated:103:1")
	require.NotContains(t, ids, "100")
	var namespaced int
	require.NoError(t, raw.QueryRow("SELECT COUNT(*) FROM message WHERE id LIKE 'migrated:100:%'").Scan(&namespaced))
	require.Equal(t, 1, namespaced)

	read, err := ledger.WasContentsRead(ctx, "bridge", "login", "5.10")
	require.NoError(t, err)
	require.True(t, read)

	// Running again must not touch already namespaced or regular IDs.
	_, err = raw.Exec("UPDATE telegram_version SET version=12")
	require.NoError(t, err)
	require.NoError(t, c.Upgrade(ctx))
	require.Equal(t, ids, messageIDs(t, raw))

	var integrity string
	require.NoError(t, raw.QueryRow("PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
}
