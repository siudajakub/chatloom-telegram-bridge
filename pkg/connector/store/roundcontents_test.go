package store

import (
	"context"
	"path/filepath"
	"testing"

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
