package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A sticker cached as raw WebM before conversion was enabled is replaced by
// its converted file instead of failing on the primary key.
func TestTelegramFileInsertReplacesStaleEntry(t *testing.T) {
	ctx := context.Background()
	c, raw := openRenumberTestDB(t, filepath.Join(t.TempDir(), "bridge.db"))
	// The bridgev2 tables the store migrations touch.
	_, err := raw.Exec("CREATE TABLE message (id TEXT NOT NULL, room_id TEXT NOT NULL)")
	require.NoError(t, err)
	require.NoError(t, c.Upgrade(ctx))

	stale := &TelegramFile{LocationID: "42", MXC: "mxc://example.invalid/webm", MIMEType: "video/webm", Size: 10, Width: 512, Height: 512, Timestamp: time.Unix(1, 0)}
	require.NoError(t, c.TelegramFile.Insert(ctx, stale))
	fresh := &TelegramFile{LocationID: "42", MXC: "mxc://example.invalid/webp", MIMEType: "image/webp", Size: 20, Width: 256, Height: 256, Timestamp: time.Unix(2, 0)}
	require.NoError(t, c.TelegramFile.Insert(ctx, fresh))

	got, err := c.TelegramFile.GetByLocationID(ctx, "42")
	require.NoError(t, err)
	require.Equal(t, fresh.MXC, got.MXC)
	require.Equal(t, "image/webp", got.MIMEType)
	require.Equal(t, 256, got.Width)
}
