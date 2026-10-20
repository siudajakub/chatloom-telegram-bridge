package connector

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/connector/store"
)

func TestRoundSendLedgerCachesResponseAndBindsSilentOptions(t *testing.T) {
	ctx := context.Background()
	raw, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "synthetic.db"))
	require.NoError(t, err)
	defer raw.Close()
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	require.NoError(t, err)
	network := &TelegramConnector{Bridge: &bridgev2.Bridge{ID: "synthetic-bridge"}, Store: store.NewStore(db, dbutil.NoopLogger)}
	// Store migration v10 rewrites bridgev2 message IDs; isolated store tests need the table.
	_, err = db.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS message (id TEXT, room_id TEXT)")
	require.NoError(t, err)
	require.NoError(t, network.Store.Upgrade(ctx))
	client := &TelegramClient{main: network, loginID: "synthetic-login"} // no API exists
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "user:777", Receiver: "synthetic-login"}, MXID: "!synthetic-round:example.invalid"}}
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{Portal: portal, Event: &event.Event{ID: "$synthetic-round-send", Type: event.EventMessage, RoomID: portal.MXID, Content: event.Content{Raw: map[string]any{roundSendOptionsKey: map[string]any{"version": 1, "silent": false, "schedule_date": 0}}}}, Content: &event.MessageEventContent{MsgType: event.MsgVideo, Body: "synthetic-round.mp4", URL: "mxc://example.invalid/synthetic", Info: &event.FileInfo{MimeType: "video/mp4", Extra: map[string]any{"fi.mau.telegram.round_message": true}}}}}
	op, cached, err := client.BeginRoundSend(ctx, msg)
	require.NoError(t, err)
	require.Nil(t, cached)
	require.NotZero(t, op.RandomID)
	require.NoError(t, client.ClaimRoundSend(ctx, op))
	response := &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: "42", Room: portal.PortalKey, SenderID: "synthetic-login", Timestamp: time.Now(), Metadata: &MessageMetadata{IsRound: true, ProtectionKnown: true, ContentHash: []byte("synthetic-hash")}}}
	require.NoError(t, client.FinishRoundSend(ctx, op, response, nil, true))
	retry, cached, err := client.BeginRoundSend(ctx, msg)
	require.NoError(t, err)
	require.Equal(t, op.RandomID, retry.RandomID)
	require.NotNil(t, cached)
	require.Equal(t, response.DB.ID, cached.DB.ID)
	require.True(t, cached.DB.Metadata.(*MessageMetadata).IsRound)
	require.Equal(t, []byte("synthetic-hash"), cached.DB.Metadata.(*MessageMetadata).ContentHash)
	msg.Event.Content.Raw[roundSendOptionsKey].(map[string]any)["silent"] = true
	_, _, err = client.BeginRoundSend(ctx, msg)
	require.ErrorIs(t, err, store.ErrRoundIdentityChanged)
	msg.Event.Content.Raw[roundSendOptionsKey].(map[string]any)["silent"] = false
	msg.Content.URL = "mxc://example.invalid/changed-source"
	_, _, err = client.BeginRoundSend(ctx, msg)
	require.ErrorIs(t, err, store.ErrRoundIdentityChanged)
}
