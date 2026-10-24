package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exsync"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

type mediaRoundTripCryptoFixture struct {
	matrix.Crypto
	decrypted *event.Event
	mu        sync.Mutex
	states    []RoundMediaStateContent
}

func (f *mediaRoundTripCryptoFixture) Decrypt(context.Context, *event.Event) (*event.Event, error) {
	return f.decrypted, nil
}

func (f *mediaRoundTripCryptoFixture) Encrypt(_ context.Context, roomID id.RoomID, typ event.Type, content *event.Content) error {
	if typ != MediaStateEventType {
		return fmt.Errorf("unexpected plaintext type %s", typ)
	}
	parsed, ok := content.Parsed.(*RoundMediaStateContent)
	if !ok || parsed.RoomID != roomID {
		return fmt.Errorf("invalid reverse media state")
	}
	f.mu.Lock()
	f.states = append(f.states, *parsed)
	f.mu.Unlock()
	content.Parsed = nil
	content.Raw = map[string]any{"algorithm": id.AlgorithmMegolmV1, "ciphertext": "synthetic-not-real-ciphertext"}
	return nil
}

type mediaMTProtoFixture struct {
	getCalls, contentsCalls, forwardCalls atomic.Int64
	remote                                *tg.Message
	forward                               *tg.MessagesForwardMessagesRequest
	forwardUpdates                        tg.UpdatesClass
}

func (s *mediaMTProtoFixture) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	// Exercise the generated TL encoder/decoder as well as method selection.
	encoded := &bin.Buffer{}
	if err := input.Encode(encoded); err != nil {
		return err
	}
	switch req := input.(type) {
	case *tg.MessagesGetMessagesRequest:
		if len(req.ID) != 1 || req.ID[0].(*tg.InputMessageID).ID != s.remote.ID {
			return fmt.Errorf("wrong source ID")
		}
		s.getCalls.Add(1)
		output.(*tg.MessagesMessagesBox).Messages = &tg.MessagesMessages{Messages: []tg.MessageClass{s.remote}}
	case *tg.MessagesReadMessageContentsRequest:
		var wire tg.MessagesReadMessageContentsRequest
		if err := wire.Decode(encoded); err != nil {
			return err
		}
		if len(wire.ID) != 1 || wire.ID[0] != s.remote.ID {
			return fmt.Errorf("wrong contents-read ID")
		}
		s.contentsCalls.Add(1)
		*output.(*tg.MessagesAffectedMessages) = tg.MessagesAffectedMessages{Pts: 1, PtsCount: 1}
	case *tg.MessagesForwardMessagesRequest:
		var wire tg.MessagesForwardMessagesRequest
		if err := wire.Decode(encoded); err != nil {
			return err
		}
		s.forward = &wire
		s.forwardCalls.Add(1)
		output.(*tg.UpdatesBox).Updates = s.forwardUpdates
	default:
		return fmt.Errorf("forbidden RPC type %T", input)
	}
	return nil
}

func TestMediaControlAuthorizedEncryptedQueueRPCAndDurableDedup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ordinarySends, encryptedSends atomic.Int64
	homeserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/send/m.room.message/") {
			ordinarySends.Add(1)
		}
		if strings.Contains(r.URL.Path, "/send/m.room.encrypted/") {
			encryptedSends.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"event_id":"$synthetic-state"}`))
	}))
	defer homeserver.Close()
	raw, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "synthetic.db")+"?_busy_timeout=5000")
	require.NoError(t, err)
	defer raw.Close()
	raw.SetMaxOpenConns(1)
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	require.NoError(t, err)
	cfg := &bridgeconfig.Config{}
	cfg.Homeserver.Domain = "example.invalid"
	cfg.Homeserver.Address = homeserver.URL
	cfg.AppService.Bot.Username = "synthetic-telegrambot"
	cfg.AppService.UsernameTemplate = "synthetic_ghost_{{.}}"
	cfg.Bridge.Permissions = bridgeconfig.PermissionConfig{"*": &bridgeconfig.PermissionLevelUser}
	cfg.Encryption.VerificationLevels.Send = id.TrustStateVerified
	mx := matrix.NewConnector(cfg)
	network := &TelegramConnector{}
	br := bridgev2.NewBridge("synthetic-bridge", db, zerolog.Nop(), &cfg.Bridge, mx, network, commands.NewProcessor)
	br.BackgroundCtx = ctx
	require.NoError(t, br.DB.Upgrade(ctx))
	require.NoError(t, network.Store.Upgrade(ctx))
	require.NoError(t, mx.StateStore.Upgrade(ctx))
	require.True(t, network.mediaControlsEnabled) // registered by connector Init
	cryptoFixture := &mediaRoundTripCryptoFixture{}
	mx.Crypto = cryptoFixture
	sender := id.UserID("@synthetic-user:example.invalid")
	user, err := br.GetUserByMXID(ctx, sender)
	require.NoError(t, err)
	rpc := &mediaMTProtoFixture{remote: syntheticRemoteRound(&tg.PeerUser{UserID: 777})}
	var client *TelegramClient
	login, err := user.NewLogin(ctx, &database.UserLogin{ID: "123", Metadata: &UserLoginMetadata{}}, &bridgev2.NewLoginParams{LoadUserLogin: func(_ context.Context, login *bridgev2.UserLogin) error {
		// Synthetic, unstarted client: its invocation middleware consumes every
		// request. No network transport or real credentials are initialized.
		tgClient := telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc { return rpc.Invoke })}})
		metadata := &UserLoginMetadata{Session: UserLoginSession{AuthKey: make([]byte, 256)}}
		client = &TelegramClient{main: network, userLogin: login, loginID: login.ID, userID: "123", telegramUserID: 123, metadata: metadata, client: tgClient, clientInitialized: exsync.NewEvent(), clientDone: exsync.NewEvent(), ScopedStore: network.Store.GetScopedStore(123)}
		client.clientInitialized.Set()
		login.Client = client
		return nil
	}})
	require.NoError(t, err)
	require.NoError(t, client.ScopedStore.SetAccessHash(ctx, ids.PeerTypeUser, 777, 99))
	key := networkid.PortalKey{ID: "user:777", Receiver: login.ID}
	portal, err := br.GetPortalByKey(ctx, key)
	require.NoError(t, err)
	portal.MXID = "!synthetic-round:example.invalid"
	portal.RoomType = database.RoomTypeDM
	require.NoError(t, br.DB.Portal.Update(ctx, portal.Portal))
	portal, err = br.GetPortalByMXID(ctx, portal.MXID)
	require.NoError(t, err)
	_, err = br.DB.UserPortal.GetOrCreate(ctx, login.UserLogin, portal.PortalKey)
	require.NoError(t, err)
	require.NoError(t, mx.StateStore.MarkRegistered(ctx, mx.Bot.UserID))
	require.NoError(t, mx.StateStore.SetMember(ctx, portal.MXID, sender, &event.MemberEventContent{Membership: event.MembershipJoin}))
	require.NoError(t, mx.StateStore.SetMember(ctx, portal.MXID, mx.Bot.UserID, &event.MemberEventContent{Membership: event.MembershipJoin}))
	require.NoError(t, mx.StateStore.SetPowerLevels(ctx, portal.MXID, &event.PowerLevelsEventContent{Users: map[id.UserID]int{sender: 100, mx.Bot.UserID: 100}}))
	require.NoError(t, mx.StateStore.SetEncryptionEvent(ctx, portal.MXID, &event.EncryptionEventContent{Algorithm: id.AlgorithmMegolmV1}))
	unread := true
	msg := &database.Message{ID: "42", MXID: "$synthetic-round", Room: portal.PortalKey, SenderID: "777", Timestamp: time.Now(), Metadata: &MessageMetadata{IsRound: true, ProtectionKnown: true, MediaUnread: &unread}}
	require.NoError(t, br.DB.Message.Insert(ctx, msg))
	dispatch := func(e *event.Event) {
		e.Mautrix.WasEncrypted = true
		e.Mautrix.TrustState = id.TrustStateVerified
		cryptoFixture.decrypted = e
		outer := &event.Event{Type: event.EventEncrypted, ID: e.ID, Sender: e.Sender, RoomID: e.RoomID, Timestamp: e.Timestamp, Content: event.Content{Parsed: &event.EncryptedEventContent{Algorithm: id.AlgorithmMegolmV1}}}
		mx.EventProcessor.Dispatch(ctx, outer)
	}
	control := syntheticControlEvent(time.Now())
	control.Content.Raw["source"].(map[string]any)["account_id"] = "123"
	dispatch(control)
	require.EqualValues(t, 1, rpc.getCalls.Load())
	require.EqualValues(t, 1, rpc.contentsCalls.Load())
	require.Zero(t, rpc.forwardCalls.Load())
	ledger := store.NewRoundLedger(network.Store.Database)
	operation, err := ledger.Get(ctx, client.operationKey(portal, MediaPlayedEventType.Type, "synthetic-play-1"))
	require.NoError(t, err)
	require.NotNil(t, operation)
	require.Equal(t, "accepted", operation.Status)
	require.NotZero(t, operation.RandomID)
	dispatch(control) // Matrix replay, stable request: no second remote RPC
	require.EqualValues(t, 1, rpc.getCalls.Load())
	require.EqualValues(t, 1, rpc.contentsCalls.Load())
	state, err := ledger.GetMediaState(ctx, string(br.ID), string(login.ID), string(portal.ID), string(portal.Receiver), string(msg.MXID))
	require.NoError(t, err)
	require.NotNil(t, state.MediaUnread)
	require.False(t, *state.MediaUnread)
	// Wave 5 F13: played publishes one media_state carrying the new state
	// and the result, the replay one more: two events, two revisions.
	require.EqualValues(t, 2, state.Revision)
	require.Zero(t, ordinarySends.Load())
	require.EqualValues(t, 2, encryptedSends.Load())
	cryptoFixture.mu.Lock()
	last := cryptoFixture.states[len(cryptoFixture.states)-1]
	cryptoFixture.mu.Unlock()
	require.Equal(t, msg.MXID, last.SourceEventID)
	require.Equal(t, login.ID, last.AccountID)
	require.Equal(t, "accepted", last.ControlResult.Status)
	previousRevision := state.Revision
	require.NoError(t, client.onRoundContentsRead(ctx, 0, []int{42}))
	state, err = ledger.GetMediaState(ctx, string(br.ID), string(login.ID), string(portal.ID), string(portal.Receiver), string(msg.MXID))
	require.NoError(t, err)
	require.Greater(t, state.Revision, previousRevision)
	require.False(t, *state.MediaUnread)
	require.EqualValues(t, 1, rpc.getCalls.Load())
	require.EqualValues(t, 1, rpc.contentsCalls.Load())
	cryptoFixture.mu.Lock()
	official := cryptoFixture.states[len(cryptoFixture.states)-1]
	cryptoFixture.mu.Unlock()
	require.Empty(t, official.RequestID)
	require.Nil(t, official.ControlResult)
	require.Equal(t, login.ID, official.AccountID)
	require.Equal(t, msg.MXID, official.SourceEventID)
	// A delayed live/backfill snapshot still claims unread. The persisted
	// official contents-read marker and state must take precedence after replay.
	require.True(t, rpc.remote.MediaUnread)
	client.initializeRoundMediaState(ctx, portal, rpc.remote)
	state, err = ledger.GetMediaState(ctx, string(br.ID), string(login.ID), string(portal.ID), string(portal.Receiver), string(msg.MXID))
	require.NoError(t, err)
	require.False(t, *state.MediaUnread)
	require.EqualValues(t, 1, rpc.getCalls.Load())
	require.EqualValues(t, 1, rpc.contentsCalls.Load())
	for i, mutate := range []func(*event.Event){
		func(e *event.Event) { e.Content.Raw["source"].(map[string]any)["account_id"] = "foreign-login" },
		func(e *event.Event) { e.Content.Raw["source"].(map[string]any)["event_id"] = "$foreign-source" },
		func(e *event.Event) {
			e.Content.Raw["source"].(map[string]any)["room_id"] = "!foreign-room:example.invalid"
		},
		func(e *event.Event) { e.Content.Raw["manual"] = false },
		func(e *event.Event) { e.Content.Raw["incognito"] = true },
	} {
		bad := syntheticControlEvent(time.Now())
		bad.ID = id.EventID(fmt.Sprintf("$synthetic-reject-%d", i))
		bad.Content.Raw["request_id"] = fmt.Sprintf("synthetic-reject-%d", i)
		bad.Content.Raw["source"].(map[string]any)["account_id"] = "123"
		mutate(bad)
		before := encryptedSends.Load()
		dispatch(bad)
		require.EqualValues(t, 1, rpc.getCalls.Load())
		require.EqualValues(t, 1, rpc.contentsCalls.Load())
		require.Equal(t, before, encryptedSends.Load()) // no source-state oracle
	}
	// Wave 5 F13: once the sender's own login and the source mapping are
	// verified, a rejection is published as a control_result instead of only
	// being logged. No Telegram RPC is made.
	ttl := &database.Message{ID: "43", MXID: "$synthetic-ttl", Room: portal.PortalKey, SenderID: "777", Timestamp: time.Now(), Metadata: &MessageMetadata{IsRound: true, ProtectionKnown: true, TTLSeconds: 30}}
	require.NoError(t, br.DB.Message.Insert(ctx, ttl))
	unavailable := syntheticControlEvent(time.Now())
	unavailable.ID = "$synthetic-unavailable"
	unavailable.Content.Raw["request_id"] = "synthetic-unavailable"
	unavailable.Content.Raw["source"].(map[string]any)["account_id"] = "123"
	unavailable.Content.Raw["source"].(map[string]any)["event_id"] = "$synthetic-ttl"
	before := encryptedSends.Load()
	dispatch(unavailable)
	require.Equal(t, before+1, encryptedSends.Load())
	require.EqualValues(t, 1, rpc.getCalls.Load())
	cryptoFixture.mu.Lock()
	unavailableResult := cryptoFixture.states[len(cryptoFixture.states)-1]
	cryptoFixture.mu.Unlock()
	require.Equal(t, id.EventID("$synthetic-ttl"), unavailableResult.SourceEventID)
	require.Equal(t, "rejected", unavailableResult.ControlResult.Status)
	require.Equal(t, "source_media_unavailable", unavailableResult.ControlResult.ErrorCode)
	// Protection can change after Matrix ingestion. The authoritative source
	// re-fetch blocks contents-read even when stored metadata previously allowed.
	rpc.remote.Noforwards = true
	protected := syntheticControlEvent(time.Now())
	protected.ID = "$synthetic-now-protected"
	protected.Content.Raw["request_id"] = "synthetic-now-protected"
	protected.Content.Raw["source"].(map[string]any)["account_id"] = "123"
	dispatch(protected)
	require.EqualValues(t, 2, rpc.getCalls.Load())
	require.EqualValues(t, 1, rpc.contentsCalls.Load())
	cryptoFixture.mu.Lock()
	protectedResult := cryptoFixture.states[len(cryptoFixture.states)-1]
	cryptoFixture.mu.Unlock()
	require.Equal(t, "rejected", protectedResult.ControlResult.Status)
	require.Equal(t, "protected_source", protectedResult.ControlResult.ErrorCode)
	require.False(t, *protectedResult.MediaUnread)
}

func TestNativeForwardMTProtoStubAndWrongDestinationRejection(t *testing.T) {
	req := syntheticPlayedRequest(time.Now())
	req.Destination = &MediaControlDestination{AccountID: req.Source.AccountID, RoomID: "!synthetic-destination:example.invalid"}
	req.Silent = boolControl(true)
	echo := syntheticRemoteRound(&tg.PeerChannel{ChannelID: 555})
	echo.ID = 50
	echo.Out = true
	updates := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 50, RandomID: 1234}, &tg.UpdateNewChannelMessage{Message: echo}}}
	stub := &mediaMTProtoFixture{forwardUpdates: updates}
	api := tg.NewClient(stub)
	source := &tg.InputPeerUser{UserID: 777, AccessHash: 88}
	destination := &tg.InputPeerChannel{ChannelID: 555, AccessHash: 99}
	_, err := dispatchNativeRoundForward(context.Background(), api, req, source, destination, destination, 42, 12, 1234)
	require.NoError(t, err)
	require.EqualValues(t, 1, stub.forwardCalls.Load())
	require.Equal(t, []int64{1234}, stub.forward.RandomID)
	require.Equal(t, []int{42}, stub.forward.ID)
	require.True(t, stub.forward.Silent)
	require.Equal(t, 12, stub.forward.TopMsgID)
	require.Zero(t, stub.forward.AllowPaidStars)
	wire, err := json.Marshal(stub.forward)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "InputMedia")
	echo.PeerID = &tg.PeerChannel{ChannelID: 999}
	_, err = dispatchNativeRoundForward(context.Background(), api, req, source, destination, destination, 42, 12, 1234)
	require.Error(t, err)
	require.Equal(t, "uncertain", classifyRoundOperationError(err, true).Certainty)
	previousCalls := stub.forwardCalls.Load()
	req.CreatedAtMS = time.Now().Add(-6 * time.Minute).UnixMilli()
	_, err = dispatchNativeRoundForward(context.Background(), api, req, source, destination, destination, 42, 12, 1234)
	require.Error(t, err)
	require.Equal(t, previousCalls, stub.forwardCalls.Load())
}
