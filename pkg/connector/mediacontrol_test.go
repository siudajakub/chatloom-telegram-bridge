package connector

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

func boolControl(v bool) *bool { return &v }

func syntheticPlayedRequest(now time.Time) *MediaControlRequest {
	return &MediaControlRequest{Version: 1, RequestID: "synthetic-play-1", CreatedAtMS: now.UnixMilli(), Source: MediaControlSource{AccountID: "synthetic-login", RoomID: "!synthetic-round:example.invalid", EventID: "$synthetic-round"}, Manual: boolControl(true), Incognito: boolControl(false), ExplicitGrant: boolControl(false)}
}

func syntheticControlEvent(now time.Time) *event.Event {
	return &event.Event{Type: MediaPlayedEventType, ID: "$synthetic-control", RoomID: "!synthetic-round:example.invalid", Sender: "@synthetic-user:example.invalid", Timestamp: now.UnixMilli(), Content: event.Content{Raw: map[string]any{"version": 1, "request_id": "synthetic-play-1", "created_at_ms": now.UnixMilli(), "source": map[string]any{"account_id": "synthetic-login", "room_id": "!synthetic-round:example.invalid", "event_id": "$synthetic-round"}, "manual": true, "incognito": false, "explicit_grant": false}}}
}

func TestMediaControlStrictPayloadAndGrant(t *testing.T) {
	now := time.Unix(1791792000, 0)
	valid, err := decodeMediaControl(syntheticControlEvent(now), now)
	require.NoError(t, err)
	require.True(t, *valid.Manual)
	for _, test := range []struct {
		name   string
		mutate func(*event.Event)
	}{
		{"string_bool", func(e *event.Event) { e.Content.Raw["manual"] = "true" }},
		{"autoplay", func(e *event.Event) { e.Content.Raw["manual"] = false }},
		{"incognito", func(e *event.Event) { e.Content.Raw["incognito"] = true }},
		{"missing_incognito", func(e *event.Event) { delete(e.Content.Raw, "incognito") }},
		{"missing_grant", func(e *event.Event) { delete(e.Content.Raw, "explicit_grant") }},
		{"wrong_room", func(e *event.Event) { e.RoomID = "!foreign:example.invalid" }},
		{"stale", func(e *event.Event) { e.Content.Raw["created_at_ms"] = now.Add(-6 * time.Minute).UnixMilli() }},
		{"future", func(e *event.Event) { e.Content.Raw["created_at_ms"] = now.Add(time.Minute).UnixMilli() }},
		{"unknown_version", func(e *event.Event) { e.Content.Raw["version"] = 2 }},
		{"ordinary_msgtype", func(e *event.Event) {
			e.Type = event.EventMessage
			e.Content.Raw["msgtype"] = MediaPlayedEventType.Type
		}},
		{"state_event", func(e *event.Event) { key := ""; e.StateKey = &key }},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := syntheticControlEvent(now)
			test.mutate(e)
			_, err := decodeMediaControl(e, now)
			require.Error(t, err)
		})
	}
	grant := syntheticControlEvent(now)
	grant.Content.Raw["incognito"] = true
	grant.Content.Raw["explicit_grant"] = true
	_, err = decodeMediaControl(grant, now)
	require.NoError(t, err)
	_, err = decodeMediaControl(grant, now.Add(6*time.Minute))
	require.Error(t, err)
}

type syntheticMediaRPC struct {
	contentsCalls        int
	channelContentsCalls int
	forwardCalls         int
	readIDs              []int
	channelRead          *tg.ChannelsReadMessageContentsRequest
	messages             tg.MessagesMessagesClass
	readErr              error
}

func (s *syntheticMediaRPC) MessagesGetMessages(context.Context, []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
	return s.messages, nil
}
func (s *syntheticMediaRPC) ChannelsGetMessages(context.Context, *tg.ChannelsGetMessagesRequest) (tg.MessagesMessagesClass, error) {
	return s.messages, nil
}
func (s *syntheticMediaRPC) MessagesReadMessageContents(_ context.Context, ids []int) (*tg.MessagesAffectedMessages, error) {
	s.contentsCalls++
	s.readIDs = ids
	return &tg.MessagesAffectedMessages{}, s.readErr
}
func (s *syntheticMediaRPC) ChannelsReadMessageContents(_ context.Context, req *tg.ChannelsReadMessageContentsRequest) (bool, error) {
	s.channelContentsCalls++
	s.channelRead = req
	return true, s.readErr
}
func (s *syntheticMediaRPC) MessagesForwardMessages(context.Context, *tg.MessagesForwardMessagesRequest) (tg.UpdatesClass, error) {
	s.forwardCalls++
	return nil, nil
}

func syntheticRemoteRound(peer tg.PeerClass) *tg.Message {
	return &tg.Message{ID: 42, PeerID: peer, MediaUnread: true, Media: &tg.MessageMediaDocument{Document: &tg.Document{MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{RoundMessage: true, Duration: 2, W: 240, H: 240}}}}}
}

func TestMediaPlayedExactRPCAndNoRPCPrivacyNegatives(t *testing.T) {
	now := time.Now()
	req := syntheticPlayedRequest(now)
	msg := syntheticRemoteRound(&tg.PeerUser{UserID: 777})
	rpc := &syntheticMediaRPC{}
	require.NoError(t, readExactRoundContents(context.Background(), rpc, &tg.InputPeerUser{UserID: 777}, msg, req, now))
	require.Equal(t, []int{42}, rpc.readIDs)
	require.Equal(t, 1, rpc.contentsCalls)
	require.Zero(t, rpc.channelContentsCalls)
	channel := &syntheticMediaRPC{}
	require.NoError(t, readExactRoundContents(context.Background(), channel, &tg.InputPeerChannel{ChannelID: 555, AccessHash: 99}, msg, req, now))
	require.Equal(t, []int{42}, channel.channelRead.ID)
	require.EqualValues(t, 555, channel.channelRead.Channel.(*tg.InputChannel).ChannelID)
	require.Zero(t, channel.contentsCalls)
	for _, test := range []struct {
		name   string
		mutate func(*MediaControlRequest, *tg.Message)
	}{
		{"autoplay", func(r *MediaControlRequest, m *tg.Message) { r.Manual = boolControl(false) }},
		{"incognito_no_grant", func(r *MediaControlRequest, m *tg.Message) { r.Incognito = boolControl(true) }},
		{"stale_grant", func(r *MediaControlRequest, m *tg.Message) {
			r.CreatedAtMS = now.Add(-6 * time.Minute).UnixMilli()
			r.Incognito = boolControl(true)
			r.ExplicitGrant = boolControl(true)
		}},
		{"outgoing", func(r *MediaControlRequest, m *tg.Message) { m.Out = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := syntheticPlayedRequest(now)
			m := syntheticRemoteRound(&tg.PeerUser{UserID: 777})
			test.mutate(r, m)
			blocked := &syntheticMediaRPC{}
			require.Error(t, readExactRoundContents(context.Background(), blocked, &tg.InputPeerUser{UserID: 777}, m, r, now))
			require.Zero(t, blocked.contentsCalls+blocked.channelContentsCalls+blocked.forwardCalls)
		})
	}
}

func TestMediaControlSourceFetchRejectsForeignPeerTTLAndOrdinaryVideo(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*tg.Message)
	}{
		{"foreign_peer", func(m *tg.Message) { m.PeerID = &tg.PeerUser{UserID: 999} }},
		{"ttl", func(m *tg.Message) { m.Media.(*tg.MessageMediaDocument).TTLSeconds = 15 }},
		{"ordinary_video", func(m *tg.Message) {
			m.Media.(*tg.MessageMediaDocument).Document.(*tg.Document).Attributes[0].(*tg.DocumentAttributeVideo).RoundMessage = false
		}},
		{"expired_document", func(m *tg.Message) { m.Media.(*tg.MessageMediaDocument).Document = &tg.DocumentEmpty{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := syntheticRemoteRound(&tg.PeerUser{UserID: 777})
			test.mutate(m)
			rpc := &syntheticMediaRPC{messages: &tg.MessagesMessages{Messages: []tg.MessageClass{m}}}
			_, err := fetchControlMedia(context.Background(), rpc, &tg.InputPeerUser{UserID: 777}, ids.PeerTypeUser, 42, true)
			require.Error(t, err)
			require.Zero(t, rpc.contentsCalls+rpc.channelContentsCalls+rpc.forwardCalls)
		})
	}
}

func TestMediaForwardNativeRequestAndRoundEcho(t *testing.T) {
	req := syntheticPlayedRequest(time.Now())
	req.Destination = &MediaControlDestination{AccountID: req.Source.AccountID, RoomID: "!synthetic-destination:example.invalid"}
	req.PreserveAuthor = boolControl(false)
	req.Silent = boolControl(true)
	native, err := nativeForwardRequest(req, &tg.InputPeerUser{UserID: 777}, &tg.InputPeerChannel{ChannelID: 555, AccessHash: 99}, 42, 12, 1234)
	require.NoError(t, err)
	require.Equal(t, []int{42}, native.ID)
	require.Equal(t, []int64{1234}, native.RandomID)
	require.Equal(t, 12, native.TopMsgID)
	require.True(t, native.Silent)
	require.True(t, native.DropAuthor)
	require.Zero(t, native.AllowPaidStars)
	require.False(t, native.AllowPaidFloodskip)
	echo := syntheticRemoteRound(&tg.PeerChannel{ChannelID: 555})
	echo.ID = 50
	updates := &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 50, RandomID: 1234}, &tg.UpdateNewChannelMessage{Message: echo}}}
	require.True(t, forwardedRoundEcho(updates, 1234))
	require.False(t, forwardedRoundEcho(updates, 9876))
	echo.Media.(*tg.MessageMediaDocument).Document.(*tg.Document).Attributes[0].(*tg.DocumentAttributeVideo).RoundMessage = false
	require.False(t, forwardedRoundEcho(updates, 1234))
	req.Destination.AccountID = "foreign-account"
	_, err = nativeForwardRequest(req, native.FromPeer, native.ToPeer, 42, 12, 1234)
	require.Error(t, err)
	req.Destination.AccountID = req.Source.AccountID
	req.ScheduleDate = time.Now().Add(time.Hour).Unix()
	_, err = nativeForwardRequest(req, native.FromPeer, native.ToPeer, 42, 12, 1234)
	require.Error(t, err)
}

type mediaDecryptFixture struct {
	matrix.Crypto
	decrypted    *event.Event
	decryptCalls atomic.Int64
}

func (f *mediaDecryptFixture) Decrypt(context.Context, *event.Event) (*event.Event, error) {
	f.decryptCalls.Add(1)
	return f.decrypted, nil
}

func TestMediaControlFrameworkPostDecryptDispatchAndForeignAuth(t *testing.T) {
	// Real framework handleEncryptedEvent -> postDecrypt -> EventProcessor.On.
	// Only the cryptographic primitive is a synthetic fixture. No crypto fork,
	// real account, network Telegram client, or ordinary message handler exists.
	homeserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer homeserver.Close()
	raw, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "synthetic.db"))
	require.NoError(t, err)
	defer raw.Close()
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
	var logs bytes.Buffer
	br := bridgev2.NewBridge("synthetic-bridge", db, zerolog.New(&logs), &cfg.Bridge, mx, network, commands.NewProcessor)
	br.BackgroundCtx = context.Background()
	require.NoError(t, br.DB.Upgrade(context.Background()))
	require.True(t, network.mediaControlsEnabled) // registered by connector Init
	decrypted := syntheticControlEvent(time.Now())
	decrypted.Mautrix.WasEncrypted = true
	decrypted.Mautrix.TrustState = id.TrustStateVerified
	cryptoFixture := &mediaDecryptFixture{decrypted: decrypted}
	mx.Crypto = cryptoFixture
	var customDispatch, ordinaryDispatch atomic.Int64
	mx.EventProcessor.On(MediaPlayedEventType, func(ctx context.Context, evt *event.Event) { customDispatch.Add(1) })
	mx.EventProcessor.On(event.EventMessage, func(ctx context.Context, evt *event.Event) { ordinaryDispatch.Add(1) })
	outer := &event.Event{Type: event.EventEncrypted, ID: decrypted.ID, Sender: decrypted.Sender, RoomID: decrypted.RoomID, Timestamp: decrypted.Timestamp, Content: event.Content{Parsed: &event.EncryptedEventContent{Algorithm: id.AlgorithmMegolmV1}}}
	mx.EventProcessor.Dispatch(br.Log.WithContext(context.Background()), outer)
	require.EqualValues(t, 1, cryptoFixture.decryptCalls.Load())
	require.EqualValues(t, 1, customDispatch.Load())
	require.Zero(t, ordinaryDispatch.Load())
	require.Contains(t, logs.String(), "sender_forbidden") // no user/login in fixture DB
	var operationCount int
	require.NoError(t, network.Store.Upgrade(context.Background()))
	require.NoError(t, network.Store.QueryRow(context.Background(), `SELECT COUNT(*) FROM telegram_round_operation`).Scan(&operationCount))
	require.Zero(t, operationCount) // authentication fails before durable dispatch/RPC
	for _, mutate := range []func(*event.Event){
		func(e *event.Event) { e.Mautrix.WasEncrypted = false },
		func(e *event.Event) { e.Mautrix.TrustState = id.TrustStateBlacklisted },
		func(e *event.Event) { e.Sender = mx.Bot.UserID },
		func(e *event.Event) { e.Sender = "@synthetic_ghost_777:example.invalid" },
		func(e *event.Event) { e.Content.Raw["fi.mau.double_puppet_source"] = mx.AS.DoublePuppetValue },
	} {
		e := syntheticControlEvent(time.Now())
		e.Mautrix.WasEncrypted = true
		e.Mautrix.TrustState = id.TrustStateVerified
		mutate(e)
		require.Error(t, validateMediaControlEnvelope(e, mx, br))
	}
}

func TestRoundOperationErrorCertainty(t *testing.T) {
	for _, test := range []struct {
		err        error
		dispatched bool
		certainty  string
		retry      bool
	}{
		{tgerr.New(403, "VOICE_MESSAGES_FORBIDDEN"), true, "certain", false},
		{tgerr.New(400, "ALLOW_PAYMENT_REQUIRED_10"), true, "certain", false},
		{tgerr.New(420, "FLOOD_WAIT_5"), true, "certain", true},
		{tgerr.New(500, "INTERNAL"), true, "uncertain", true},
		// The stable random ID was already accepted: sent, never resend.
		{tgerr.New(500, "RANDOM_ID_DUPLICATE"), true, "certain", false},
		{errors.New("ACK lost"), true, "uncertain", true},
		{context.DeadlineExceeded, false, "certain", true},
		{&RoundMediaError{Code: "unsupported_codec"}, false, "certain", false},
		{&RoundMediaError{Code: "encode_timeout"}, false, "certain", true},
	} {
		typed := classifyRoundOperationError(test.err, test.dispatched)
		require.Equal(t, test.certainty, typed.Certainty)
		require.Equal(t, test.retry, typed.Retriable)
	}
}

func TestMediaControlSourceFetchAcceptsVoiceOnlyForPlayback(t *testing.T) {
	m := syntheticRemoteRound(&tg.PeerUser{UserID: 777})
	m.Media.(*tg.MessageMediaDocument).Document.(*tg.Document).Attributes = []tg.DocumentAttributeClass{&tg.DocumentAttributeAudio{Voice: true, Duration: 3}}
	rpc := &syntheticMediaRPC{messages: &tg.MessagesMessages{Messages: []tg.MessageClass{m}}}
	_, err := fetchControlMedia(context.Background(), rpc, &tg.InputPeerUser{UserID: 777}, ids.PeerTypeUser, 42, true)
	require.NoError(t, err)
	_, err = fetchControlMedia(context.Background(), rpc, &tg.InputPeerUser{UserID: 777}, ids.PeerTypeUser, 42, false)
	require.Error(t, err)
	m.Media.(*tg.MessageMediaDocument).Document.(*tg.Document).Attributes = []tg.DocumentAttributeClass{&tg.DocumentAttributeAudio{Duration: 3}}
	_, err = fetchControlMedia(context.Background(), rpc, &tg.InputPeerUser{UserID: 777}, ids.PeerTypeUser, 42, true)
	require.Error(t, err)
}
