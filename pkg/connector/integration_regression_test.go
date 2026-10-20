// SPDX-License-Identifier: AGPL-3.0-or-later
package connector

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/exsync"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/crypto/attachment"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

func TestIntegrationRoundCanonicalMixedAndCombinedResponses(t *testing.T) {
	matching := &tg.Message{ID: 7, Date: 100, Out: true, PeerID: &tg.PeerChat{ChatID: 123}, Media: roundEchoMedia(true)}
	updates := []tg.UpdateClass{
		&tg.UpdateMessageID{ID: 7, RandomID: 42},
		&tg.UpdateNewMessage{Message: matching},
		&tg.UpdateMessageID{ID: 900, RandomID: 43},
		&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: 900, PeerID: &tg.PeerChannel{ChannelID: 321}, Media: roundEchoMedia(false)}},
	}
	for _, response := range []tg.UpdatesClass{&tg.Updates{Updates: updates}, &tg.UpdatesCombined{Updates: updates}} {
		canonical, err := canonicalRoundSendUpdates(response, 42, &tg.InputPeerChat{ChatID: 123})
		require.NoError(t, err)
		filtered := canonical.(*tg.Updates)
		require.Len(t, filtered.Updates, 2)
		require.Equal(t, 7, filtered.Updates[0].(*tg.UpdateMessageID).ID)
		require.Same(t, matching, filtered.Updates[1].(*tg.UpdateNewMessage).Message)
	}
	for _, conflicting := range [][]tg.UpdateClass{
		{&tg.UpdateMessageID{ID: 7, RandomID: 42}, &tg.UpdateMessageID{ID: 8, RandomID: 42}, &tg.UpdateNewMessage{Message: matching}},
		{&tg.UpdateMessageID{ID: 7, RandomID: 42}, &tg.UpdateNewMessage{Message: matching}, &tg.UpdateNewChannelMessage{Message: &tg.Message{ID: 7, Media: roundEchoMedia(true)}}},
	} {
		canonical, err := canonicalRoundSendUpdates(&tg.Updates{Updates: conflicting}, 42)
		require.Error(t, err)
		require.Nil(t, canonical)
	}
	foreignPeer := &tg.UpdatesCombined{Updates: []tg.UpdateClass{
		&tg.UpdateMessageID{ID: 7, RandomID: 42},
		&tg.UpdateNewChannelMessage{Message: &tg.Message{ID: 7, Out: true, PeerID: &tg.PeerChannel{ChannelID: 321}, Media: roundEchoMedia(true)}},
	}}
	canonical, err := canonicalRoundSendUpdates(foreignPeer, 42, &tg.InputPeerChannel{ChannelID: 123})
	require.Error(t, err)
	require.Nil(t, canonical)
}

func TestIntegrationHandleMatrixRoundInvalidRequestsZeroRPC(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*bridgev2.MatrixMessage)
	}{
		{"truthy_marker", func(m *bridgev2.MatrixMessage) { m.Content.Info.Extra[roundMessageMarker] = "true" }},
		{"wrong_msgtype", func(m *bridgev2.MatrixMessage) { m.Content.MsgType = event.MsgFile }},
		{"wrong_event_type", func(m *bridgev2.MatrixMessage) { m.Event.Type = event.EventSticker }},
		{"force_document", func(m *bridgev2.MatrixMessage) { m.Event.Content.Raw["fi.mau.telegram.force_document"] = true }},
		{"caption", func(m *bridgev2.MatrixMessage) { m.Content.Body = "unsupported caption" }},
		{"time_limited", func(m *bridgev2.MatrixMessage) { m.Content.Info.Extra["ttl_seconds"] = float64(30) }},
		{"view_once", func(m *bridgev2.MatrixMessage) { m.Event.Content.Raw["view_once"] = true }},
		{"protected_policy", func(m *bridgev2.MatrixMessage) {
			m.Event.Content.Raw[mediaPolicyKey] = map[string]any{"protected_content": true}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			client := telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{
				telegram.MiddlewareFunc(func(_ tg.Invoker) telegram.InvokeFunc {
					return func(context.Context, bin.Encoder, bin.Decoder) error {
						calls.Add(1)
						return errors.New("unexpected synthetic RPC")
					}
				}),
			}})
			tc := &TelegramClient{client: client}
			msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
				Event:   &event.Event{ID: "$synthetic-round", Type: event.EventMessage, Content: event.Content{Raw: map[string]any{}}},
				Content: syntheticRoundContent(true),
			}}
			test.change(msg)
			response, err := tc.HandleMatrixMessage(context.Background(), msg)
			require.Error(t, err)
			require.Nil(t, response)
			require.Zero(t, calls.Load())
		})
	}
}

func integrationStore(t *testing.T, path string) (*store.Container, *sql.DB) {
	t.Helper()
	raw, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	require.NoError(t, err)
	container := store.NewStore(db, dbutil.NoopLogger)
	// Store migration v10 rewrites bridgev2 message IDs; isolated store tests need the table.
	_, err = db.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS message (id TEXT, room_id TEXT)")
	require.NoError(t, err)
	require.NoError(t, container.Upgrade(context.Background()))
	return container, raw
}

type integrationSendRPC struct {
	mu                              sync.Mutex
	ledger                          *store.RoundLedger
	key                             store.RoundOperationKey
	deny                            bool
	revokeAfterUpload               bool
	ackLoss                         bool
	permissionCalls, uploads, sends int
	randomIDs                       []int64
	lastRequest                     *tg.MessagesSendMediaRequest
	// response selects the sendMedia result shape: "" (combined with echo),
	// "ack_only", "short", "downgraded" or "duplicate" (RANDOM_ID_DUPLICATE).
	response string
	// landed makes the server keep a dispatch that the client never saw
	// acknowledged (ack loss), so it is visible in history.
	landed, getMessagesFail        bool
	getMessagesCalls, historyCalls int
	uploadedSizes                  map[int64]int64
	accepted                       *tg.Message
}

func (stub *integrationSendRPC) echoMessage(doc *tg.InputMediaUploadedDocument, round bool) *tg.Message {
	attrs := doc.Attributes
	if !round {
		attrs = []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{W: 128, H: 128, Duration: 0.6}}
	}
	size := stub.uploadedSizes[doc.File.(*tg.InputFile).ID]
	message := &tg.Message{ID: 7, Date: int(time.Now().Unix()), Out: true, PeerID: &tg.PeerChat{ChatID: 123}}
	message.SetMedia(&tg.MessageMediaDocument{Document: &tg.Document{ID: 12, Date: 100, Size: size, MimeType: doc.MimeType, Attributes: attrs}})
	return message
}

func integrationMessages(messages ...*tg.Message) *tg.MessagesMessages {
	result := &tg.MessagesMessages{Chats: []tg.ChatClass{}, Users: []tg.UserClass{}}
	for _, msg := range messages {
		if msg != nil {
			result.Messages = append(result.Messages, msg)
		}
	}
	if result.Messages == nil {
		result.Messages = []tg.MessageClass{}
	}
	return result
}

func (stub *integrationSendRPC) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	var result bin.Encoder
	switch request := input.(type) {
	case *tg.MessagesGetChatsRequest:
		stub.permissionCalls++
		result = &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Chat{ID: 123, Title: "Synthetic", Photo: &tg.ChatPhotoEmpty{}, DefaultBannedRights: tg.ChatBannedRights{SendRoundvideos: stub.deny || stub.revokeAfterUpload && stub.uploads > 0}}}}
	case *tg.UploadSaveFilePartRequest:
		stub.uploads++
		if stub.uploadedSizes == nil {
			stub.uploadedSizes = map[int64]int64{}
		}
		stub.uploadedSizes[request.FileID] += int64(len(request.Bytes))
		op, err := stub.ledger.Get(ctx, stub.key)
		if err != nil {
			return err
		}
		if op == nil || op.RandomID == 0 || op.Status != "prepared" && op.Status != "uncertain" {
			return errors.New("round upload did not retain durable pre-dispatch identity")
		}
		result = &tg.BoolTrue{}
	case *tg.MessagesSendMediaRequest:
		stub.sends++
		wire := &bin.Buffer{}
		if err := request.Encode(wire); err != nil {
			return err
		}
		decoded := &tg.MessagesSendMediaRequest{}
		if err := decoded.Decode(wire); err != nil {
			return err
		}
		stub.lastRequest = decoded
		stub.randomIDs = append(stub.randomIDs, decoded.RandomID)
		op, err := stub.ledger.Get(ctx, stub.key)
		if err != nil {
			return err
		}
		if op == nil || op.RandomID != decoded.RandomID || op.Status != "dispatching" || op.LeaseUntilMS <= time.Now().UnixMilli() {
			return errors.New("sendMedia was not atomically claimed with persisted random ID")
		}
		doc, ok := decoded.Media.(*tg.InputMediaUploadedDocument)
		if !ok || doc.ForceFile || doc.Thumb == nil || decoded.Message != "" || len(decoded.Entities) != 0 || decoded.AllowPaidStars != 0 || decoded.AllowPaidFloodskip {
			return errors.New("main send path downgraded round/profile/paid policy")
		}
		if stub.ackLoss && stub.sends == 1 {
			if stub.landed {
				stub.accepted = stub.echoMessage(doc, true)
			}
			return context.DeadlineExceeded
		}
		switch stub.response {
		case "duplicate":
			// An earlier dispatch with this random ID was accepted.
			stub.accepted = stub.echoMessage(doc, true)
			return tgerr.New(500, "RANDOM_ID_DUPLICATE")
		case "duplicate_unmapped":
			return tgerr.New(500, "RANDOM_ID_DUPLICATE")
		case "ack_only":
			stub.accepted = stub.echoMessage(doc, true)
			result = &tg.Updates{Date: 100, Updates: []tg.UpdateClass{
				&tg.UpdateReadHistoryOutbox{Peer: &tg.PeerChat{ChatID: 123}, MaxID: 6},
				&tg.UpdateMessageID{ID: 7, RandomID: decoded.RandomID},
			}, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}}
		case "short":
			stub.accepted = stub.echoMessage(doc, true)
			short := &tg.UpdateShortSentMessage{ID: 7, Date: 100, Out: true}
			short.SetMedia(stub.accepted.Media)
			result = short
		case "downgraded":
			stub.accepted = stub.echoMessage(doc, false)
			result = &tg.Updates{Date: 100, Updates: []tg.UpdateClass{
				&tg.UpdateMessageID{ID: 7, RandomID: decoded.RandomID}, &tg.UpdateNewMessage{Message: stub.accepted},
			}, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}}
		}
		if result != nil {
			break
		}
		message := &tg.Message{ID: 7, Date: 100, Out: true, PeerID: &tg.PeerChat{ChatID: 123}}
		message.SetMedia(&tg.MessageMediaDocument{Document: &tg.Document{ID: 12, Date: 100, Size: 1000, MimeType: doc.MimeType, Attributes: doc.Attributes}})
		foreign := &tg.Message{ID: 900, Date: 200, PeerID: &tg.PeerChannel{ChannelID: 321}}
		foreign.SetMedia(roundEchoMedia(false))
		result = &tg.UpdatesCombined{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 7, RandomID: decoded.RandomID}, &tg.UpdateNewMessage{Message: message},
			&tg.UpdateMessageID{ID: 900, RandomID: decoded.RandomID + 1}, &tg.UpdateNewChannelMessage{Message: foreign},
		}}
	case *tg.MessagesGetMessagesRequest:
		stub.getMessagesCalls++
		if stub.getMessagesFail {
			return tgerr.New(500, "INTERNAL")
		}
		result = integrationMessages(stub.accepted)
	case *tg.MessagesGetHistoryRequest:
		stub.historyCalls++
		if request.Limit <= 0 || request.Limit > 100 {
			return errors.New("unbounded history reconciliation")
		}
		result = integrationMessages(stub.accepted)
	default:
		return errors.New("unexpected synthetic main-send RPC")
	}
	encoded := &bin.Buffer{}
	if err := result.Encode(encoded); err != nil {
		return err
	}
	return output.Decode(encoded)
}

func integrationSendClient(t *testing.T, container *store.Container, stub *integrationSendRPC, bot bridgev2.MatrixAPI) (*TelegramClient, *bridgev2.Portal) {
	t.Helper()
	bridge := &bridgev2.Bridge{ID: "synthetic-bridge", Bot: bot}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "456", UserMXID: "@synthetic:example.invalid"}, Bridge: bridge}
	portal := intakePortal(ids.PeerTypeChat, boolPtr(false))
	portal.Bridge = bridge
	portal.MXID = "" // no capability state HTTP is relevant to this fixture
	portal.BridgeID = bridge.ID
	initialized, done := exsync.NewEvent(), exsync.NewEvent()
	initialized.Set()
	client := telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{
		telegram.MiddlewareFunc(func(_ tg.Invoker) telegram.InvokeFunc { return stub.Invoke }),
	}})
	tc := &TelegramClient{
		main:   &TelegramConnector{Bridge: bridge, Store: container, maxFileSize: roundMaxFileBytes},
		client: client, clientInitialized: initialized, clientDone: done,
		userLogin: login, loginID: login.ID, telegramUserID: 456,
		metadata:  &UserLoginMetadata{Session: UserLoginSession{AuthKey: bytes.Repeat([]byte{0x42}, 256)}},
		appConfig: map[string]any{"upload_max_fileparts_default": float64(4000)},
	}
	login.Client = tc
	stub.ledger = store.NewRoundLedger(container.Database)
	stub.key = tc.operationKey(portal, "round_send", "$synthetic-round")
	return tc, portal
}

func integrationMessage(portal *bridgev2.Portal) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Event:  &event.Event{ID: "$synthetic-round", Type: event.EventMessage, Content: event.Content{Raw: map[string]any{}}},
		Portal: portal, Content: syntheticRoundContent(true),
	}, ReplyTo: &database.Message{ID: ids.MakeMessageID(portal.PortalKey, 5)}}
}

func TestIntegrationHandleMatrixRoundPermissionDenialNoMediaRPC(t *testing.T) {
	container, raw := integrationStore(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	stub := &integrationSendRPC{deny: true}
	tc, portal := integrationSendClient(t, container, stub, nil)
	response, err := tc.HandleMatrixMessage(context.Background(), integrationMessage(portal))
	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, stub.permissionCalls)
	require.Zero(t, stub.uploads)
	require.Zero(t, stub.sends)
	operation, err := stub.ledger.Get(context.Background(), stub.key)
	require.NoError(t, err)
	require.Equal(t, "prepared", operation.Status)
}

func TestIntegrationHandleMatrixRoundPermissionRevokedAfterUpload(t *testing.T) {
	path := syntheticRoundFile(t, 128, "0.6", "libx264", "aac")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(data)
	}))
	defer server.Close()
	matrixClient, err := mautrix.NewClient(server.URL, "@synthetic:example.invalid", "synthetic-not-a-token")
	require.NoError(t, err)
	bot := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: matrixClient}}
	container, raw := integrationStore(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	stub := &integrationSendRPC{revokeAfterUpload: true}
	tc, portal := integrationSendClient(t, container, stub, bot)
	msg := integrationMessage(portal)
	msg.Content.URL = "mxc://example.invalid/synthetic-round"
	msg.Content.Info.Size = len(data)
	response, err := tc.HandleMatrixMessage(context.Background(), msg)
	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 2, stub.permissionCalls)
	require.Equal(t, 2, stub.uploads)
	require.Zero(t, stub.sends)
	operation, err := stub.ledger.Get(context.Background(), stub.key)
	require.NoError(t, err)
	require.Equal(t, "prepared", operation.Status)
	require.Zero(t, operation.LeaseUntilMS)
}

func TestIntegrationHandleMatrixRoundEncryptedDispatchAndCrashBeforeCoreSave(t *testing.T) {
	for _, ackLoss := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted_before_core_save", true: "ack_lost_same_identity_retry"}[ackLoss], func(t *testing.T) {
			path := syntheticRoundFile(t, 128, "0.6", "libx264", "aac")
			plain, err := os.ReadFile(path)
			require.NoError(t, err)
			encrypted := attachment.NewEncryptedFile()
			ciphertext := encrypted.Encrypt(plain)
			var downloads atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				downloads.Add(1)
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write(ciphertext)
			}))
			defer server.Close()
			matrixClient, err := mautrix.NewClient(server.URL, "@synthetic:example.invalid", "synthetic-not-a-token")
			require.NoError(t, err)
			bot := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: matrixClient}}
			ledgerPath := filepath.Join(t.TempDir(), "ledger.db")
			container, raw := integrationStore(t, ledgerPath)
			stub := &integrationSendRPC{ackLoss: ackLoss}
			tc, portal := integrationSendClient(t, container, stub, bot)
			msg := integrationMessage(portal)
			msg.Content.Info.Size = len(plain)
			// Reproduce a received Matrix wire object, not the encryptor's
			// private decoded-key cache, which does not retain a decoded hash.
			fileJSON, err := json.Marshal(&event.EncryptedFileInfo{EncryptedFile: *encrypted, URL: id.ContentURIString("mxc://example.invalid/synthetic-round")})
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(fileJSON, &msg.Content.File))
			response, err := tc.HandleMatrixMessage(context.Background(), msg)
			if ackLoss {
				require.Error(t, err)
				require.Nil(t, response)
				operation, readErr := stub.ledger.Get(context.Background(), stub.key)
				require.NoError(t, readErr)
				require.Equal(t, "uncertain", operation.Status)
				response, err = tc.HandleMatrixMessage(context.Background(), msg)
				require.Equal(t, stub.randomIDs[0], stub.randomIDs[1])
			}
			require.NoError(t, err)
			require.NotNil(t, response)
			require.Equal(t, ids.MakeMessageID(portal.PortalKey, 7), response.DB.ID)
			require.True(t, response.DB.Metadata.(*MessageMetadata).IsRound)
			require.Equal(t, 5, stub.lastRequest.ReplyTo.(*tg.InputReplyToMessage).ReplyToMsgID)
			operation, err := stub.ledger.Get(context.Background(), stub.key)
			require.NoError(t, err)
			require.Equal(t, "accepted", operation.Status)
			// Close the store immediately after the handler ACK, without any
			// core message-row save, then repeat through the actual handler.
			require.NoError(t, raw.Close())
			container, raw = integrationStore(t, ledgerPath)
			defer raw.Close()
			replayStub := &integrationSendRPC{}
			tc, portal = integrationSendClient(t, container, replayStub, bot)
			msg.Portal = portal
			beforeDownloads := downloads.Load()
			replayed, err := tc.HandleMatrixMessage(context.Background(), msg)
			require.NoError(t, err)
			require.Equal(t, response.DB.ID, replayed.DB.ID)
			require.True(t, replayed.DB.Metadata.(*MessageMetadata).IsRound)
			require.Zero(t, replayStub.permissionCalls)
			require.Zero(t, replayStub.uploads)
			require.Zero(t, replayStub.sends)
			require.Equal(t, beforeDownloads, downloads.Load())
		})
	}
}

type integrationRoundFixture struct {
	tc     *TelegramClient
	portal *bridgev2.Portal
	msg    *bridgev2.MatrixMessage
	stub   *integrationSendRPC
	logs   *bytes.Buffer
	ctx    context.Context
}

func newIntegrationRoundFixture(t *testing.T, stub *integrationSendRPC) *integrationRoundFixture {
	t.Helper()
	path := syntheticRoundFile(t, 128, "0.6", "libx264", "aac")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	matrixClient, err := mautrix.NewClient(server.URL, "@synthetic:example.invalid", "synthetic-not-a-token")
	require.NoError(t, err)
	bot := &matrix.ASIntent{Matrix: &appservice.IntentAPI{Client: matrixClient}}
	container, raw := integrationStore(t, filepath.Join(t.TempDir(), "ledger.db"))
	t.Cleanup(func() { _ = raw.Close() })
	tc, portal := integrationSendClient(t, container, stub, bot)
	msg := integrationMessage(portal)
	msg.Content.URL = "mxc://example.invalid/synthetic-round"
	msg.Content.Info.Size = len(data)
	logs := &bytes.Buffer{}
	logger := zerolog.New(logs)
	return &integrationRoundFixture{tc: tc, portal: portal, msg: msg, stub: stub, logs: logs, ctx: logger.WithContext(context.Background())}
}

func (f *integrationRoundFixture) operation(t *testing.T) *store.RoundOperation {
	t.Helper()
	op, err := f.stub.ledger.Get(context.Background(), f.stub.key)
	require.NoError(t, err)
	require.NotNil(t, op)
	return op
}

func (f *integrationRoundFixture) requireAccepted(t *testing.T, response *bridgev2.MatrixMessageResponse, err error) {
	t.Helper()
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, ids.MakeMessageID(f.portal.PortalKey, 7), response.DB.ID)
	require.Equal(t, int64(7), response.StreamOrder)
	require.True(t, response.DB.Metadata.(*MessageMetadata).IsRound)
	op := f.operation(t)
	require.Equal(t, "accepted", op.Status)
	var cached cachedRoundSendResponse
	require.NoError(t, json.Unmarshal([]byte(op.Result), &cached))
	require.Equal(t, int64(7), cached.TelegramMessageID)
	// Logs carry reason codes and durations only: no peer, event or content.
	// The mautrix HTTP client's own debug request log is framework output.
	var lines []string
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if !strings.Contains(line, `"message":"Request completed"`) {
			lines = append(lines, line)
		}
	}
	logs := strings.Join(lines, "\n")
	require.Contains(t, logs, `"outcome":"accepted"`)
	require.Contains(t, logs, `"encode_ms"`)
	require.Contains(t, logs, `"send_media_ms"`)
	require.NotContains(t, logs, "synthetic-round")
	require.NotContains(t, logs, "peer_id")
	require.NotContains(t, logs, "example.invalid")
}

func TestIntegrationRoundAckOnlyIsCertainAcceptance(t *testing.T) {
	for _, fetchFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "echo_fetched", true: "echo_fetch_failed"}[fetchFails], func(t *testing.T) {
			f := newIntegrationRoundFixture(t, &integrationSendRPC{response: "ack_only", getMessagesFail: fetchFails})
			response, err := f.tc.HandleMatrixMessage(f.ctx, f.msg)
			f.requireAccepted(t, response, err)
			require.Equal(t, 1, f.stub.sends)
			require.Equal(t, 1, f.stub.getMessagesCalls)
			require.Zero(t, f.stub.historyCalls)
			if fetchFails {
				require.Contains(t, f.logs.String(), `"reason_code":"echo_fetch_failed"`)
			} else {
				require.Contains(t, f.logs.String(), `"echo_verified":true`)
			}
		})
	}
}

func TestIntegrationRoundShortSentMessageAccepted(t *testing.T) {
	f := newIntegrationRoundFixture(t, &integrationSendRPC{response: "short"})
	response, err := f.tc.HandleMatrixMessage(f.ctx, f.msg)
	f.requireAccepted(t, response, err)
	require.Zero(t, f.stub.getMessagesCalls)
	require.Contains(t, f.logs.String(), `"echo_verified":true`)
}

func TestIntegrationRoundProfileMismatchIsWarningNotFailure(t *testing.T) {
	f := newIntegrationRoundFixture(t, &integrationSendRPC{response: "downgraded"})
	response, err := f.tc.HandleMatrixMessage(f.ctx, f.msg)
	f.requireAccepted(t, response, err)
	require.Equal(t, 1, f.stub.sends)
	require.Zero(t, f.stub.getMessagesCalls)
	require.Contains(t, f.logs.String(), `"level":"warn"`)
	require.Contains(t, f.logs.String(), `"reason_code":"echo_round_flag_missing"`)
	require.Contains(t, f.logs.String(), `"echo_verified":false`)
}

func TestIntegrationRoundRetryAfterUncertainReconcilesWithoutResend(t *testing.T) {
	f := newIntegrationRoundFixture(t, &integrationSendRPC{ackLoss: true, landed: true})
	response, err := f.tc.HandleMatrixMessage(f.ctx, f.msg)
	require.Error(t, err)
	require.Nil(t, response)
	op := f.operation(t)
	require.Equal(t, "uncertain", op.Status)
	record := parseRoundDispatchRecord(op.Result)
	require.NotNil(t, record)
	require.Positive(t, record.Size)
	uploads := f.stub.uploads
	response, err = f.tc.HandleMatrixMessage(f.ctx, f.msg)
	f.requireAccepted(t, response, err)
	require.Equal(t, 1, f.stub.sends, "uncertain retry must not send again")
	require.Equal(t, uploads, f.stub.uploads, "uncertain retry must not upload again")
	require.Equal(t, 1, f.stub.historyCalls)
	require.Contains(t, f.logs.String(), `"reconcile_result":"found"`)
	// A further retry returns the ledger result with no RPC at all.
	calls := f.stub.historyCalls + f.stub.sends + f.stub.uploads + f.stub.permissionCalls
	response, err = f.tc.HandleMatrixMessage(f.ctx, f.msg)
	require.NoError(t, err)
	require.Equal(t, ids.MakeMessageID(f.portal.PortalKey, 7), response.DB.ID)
	require.Equal(t, calls, f.stub.historyCalls+f.stub.sends+f.stub.uploads+f.stub.permissionCalls)
}

func TestIntegrationRoundRandomIDDuplicateReconciles(t *testing.T) {
	f := newIntegrationRoundFixture(t, &integrationSendRPC{response: "duplicate"})
	response, err := f.tc.HandleMatrixMessage(f.ctx, f.msg)
	f.requireAccepted(t, response, err)
	require.Equal(t, 1, f.stub.sends)
	require.Equal(t, 1, f.stub.historyCalls)

	unmapped := newIntegrationRoundFixture(t, &integrationSendRPC{response: "duplicate_unmapped"})
	response, err = unmapped.tc.HandleMatrixMessage(unmapped.ctx, unmapped.msg)
	require.Error(t, err)
	require.Nil(t, response)
	status := bridgev2.WrapErrorInStatus(err)
	require.True(t, status.IsCertain, "already sent is not an uncertain/retriable failure")
	require.Equal(t, event.MessageStatusFail, status.Status)
	op := unmapped.operation(t)
	require.Equal(t, "permanent", op.Status)
	require.Equal(t, "round_already_sent_unmapped", op.ErrorCode)
	require.Contains(t, unmapped.logs.String(), `"reason_code":"reconcile_not_found"`)
	// Retrying the same event never dispatches again.
	sends := unmapped.stub.sends
	_, err = unmapped.tc.HandleMatrixMessage(unmapped.ctx, unmapped.msg)
	require.Error(t, err)
	require.Equal(t, sends, unmapped.stub.sends)
}
