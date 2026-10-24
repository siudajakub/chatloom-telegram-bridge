// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// protectionRPC is a synthetic MTProto endpoint for messages.getChats. It
// counts calls and either answers with chat 123 or fails.
type protectionRPC struct {
	calls      atomic.Int64
	noforwards bool
	fail       error
	release    chan struct{}
}

func (p *protectionRPC) client() *telegram.Client {
	return telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{
		telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc {
			return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				if _, ok := input.(*tg.MessagesGetChatsRequest); !ok {
					return tgerr.New(400, "UNEXPECTED_SYNTHETIC_RPC")
				}
				p.calls.Add(1)
				if p.release != nil {
					<-p.release
				}
				if p.fail != nil {
					return p.fail
				}
				result := &tg.MessagesChats{Chats: []tg.ChatClass{&tg.Chat{ID: 123, Title: "Synthetic", Photo: &tg.ChatPhotoEmpty{}, Noforwards: p.noforwards}}}
				encoded := &bin.Buffer{}
				if err := result.Encode(encoded); err != nil {
					return err
				}
				return output.Decode(encoded)
			}
		}),
	}})
}

func protectionClient(rpc *protectionRPC, staleOnError bool) *TelegramClient {
	return &TelegramClient{main: &TelegramConnector{protectionStaleOnError: staleOnError}, client: rpc.client()}
}

func photoMessage(text string) *tg.Message {
	msg := &tg.Message{ID: 7, Message: text}
	msg.SetMedia(&tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 12}})
	return msg
}

func requirePolicy(t *testing.T, part any) *TelegramMediaPolicy {
	t.Helper()
	policy, ok := part.(*TelegramMediaPolicy)
	require.True(t, ok)
	return policy
}

func TestUnknownProtectionKeepsTextAndBlocksOnlyMedia(t *testing.T) {
	rpc := &protectionRPC{fail: tgerr.New(500, "INTERNAL")}
	tc := protectionClient(rpc, false)
	msg := photoMessage("synthetic caption")
	msg.SetReplyTo(&tg.MessageReplyHeader{ReplyToMsgID: 5})
	converted, err := tc.convertToMatrix(context.Background(), intakePortal(ids.PeerTypeChat, nil), nil, msg)
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.calls.Load())
	require.Len(t, converted.Parts, 2)
	text, media := converted.Parts[0], converted.Parts[1]
	require.Equal(t, event.MsgText, text.Content.MsgType)
	require.Equal(t, "synthetic caption", text.Content.Body)
	require.NotNil(t, converted.ReplyTo)
	textPolicy := requirePolicy(t, text.Extra[mediaPolicyKey])
	require.True(t, textPolicy.TransferAllowed, "clients must render the text")
	require.Nil(t, textPolicy.ProtectedContent, "unknown protection keeps forward/copy off")
	require.Equal(t, event.MsgNotice, media.Content.MsgType)
	require.Empty(t, media.Content.URL)
	require.Nil(t, media.Content.File)
	require.Nil(t, media.Content.Info)
	mediaPolicy := requirePolicy(t, media.Extra[mediaPolicyKey])
	require.False(t, mediaPolicy.TransferAllowed)
	require.Equal(t, "unknown_protection", mediaPolicy.Reason)
	meta := text.DBMetadata.(*MessageMetadata)
	require.False(t, meta.ProtectionKnown)
	require.Empty(t, meta.ContentURI)
	data, err := json.Marshal(converted)
	require.NoError(t, err)
	require.NotContains(t, string(data), "mxc://")
}

func TestProtectedChatBridgesTextAndBlocksOnlyMedia(t *testing.T) {
	// F15 (approved by J 2026-10-22): text in a noforwards chat is shown,
	// media is not, and the text carries protected_content=true.
	for _, test := range []struct {
		name       string
		msg        *tg.Message
		protection *bool
		parts      int
	}{
		{"chat_protected_plain_text", &tg.Message{ID: 7, Message: "synthetic text"}, ptr.Ptr(true), 1},
		{"message_protected_plain_text", &tg.Message{ID: 7, Message: "synthetic text", Noforwards: true}, ptr.Ptr(false), 1},
		{"chat_protected_caption", photoMessage("synthetic text"), nil, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			rpc := &protectionRPC{noforwards: true}
			tc := protectionClient(rpc, false)
			test.msg.SetTTLPeriod(30)
			converted, err := tc.convertToMatrix(context.Background(), intakePortal(ids.PeerTypeChat, test.protection), nil, test.msg)
			require.NoError(t, err)
			require.Len(t, converted.Parts, test.parts)
			text := converted.Parts[0]
			require.Equal(t, event.MsgText, text.Content.MsgType)
			require.Equal(t, "synthetic text", text.Content.Body)
			policy := requirePolicy(t, text.Extra[mediaPolicyKey])
			require.True(t, policy.TransferAllowed)
			require.NotNil(t, policy.ProtectedContent)
			require.True(t, *policy.ProtectedContent)
			require.True(t, text.DBMetadata.(*MessageMetadata).NoForwards)
			// The text follows the chat's auto-delete timer like any text.
			require.Equal(t, 30*time.Second, converted.Disappear.Timer)
			if test.parts == 2 {
				require.Equal(t, event.MsgNotice, converted.Parts[1].Content.MsgType)
				require.Equal(t, "This protected content can only be opened in Telegram.", converted.Parts[1].Content.Body)
				require.Nil(t, converted.Parts[1].Content.Info)
			}
		})
	}
}

func TestUnknownProtectionCustomEmojiKeepsFallbackText(t *testing.T) {
	rpc := &protectionRPC{fail: tgerr.New(500, "INTERNAL")}
	tc := protectionClient(rpc, false)
	msg := &tg.Message{ID: 7, Message: "synthetic 🙂", Entities: []tg.MessageEntityClass{&tg.MessageEntityCustomEmoji{Offset: 10, Length: 2, DocumentID: 12}}}
	converted, err := tc.convertToMatrix(context.Background(), intakePortal(ids.PeerTypeChat, nil), nil, msg)
	require.NoError(t, err)
	require.Len(t, converted.Parts, 1)
	require.Equal(t, event.MsgText, converted.Parts[0].Content.MsgType)
	require.Equal(t, "synthetic 🙂", converted.Parts[0].Content.Body)
	data, err := json.Marshal(converted)
	require.NoError(t, err)
	require.NotContains(t, string(data), "mxc://")
}

func TestProtectionFedFromUpdateEntitiesAvoidsRPC(t *testing.T) {
	rpc := &protectionRPC{fail: tgerr.New(500, "INTERNAL")}
	tc := protectionClient(rpc, false)
	tc.rememberProtectionFromEntities(tg.Entities{
		Chats:    map[int64]*tg.Chat{123: {ID: 123}},
		Channels: map[int64]*tg.Channel{123: {ID: 123, Noforwards: true}, 124: {ID: 124, Min: true}},
	})
	for _, test := range []struct {
		peer  ids.PeerType
		value bool
	}{{ids.PeerTypeChat, false}, {ids.PeerTypeChannel, true}} {
		portal := intakePortal(test.peer, nil)
		value, err := tc.protectionFor(context.Background(), portal)
		require.NoError(t, err)
		require.NotNil(t, value)
		require.Equal(t, test.value, *value)
		require.Equal(t, test.value, *getPortalProtection(portal))
	}
	require.Zero(t, rpc.calls.Load())
	// A min channel constructor is not evidence: no cache entry.
	_, ok := tc.protection.lookup(protectionKey{ids.PeerTypeChannel, 124})
	require.False(t, ok)
}

func TestProtectionRPCSingleFlight(t *testing.T) {
	rpc := &protectionRPC{release: make(chan struct{})}
	tc := protectionClient(rpc, false)
	portal := intakePortal(ids.PeerTypeChat, nil)
	var wg sync.WaitGroup
	results := make(chan *bool, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, _ := tc.protectionFor(context.Background(), portal)
			results <- value
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(rpc.release)
	wg.Wait()
	close(results)
	require.EqualValues(t, 1, rpc.calls.Load())
	for value := range results {
		require.NotNil(t, value)
		require.False(t, *value)
	}
	// Fresh for protectionFreshFor: no further RPC.
	_, err := tc.protectionFor(context.Background(), portal)
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.calls.Load())
}

func TestProtectionRPCErrorNeverNilsKnownValue(t *testing.T) {
	for _, stale := range []bool{false, true} {
		rpc := &protectionRPC{fail: tgerr.New(500, "INTERNAL")}
		tc := protectionClient(rpc, stale)
		portal := intakePortal(ids.PeerTypeChat, nil)
		setPortalProtection(portal, ptr.Ptr(false))
		value, err := tc.protectionFor(context.Background(), portal)
		require.Error(t, err)
		if stale {
			require.NotNil(t, value, "last known value within the TTL is used")
			require.False(t, *value)
		} else {
			require.Nil(t, value, "fallback disabled: unknown")
		}
		// The stored value survives the failed lookup either way.
		require.NotNil(t, getPortalProtection(portal))
		require.False(t, *getPortalProtection(portal))
	}
}

func TestProtectionFallbackTTL(t *testing.T) {
	rpc := &protectionRPC{fail: tgerr.New(500, "INTERNAL")}
	tc := protectionClient(rpc, true)
	key := protectionKey{ids.PeerTypeChat, 123}
	portal := intakePortal(ids.PeerTypeChat, ptr.Ptr(false)) // stored before wave 5: no timestamp
	value, _ := tc.protectionFor(context.Background(), portal)
	require.Nil(t, value, "a value without a confirmation time is not used")
	meta := portal.Metadata.(*PortalMetadata)
	meta.ProtectionCheckedAtMS = time.Now().Add(-protectionStaleMaxAge - time.Hour).UnixMilli()
	value, _ = tc.protectionFor(context.Background(), portal)
	require.Nil(t, value, "an expired value is not used")
	tc.protection.remember(key, true, time.Now().Add(-time.Hour))
	value, _ = tc.protectionFor(context.Background(), portal)
	require.NotNil(t, value)
	require.True(t, *value, "the cached value within the TTL wins")
	tc.protection.remember(key, false, time.Now().Add(-protectionStaleMaxAge-time.Minute))
	value, _ = tc.protectionFor(context.Background(), portal)
	require.Nil(t, value)
}

func TestProtectionFloodWaitBacksOff(t *testing.T) {
	rpc := &protectionRPC{fail: tgerr.New(420, "FLOOD_WAIT_30")}
	tc := protectionClient(rpc, false)
	portal := intakePortal(ids.PeerTypeChat, nil)
	_, err := tc.protectionFor(context.Background(), portal)
	require.Error(t, err)
	require.Equal(t, "flood_wait", protectionLookupReason(err))
	for range 5 {
		_, err = tc.protectionFor(context.Background(), portal)
		require.ErrorIs(t, err, errProtectionFloodWait)
	}
	require.EqualValues(t, 1, rpc.calls.Load(), "no RPC while Telegram asks to wait")
}

func TestPortalProtectionConcurrentAccess(t *testing.T) {
	portal := intakePortal(ids.PeerTypeChat, nil)
	tc := &TelegramClient{}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 200 {
				tc.rememberPortalProtection(portal, ptr.Ptr((i+j)%2 == 0))
				setPortalProtection(portal, nil) // never clears
				require.NotNil(t, getPortalProtection(portal))
				_, err := json.Marshal(portal.Metadata)
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.NotNil(t, getPortalProtection(portal))
}
