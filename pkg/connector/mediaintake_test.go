// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func intakePortal(peerType ids.PeerType, protection *bool) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: ids.InternalMakePortalKey(peerType, 123, 0, "456"),
		MXID:      "!fixture:example.invalid", Metadata: &PortalMetadata{IsSuperGroup: true, ProtectedContent: protection},
	}}
}

func intakeDocument(round bool, ttl *int) *tg.MessageMediaDocument {
	doc := &tg.Document{ID: 12, MimeType: "video/mp4", Size: 1024, Attributes: []tg.DocumentAttributeClass{
		&tg.DocumentAttributeVideo{RoundMessage: round, Duration: 5.125, W: 240, H: 240},
	}}
	doc.SetThumbs([]tg.PhotoSizeClass{&tg.PhotoSize{Type: "m", W: 128, H: 128, Size: 100}})
	m := &tg.MessageMediaDocument{Document: doc}
	if ttl != nil {
		m.SetTTLSeconds(*ttl)
	}
	return m
}

func TestRestrictedMediaNoTransferOrTimers(t *testing.T) {
	for _, test := range []struct {
		name       string
		media      tg.MessageMediaClass
		noForwards bool
		protection *bool
		reason     string
		// keepText: since wave 5 (F1/F15) protected and unknown-protection
		// messages bridge their text when the client is logged in; the
		// media part stays a notice without any transfer.
		keepText bool
	}{
		{"ttl_round", intakeDocument(true, ptr.Ptr(30)), false, ptr.Ptr(false), "ttl", false},
		{"ttl_ordinary", intakeDocument(false, ptr.Ptr(1)), false, ptr.Ptr(false), "ttl", false},
		{"view_once_round", intakeDocument(true, ptr.Ptr(2147483647)), false, ptr.Ptr(false), "view_once", false},
		{"unknown_zero_ttl", intakeDocument(true, ptr.Ptr(0)), false, ptr.Ptr(false), "unknown_ttl", false},
		{"unknown_negative_ttl", intakeDocument(true, ptr.Ptr(-1)), false, ptr.Ptr(false), "unknown_ttl", false},
		{"message_protected", intakeDocument(true, nil), true, ptr.Ptr(false), "protected_content", true},
		{"chat_protected", intakeDocument(false, nil), false, ptr.Ptr(true), "protected_content", false},
		{"unknown_chat", intakeDocument(true, nil), false, nil, "unknown_protection", false},
		{"protected_web_preview", &tg.MessageMediaWebPage{Webpage: &tg.WebPage{URL: "https://example.invalid", Photo: &tg.Photo{ID: 12}}}, true, ptr.Ptr(false), "protected_content", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			portal := intakePortal(ids.PeerTypeChannel, test.protection)
			msg := &tg.Message{ID: 7, Noforwards: test.noForwards, Message: "caption must not leak", MediaUnread: true}
			msg.SetMedia(test.media)
			msg.SetReplyTo(&tg.MessageReplyHeader{ReplyToMsgID: 5})
			// There is no MTProto invoker, store, Matrix API or downloader.
			// TTL/noforwards also test with a non-nil client to prove the
			// central policy refresh cannot issue even a metadata RPC.
			client := &TelegramClient{}
			if ttl, ok := msg.Media.(ttlable); ok {
				if _, present := ttl.GetTTLSeconds(); present {
					client.client = &telegram.Client{}
				}
			}
			if msg.Noforwards {
				client.client = &telegram.Client{}
			}
			converted, err := client.convertToMatrix(context.Background(), portal, nil, msg)
			require.NoError(t, err)
			if test.keepText {
				require.Equal(t, event.MsgText, converted.Parts[0].Content.MsgType)
				require.Equal(t, msg.Message, converted.Parts[0].Content.Body)
				require.Empty(t, converted.Parts[0].Content.BeeperLinkPreviews)
				require.NotNil(t, converted.ReplyTo)
				for _, part := range converted.Parts[1:] {
					require.Equal(t, event.MsgNotice, part.Content.MsgType)
					require.Nil(t, part.Content.Info)
				}
				data, err := json.Marshal(converted)
				require.NoError(t, err)
				for _, forbidden := range []string{"mxc://", "file_reference", "thumbnail_url"} {
					require.NotContains(t, string(data), forbidden)
				}
				return
			}
			require.Len(t, converted.Parts, 1)
			require.Zero(t, converted.Disappear.Timer)
			require.Empty(t, converted.Disappear.Type)
			require.NotNil(t, converted.ReplyTo)
			part := converted.Parts[0]
			require.Equal(t, event.MsgNotice, part.Content.MsgType)
			require.Empty(t, part.Content.URL)
			require.Nil(t, part.Content.File)
			require.Nil(t, part.Content.Info)
			require.Empty(t, part.Content.BeeperLinkPreviews)
			require.NotContains(t, part.Content.Body, msg.Message)
			meta := part.DBMetadata.(*MessageMetadata)
			require.Equal(t, isRoundMedia(test.media), meta.IsRound)
			require.Equal(t, test.reason, meta.MediaPolicy.Reason)
			require.False(t, meta.MediaPolicy.TransferAllowed)
			require.False(t, meta.MediaPolicy.LazySupported)
			require.Empty(t, meta.ContentURI)
			data, err := json.Marshal(part)
			require.NoError(t, err)
			for _, forbidden := range []string{"mxc://", "file_reference", "thumbnail_url", "caption must not leak"} {
				require.NotContains(t, string(data), forbidden)
			}
		})
	}
}

func TestRestrictedConversionEntrypointsCannotBypassGuard(t *testing.T) {
	portal := intakePortal(ids.PeerTypeUser, nil)
	media := intakeDocument(true, ptr.Ptr(60))
	msg := &tg.Message{ID: 7}
	msg.SetMedia(media)
	client := &TelegramClient{}
	part, timer, hash := client.mediaToMatrix(context.Background(), portal, nil, msg)
	require.Equal(t, event.MsgNotice, part.Content.MsgType)
	require.Nil(t, timer)
	require.Nil(t, hash)
	part, timer = client.convertMediaRequiringUpload(context.Background(), portal, nil, msg, media, true)
	require.Equal(t, event.MsgNotice, part.Content.MsgType)
	require.Nil(t, timer)
	preview, err := client.webpageToBeeperLinkPreview(context.Background(), portal, nil, msg, &tg.MessageMediaWebPage{})
	require.ErrorIs(t, err, errRestrictedMedia)
	require.Nil(t, preview)
}

// Wave 5: protected plain text is shown (F15, approved by J 2026-10-22) and
// text with custom emoji of unknown protection keeps its fallback text (F1).
// Neither exports a custom emoji document. A logged-out client keeps the old
// full block.
func TestProtectedTextAndUnknownCustomEmojiExportNoMedia(t *testing.T) {
	for _, test := range []struct {
		name       string
		protection *bool
		entities   []tg.MessageEntityClass
		protected  *bool
	}{
		{"protected_plain_text", ptr.Ptr(true), nil, ptr.Ptr(true)},
		{"unknown_custom_emoji", nil, []tg.MessageEntityClass{&tg.MessageEntityCustomEmoji{DocumentID: 12}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := &tg.Message{ID: 7, Message: "content is text", Entities: test.entities}
			msg.SetTTLPeriod(30)
			rpc := &protectionRPC{fail: errors.New("synthetic protection lookup failure")}
			client := protectionClient(rpc, false)
			converted, err := client.convertToMatrix(context.Background(), intakePortal(ids.PeerTypeChat, test.protection), nil, msg)
			require.NoError(t, err)
			require.Len(t, converted.Parts, 1)
			require.Equal(t, event.MsgText, converted.Parts[0].Content.MsgType)
			require.Equal(t, msg.Message, converted.Parts[0].Content.Body)
			policy := converted.Parts[0].Extra[mediaPolicyKey].(*TelegramMediaPolicy)
			require.True(t, policy.TransferAllowed)
			require.Equal(t, test.protected, policy.ProtectedContent)
			require.Equal(t, 30*time.Second, converted.Disappear.Timer)
			data, err := json.Marshal(converted)
			require.NoError(t, err)
			require.NotContains(t, string(data), "mxc://")

			loggedOut, err := (&TelegramClient{}).convertToMatrix(context.Background(), intakePortal(ids.PeerTypeChat, test.protection), nil, msg)
			require.NoError(t, err)
			require.Equal(t, event.MsgNotice, loggedOut.Parts[0].Content.MsgType)
			require.NotContains(t, loggedOut.Parts[0].Content.Body, msg.Message)
			require.Zero(t, loggedOut.Disappear.Timer)
		})
	}
}

func TestPhotoTTLGuardBeforeThumbnail(t *testing.T) {
	for _, ttl := range []int{0, 15, 2147483647} {
		photo := &tg.MessageMediaPhoto{Photo: &tg.Photo{ID: 12}, Video: &tg.Document{ID: 13}}
		photo.SetTTLSeconds(ttl)
		msg := &tg.Message{Media: photo}
		converted, err := (&TelegramClient{client: &telegram.Client{}}).convertToMatrix(context.Background(), intakePortal(ids.PeerTypeUser, nil), nil, msg)
		require.NoError(t, err)
		require.Equal(t, event.MsgNotice, converted.Parts[0].Content.MsgType)
		require.Nil(t, converted.Parts[0].Content.Info)
		require.Zero(t, converted.Disappear.Timer)
	}
}

type intakeMatrix struct {
	bridgev2.MatrixConnector
	urls int
}

func (m *intakeMatrix) GenerateContentURI(_ context.Context, _ networkid.MediaID) (id.ContentURIString, error) {
	m.urls++
	return "mxc://example.invalid/synthetic", nil
}

func TestNormalAndRoundHistoryConversionPreservesMediaAndReply(t *testing.T) {
	for _, round := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "round"}[round], func(t *testing.T) {
			matrix := &intakeMatrix{}
			bridge := &bridgev2.Bridge{Matrix: matrix}
			portal := intakePortal(ids.PeerTypeUser, nil)
			portal.Bridge = bridge
			client := &TelegramClient{
				main: &TelegramConnector{Bridge: bridge, useDirectMedia: true},
				// A zero client has no MTProto invoker. URL generation must not
				// issue a download or a played/content-read RPC.
				client: &telegram.Client{}, telegramUserID: 456,
			}
			msg := &tg.Message{ID: 7, MediaUnread: true, Message: "caption"}
			msg.SetMedia(intakeDocument(round, nil))
			msg.SetReplyTo(&tg.MessageReplyHeader{ReplyToMsgID: 5})
			// Backfill and live ingestion share this exact converter.
			for range 2 {
				converted, err := client.convertToMatrix(context.Background(), portal, nil, msg)
				require.NoError(t, err)
				require.Len(t, converted.Parts, 1)
				part := converted.Parts[0]
				require.Equal(t, event.MsgVideo, part.Content.MsgType)
				require.Equal(t, "caption", part.Content.Body)
				require.Equal(t, id.ContentURIString("mxc://example.invalid/synthetic"), part.Content.URL)
				require.Equal(t, 5125, part.Content.Info.Duration)
				require.Equal(t, 240, part.Content.Info.Width)
				require.Equal(t, round, part.Content.Info.Extra["fi.mau.telegram.round_message"] == true)
				require.Equal(t, ids.MakeMessageID(portal.PortalKey, 5), converted.ReplyTo.MessageID)
				meta := part.DBMetadata.(*MessageMetadata)
				require.Equal(t, round, meta.IsRound)
				require.True(t, meta.ProtectionKnown)
				require.False(t, meta.NoForwards)
				require.True(t, meta.MediaPolicy.TransferAllowed)
				if round {
					require.NotNil(t, meta.MediaUnread)
					require.True(t, *meta.MediaUnread)
				} else {
					require.Nil(t, meta.MediaUnread)
				}
				require.Zero(t, converted.Disappear.Timer)
			}
			require.Equal(t, 4, matrix.urls) // media + thumbnail per conversion
			require.True(t, msg.MediaUnread)
		})
	}
}

func TestDirectRefetchPolicyBeforeByteTransfer(t *testing.T) {
	msg := &tg.Message{Media: intakeDocument(true, nil)}
	for _, test := range []struct {
		name       string
		peer       ids.PeerType
		chats      []tg.ChatClass
		noForwards bool
		blocked    bool
	}{
		{"private_ordinary", ids.PeerTypeUser, nil, false, false},
		{"private_protected", ids.PeerTypeUser, nil, true, true},
		{"group_unprotected", ids.PeerTypeChat, []tg.ChatClass{&tg.Chat{ID: 123}}, false, false},
		{"group_protected", ids.PeerTypeChat, []tg.ChatClass{&tg.Chat{ID: 123, Noforwards: true}}, false, true},
		{"group_unknown", ids.PeerTypeChat, nil, false, true},
		{"channel_protected", ids.PeerTypeChannel, []tg.ChatClass{&tg.Channel{ID: 123, Noforwards: true}}, false, true},
		{"channel_unprotected", ids.PeerTypeChannel, []tg.ChatClass{&tg.Channel{ID: 123}}, false, false},
		{"channel_unknown", ids.PeerTypeChannel, nil, false, true},
		{"channel_min_unknown", ids.PeerTypeChannel, []tg.ChatClass{&tg.Channel{ID: 123, Min: true}}, false, true},
		{"wrong_chat_policy", ids.PeerTypeChannel, []tg.ChatClass{&tg.Channel{ID: 321}}, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg.Noforwards = test.noForwards
			switch test.peer {
			case ids.PeerTypeUser:
				msg.PeerID = &tg.PeerUser{UserID: 123}
			case ids.PeerTypeChat:
				msg.PeerID = &tg.PeerChat{ChatID: 123}
			case ids.PeerTypeChannel:
				msg.PeerID = &tg.PeerChannel{ChannelID: 123}
			}
			media, err := safeRefetchedMedia(msg, test.peer, 123, test.chats)
			if test.blocked {
				require.ErrorIs(t, err, errRestrictedMedia)
				require.Nil(t, media)
			} else {
				require.NoError(t, err)
				require.Same(t, msg.Media, media)
			}
		})
	}
	for _, ttl := range []int{0, -1, 60, 2147483647} {
		msg.Noforwards = false
		msg.PeerID = &tg.PeerUser{UserID: 123}
		msg.Media = intakeDocument(true, &ttl)
		media, err := safeRefetchedMedia(msg, ids.PeerTypeUser, 123, nil)
		require.ErrorIs(t, err, errRestrictedMedia)
		require.Nil(t, media)
	}
	msg.Media = intakeDocument(true, nil)
	msg.PeerID = &tg.PeerUser{UserID: 321}
	media, err := safeRefetchedMedia(msg, ids.PeerTypeUser, 123, nil)
	require.ErrorIs(t, err, errRestrictedMedia)
	require.Nil(t, media)
}

func TestMediaPolicyJSONKeepsUnknownDistinct(t *testing.T) {
	policy := evaluateMediaPolicy(&tg.Message{Media: intakeDocument(true, nil)}, nil)
	data, err := json.Marshal(policy)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(data), `"protected_content":null`))
	require.NotContains(t, string(data), "ttl_seconds")
	require.Contains(t, string(data), `"transfer_allowed":false`)
	require.Contains(t, string(data), `"lazy_supported":false`)
}
