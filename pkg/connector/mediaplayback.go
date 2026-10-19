// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

type mediaControlRPC interface {
	MessagesGetMessages(context.Context, []tg.InputMessageClass) (tg.MessagesMessagesClass, error)
	ChannelsGetMessages(context.Context, *tg.ChannelsGetMessagesRequest) (tg.MessagesMessagesClass, error)
	MessagesReadMessageContents(context.Context, []int) (*tg.MessagesAffectedMessages, error)
	ChannelsReadMessageContents(context.Context, *tg.ChannelsReadMessageContentsRequest) (bool, error)
	MessagesForwardMessages(context.Context, *tg.MessagesForwardMessagesRequest) (tg.UpdatesClass, error)
}

var _ mediaControlRPC = (*tg.Client)(nil)

// fetchControlMedia checks current remote identity/media without downloading
// bytes, thumbnails, file references, or falling back to a re-upload.
func fetchControlMedia(ctx context.Context, rpc mediaControlRPC, peer tg.InputPeerClass, peerType ids.PeerType, messageID int) (*tg.Message, error) {
	var result tg.MessagesMessagesClass
	var err error
	input := []tg.InputMessageClass{&tg.InputMessageID{ID: messageID}}
	if ch, ok := peer.(*tg.InputPeerChannel); ok {
		result, err = rpc.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash}, ID: input})
	} else {
		result, err = rpc.MessagesGetMessages(ctx, input)
	}
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, mediaControlRejected("source_media_unavailable")
	}
	var messages []tg.MessageClass
	switch res := result.(type) {
	case *tg.MessagesMessages:
		messages = res.Messages
	case *tg.MessagesMessagesSlice:
		messages = res.Messages
	case *tg.MessagesChannelMessages:
		messages = res.Messages
	default:
		return nil, mediaControlRejected("source_media_unavailable")
	}
	for _, raw := range messages {
		msg, ok := raw.(*tg.Message)
		if !ok || msg.ID != messageID {
			continue
		}
		// Nonchannel IDs are per account, but the exact portal still needs a peer
		// check. DM outgoing has destination in PeerID; incoming has sender there.
		if !controlPeerMatches(peer, peerType, msg.PeerID) {
			return nil, mediaControlRejected("source_mapping_mismatch")
		}
		media, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok || media.TTLSeconds != 0 {
			return nil, mediaControlRejected("ttl_media_unavailable")
		}
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			return nil, mediaControlRejected("source_media_unavailable")
		}
		for _, attr := range doc.Attributes {
			if video, ok := attr.(*tg.DocumentAttributeVideo); ok && video.RoundMessage {
				return msg, nil
			}
		}
		return nil, mediaControlRejected("not_round_media")
	}
	return nil, mediaControlRejected("source_media_unavailable")
}

func controlPeerMatches(input tg.InputPeerClass, _ ids.PeerType, peer tg.PeerClass) bool {
	switch p := input.(type) {
	case *tg.InputPeerUser:
		actual, ok := peer.(*tg.PeerUser)
		return ok && actual.UserID == p.UserID
	case *tg.InputPeerChat:
		actual, ok := peer.(*tg.PeerChat)
		return ok && actual.ChatID == p.ChatID
	case *tg.InputPeerChannel:
		actual, ok := peer.(*tg.PeerChannel)
		return ok && actual.ChannelID == p.ChannelID
	case *tg.InputPeerSelf:
		_, ok := peer.(*tg.PeerUser)
		return ok
	default:
		return false
	}
}

func readExactRoundContents(ctx context.Context, rpc mediaControlRPC, peer tg.InputPeerClass, msg *tg.Message, req *MediaControlRequest, now time.Time) error {
	if err := validateMediaControlAge(req, now); err != nil {
		return err
	}
	if req.Manual == nil || !*req.Manual || req.Incognito == nil || req.ExplicitGrant == nil || (*req.Incognito && !*req.ExplicitGrant) {
		return mediaControlRejected("playback_not_authorized")
	}
	if msg.Out {
		return mediaControlRejected("incoming_media_required")
	}
	if !msg.MediaUnread {
		return nil
	}
	if channel, ok := peer.(*tg.InputPeerChannel); ok {
		accepted, err := rpc.ChannelsReadMessageContents(ctx, &tg.ChannelsReadMessageContentsRequest{Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash}, ID: []int{msg.ID}})
		if err != nil {
			return err
		}
		if !accepted {
			return mediaControlRejected("contents_read_rejected")
		}
		return nil
	}
	_, err := rpc.MessagesReadMessageContents(ctx, []int{msg.ID})
	return err
}

func (tc *TelegramClient) controlSourceMessage(ctx context.Context, authorized *authorizedMediaControl) (tg.InputPeerClass, *tg.Message, error) {
	peerType, _, _, err := ids.ParsePortalID(authorized.Source.ID)
	if err != nil {
		return nil, nil, err
	}
	peer, _, err := tc.inputPeerForPortalID(ctx, authorized.Source.ID)
	if err != nil {
		return nil, nil, err
	}
	_, remoteID, err := ids.ParseMessageID(authorized.Message.ID)
	if err != nil {
		return nil, nil, err
	}
	msg, err := fetchControlMedia(ctx, tc.client.API(), peer, peerType, remoteID)
	return peer, msg, err
}

func (tc *TelegramClient) playMediaControl(ctx context.Context, req *MediaControlRequest, authorized *authorizedMediaControl) error {
	peer, msg, err := tc.controlSourceMessage(ctx, authorized)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	if msg.Noforwards {
		return mediaControlRejected("protected_source")
	}
	if err = tc.checkCurrentForwardProtection(ctx, authorized); err != nil {
		return classifyRoundOperationError(err, false)
	}
	if err = readExactRoundContents(ctx, tc.client.API(), peer, msg, req, time.Now()); err != nil {
		return err
	}
	unread := false
	authorized.Message.Metadata.(*MessageMetadata).MediaUnread = &unread
	if err = tc.main.Bridge.DB.Message.Update(ctx, authorized.Message); err != nil {
		return err
	}
	return tc.PublishRoundMediaState(ctx, authorized.Source, authorized.Message, &unread, false, req.RequestID)
}

type RoundMediaStateContent struct {
	Version        int                   `json:"version"`
	AccountID      networkid.UserLoginID `json:"account_id"`
	RoomID         id.RoomID             `json:"room_id"`
	BridgeStateKey string                `json:"bridge_state_key"`
	SourceEventID  id.EventID            `json:"source_event_id"`
	Revision       int64                 `json:"revision"`
	MediaUnread    *bool                 `json:"media_unread"`
	Expired        bool                  `json:"expired"`
	RequestID      string                `json:"request_id,omitempty"`
	ControlResult  *RoundControlResult   `json:"control_result,omitempty"`
}

type RoundControlResult struct {
	RequestID string `json:"request_id"`
	Operation string `json:"operation"`
	Status    string `json:"status"`
	ErrorCode string `json:"error_code"`
	Certainty string `json:"certainty"`
	Retriable bool   `json:"retriable"`
}

func (tc *TelegramConnector) mediaBridgeStateKey() string {
	if tc.Bridge.Config.NoBridgeInfoStateKey {
		return ""
	}
	if unique, ok := tc.Bridge.Matrix.(bridgev2.MatrixConnectorWithBridgeIdentifier); ok {
		return unique.GetUniqueBridgeID()
	}
	return string(tc.Bridge.ID)
}

// PublishRoundMediaState persists revision before publishing an encrypted
// custom event from the exact same bridge bot as its capabilities/bridge-info.
// Official Telegram-device updates use requestID=""; they are not chat reads.
func (tc *TelegramClient) PublishRoundMediaState(ctx context.Context, portal *bridgev2.Portal, msg *database.Message, unread *bool, expired bool, requestID string) error {
	if msg == nil || msg.Room != portal.PortalKey || msg.MXID == "" {
		return mediaControlRejected("source_mapping_mismatch")
	}
	state := &store.RoundMediaState{BridgeID: string(tc.main.Bridge.ID), LoginID: string(tc.loginID), PortalID: string(portal.ID), PortalReceiver: string(portal.Receiver), EventID: string(msg.MXID), MediaUnread: unread, Expired: expired}
	if unread != nil && !*unread {
		state.PlayedAtMS = time.Now().UnixMilli()
		if err := store.NewRoundLedger(tc.main.Store.Database).RememberContentsRead(ctx, string(tc.main.Bridge.ID), string(tc.loginID), string(msg.ID)); err != nil {
			return err
		}
	}
	state, err := store.NewRoundLedger(tc.main.Store.Database).PutMediaState(ctx, state)
	if err != nil {
		return err
	}
	return tc.sendRoundMediaState(ctx, portal, state, requestID)
}

func (tc *TelegramClient) sendRoundMediaState(ctx context.Context, portal *bridgev2.Portal, state *store.RoundMediaState, requestID string) error {
	return tc.sendRoundMediaStateWithResult(ctx, portal, state, requestID, nil)
}

func (tc *TelegramClient) sendRoundMediaStateWithResult(ctx context.Context, portal *bridgev2.Portal, state *store.RoundMediaState, requestID string, result *RoundControlResult) error {
	mx, ok := tc.main.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.Crypto == nil {
		return mediaControlRejected("encryption_required")
	}
	encrypted, err := mx.AS.StateStore.IsEncrypted(ctx, portal.MXID)
	if err != nil {
		return err
	}
	if !encrypted {
		return mediaControlRejected("encryption_required")
	}
	content := RoundMediaStateContent{Version: 1, AccountID: tc.loginID, RoomID: portal.MXID, BridgeStateKey: tc.main.mediaBridgeStateKey(), SourceEventID: id.EventID(state.EventID), Revision: state.Revision, MediaUnread: state.MediaUnread, Expired: state.Expired, RequestID: requestID, ControlResult: result}
	_, err = tc.main.Bridge.Matrix.BotIntent().SendMessage(ctx, portal.MXID, MediaStateEventType, &event.Content{Parsed: &content}, &bridgev2.MatrixSendExtra{})
	if err != nil {
		return fmt.Errorf("publish round media state: %w", err)
	}
	return nil
}

func (tc *TelegramClient) publishRoundControlResult(ctx context.Context, req *MediaControlRequest, authorized *authorizedMediaControl, failure *RoundOperationError) error {
	ledger := store.NewRoundLedger(tc.main.Store.Database)
	state, err := ledger.GetMediaState(ctx, string(tc.main.Bridge.ID), string(tc.loginID), string(authorized.Source.ID), string(authorized.Source.Receiver), string(authorized.Message.MXID))
	if err != nil {
		return err
	}
	if state == nil {
		meta := authorized.Message.Metadata.(*MessageMetadata)
		state = &store.RoundMediaState{BridgeID: string(tc.main.Bridge.ID), LoginID: string(tc.loginID), PortalID: string(authorized.Source.ID), PortalReceiver: string(authorized.Source.Receiver), EventID: string(authorized.Message.MXID), MediaUnread: meta.MediaUnread, Expired: meta.MediaExpired}
	}
	// An operation acknowledgement retains content state. Forward success and
	// failures never manufacture a played value or start a TTL timer.
	state, err = ledger.PutMediaState(ctx, state)
	if err != nil {
		return err
	}
	result := &RoundControlResult{RequestID: req.RequestID, Operation: "played", Status: "accepted", Certainty: "certain"}
	if req.Destination != nil {
		result.Operation = "forward"
	}
	if failure != nil {
		result.ErrorCode = failure.Code
		result.Certainty = failure.Certainty
		result.Retriable = failure.Retriable
		result.Status = "rejected"
		if failure.Certainty == "uncertain" {
			result.Status = "uncertain"
		} else if failure.Retriable {
			result.Status = "retry"
		}
	}
	return tc.sendRoundMediaStateWithResult(ctx, authorized.Source, state, req.RequestID, result)
}
