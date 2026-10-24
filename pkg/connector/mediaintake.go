// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"

	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

const mediaPolicyKey = "pl.personal.chat.telegram.media_policy"

var errRestrictedMedia = errors.New("restricted Telegram media must be opened in Telegram")

// TelegramMediaPolicy is minimal intake metadata. It never includes a file
// reference, thumbnail, media URL, or credentials. Absence of protection
// knowledge is distinct from an explicit false value.
type TelegramMediaPolicy struct {
	Version          int    `json:"version"`
	TransferAllowed  bool   `json:"transfer_allowed"`
	Reason           string `json:"reason,omitempty"`
	TTLSeconds       *int   `json:"ttl_seconds,omitempty"`
	ViewOnce         bool   `json:"view_once"`
	ProtectedContent *bool  `json:"protected_content"`
	LazySupported    bool   `json:"lazy_supported"`
}

func policyTTLSeconds(policy *TelegramMediaPolicy) int {
	if policy.TTLSeconds == nil {
		return 0
	}
	return *policy.TTLSeconds
}

func isRoundMedia(raw tg.MessageMediaClass) bool {
	media, ok := raw.(*tg.MessageMediaDocument)
	if !ok || media == nil {
		return false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok || doc == nil {
		return false
	}
	for _, rawAttr := range doc.Attributes {
		if attr, ok := rawAttr.(*tg.DocumentAttributeVideo); ok && attr.RoundMessage {
			return true
		}
	}
	return false
}

func roundMediaUnread(msg *tg.Message) *bool {
	if !isRoundMedia(msg.Media) {
		return nil
	}
	return ptr.Ptr(msg.MediaUnread)
}

// mediaPolicyForMessage uses the protection decision of the current
// conversion when there is one (see withProtectionDecision), otherwise the
// portal's last known value.
func mediaPolicyForMessage(ctx context.Context, portal *bridgev2.Portal, msg *tg.Message) *TelegramMediaPolicy {
	protection, decided := protectionDecisionFrom(ctx)
	if portal != nil {
		if !decided {
			protection = getPortalProtection(portal)
		}
		// Telegram's chat/channel noforwards flag is not inherited by the
		// message. A private chat has no chat-level noforwards flag.
		if peerType, _, _, err := ids.ParsePortalID(portal.ID); err == nil && peerType == ids.PeerTypeUser {
			protection = ptr.Ptr(false)
		}
	}
	return evaluateMediaPolicy(msg, protection)
}

func evaluateMediaPolicy(msg *tg.Message, protection *bool) *TelegramMediaPolicy {
	policy := &TelegramMediaPolicy{Version: 1, TransferAllowed: true, ProtectedContent: protection}
	if msg == nil {
		policy.TransferAllowed, policy.Reason = false, "unknown_protection"
		return policy
	}
	if msg.Noforwards {
		policy.ProtectedContent = ptr.Ptr(true)
	}
	if ttlMedia, ok := msg.Media.(ttlable); ok {
		if ttl, present := ttlMedia.GetTTLSeconds(); present {
			policy.TTLSeconds = ptr.Ptr(ttl)
			policy.TransferAllowed = false
			switch {
			case ttl == 2147483647:
				policy.ViewOnce, policy.Reason = true, "view_once"
			case ttl > 0:
				policy.Reason = "ttl"
			default:
				// A present but invalid TTL is not ordinary media.
				policy.Reason = "unknown_ttl"
			}
		}
	}
	if policy.Reason == "" && policy.ProtectedContent != nil && *policy.ProtectedContent {
		policy.TransferAllowed, policy.Reason = false, "protected_content"
	} else if policy.Reason == "" && messageNeedsMediaTransfer(msg) && policy.ProtectedContent == nil {
		policy.TransferAllowed, policy.Reason = false, "unknown_protection"
	}
	return policy
}

func messageNeedsMediaTransfer(msg *tg.Message) bool {
	if msg.Media != nil || !msg.RichMessage.Zero() {
		return true
	}
	for _, entity := range msg.Entities {
		if _, ok := entity.(*tg.MessageEntityCustomEmoji); ok {
			return true
		}
	}
	return false
}

// keptTextPolicy is the wire policy of a text part bridged while chat
// protection is unknown or on: displayable, with protected_content null or
// true, so clients keep forward, copy and transcription off.
func keptTextPolicy(policy *TelegramMediaPolicy) *TelegramMediaPolicy {
	kept := &TelegramMediaPolicy{Version: policy.Version, TransferAllowed: true, LazySupported: policy.LazySupported}
	if policy.ProtectedContent != nil {
		kept.ProtectedContent = ptr.Ptr(*policy.ProtectedContent)
	}
	return kept
}

func blockedMediaPart(policy *TelegramMediaPolicy) *bridgev2.ConvertedMessagePart {
	body := "This restricted media can only be opened in Telegram."
	switch policy.Reason {
	case "view_once":
		body = "This view-once media can only be opened in Telegram."
	case "ttl", "unknown_ttl":
		body = "This time-limited media can only be opened in Telegram."
	case "protected_content":
		body = "This protected content can only be opened in Telegram."
	case "unknown_protection":
		body = "Media is unavailable until Telegram content protection has been verified."
	}
	return &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: body},
		Extra:   map[string]any{mediaPolicyKey: policy},
	}
}

// The restricted path returns before rich text, web preview, media hashes,
// thumbnail and file transfer, and leaves the disappearing timer unset.
func blockedMediaMessage(portal *bridgev2.Portal, msg *tg.Message, policy *TelegramMediaPolicy) *bridgev2.ConvertedMessage {
	part := blockedMediaPart(policy)
	part.DBMetadata = &MessageMetadata{
		IsRound: isRoundMedia(msg.Media), MediaUnread: roundMediaUnread(msg),
		NoForwards:      policy.ProtectedContent != nil && *policy.ProtectedContent,
		ProtectionKnown: policy.ProtectedContent != nil,
		TTLSeconds:      policyTTLSeconds(policy), MediaPolicy: policy,
	}
	cm := &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{part}}
	if reply, ok := msg.GetReplyTo(); ok {
		if reply, ok := reply.(*tg.MessageReplyHeader); ok && (reply.ReplyToTopID != 0 || !reply.ForumTopic) && reply.ReplyToTopID != reply.ReplyToMsgID {
			cm.ReplyTo = &networkid.MessageOptionalPartID{}
			if peer, present := reply.GetReplyToPeerID(); present {
				cm.ReplyTo.MessageID = ids.MakeMessageID(peer, reply.ReplyToMsgID)
			} else {
				cm.ReplyTo.MessageID = ids.MakeMessageID(portal.PortalKey, reply.ReplyToMsgID)
			}
		}
	}
	return cm
}

// Direct-media requests must re-evaluate the message and current chat policy;
// an old URL or cached portal cannot grant access to newly protected media.
func safeRefetchedMedia(msg *tg.Message, peerType ids.PeerType, peerID int64, chats []tg.ChatClass) (tg.MessageMediaClass, error) {
	if msg == nil {
		return nil, errRestrictedMedia
	}
	// MessagesGetMessages uses account-global message IDs. Do not let an
	// unrelated message satisfy a direct-media request for another portal.
	var sourceMatches bool
	switch peer := msg.PeerID.(type) {
	case *tg.PeerUser:
		sourceMatches = peerType == ids.PeerTypeUser && peer.UserID == peerID
	case *tg.PeerChat:
		sourceMatches = peerType == ids.PeerTypeChat && peer.ChatID == peerID
	case *tg.PeerChannel:
		sourceMatches = peerType == ids.PeerTypeChannel && peer.ChannelID == peerID
	}
	if !sourceMatches {
		return nil, errRestrictedMedia
	}
	protection := mediaProtectionFromChats(peerType, peerID, chats)
	if policy := evaluateMediaPolicy(msg, protection); !policy.TransferAllowed {
		return nil, errRestrictedMedia
	}
	return msg.Media, nil
}

func mediaProtectionFromChats(peerType ids.PeerType, peerID int64, chats []tg.ChatClass) *bool {
	var protection *bool
	if peerType == ids.PeerTypeUser {
		protection = ptr.Ptr(false)
	}
	for _, rawChat := range chats {
		switch chat := rawChat.(type) {
		case *tg.Chat:
			if peerType == ids.PeerTypeChat && chat.ID == peerID {
				protection = ptr.Ptr(chat.Noforwards)
			}
		case *tg.Channel:
			if peerType == ids.PeerTypeChannel && chat.ID == peerID && !chat.Min {
				protection = ptr.Ptr(chat.Noforwards)
			}
		}
	}
	return protection
}
