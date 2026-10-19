// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func nativeForwardRequest(req *MediaControlRequest, source, destination tg.InputPeerClass, messageID, topicID int, randomID int64) (*tg.MessagesForwardMessagesRequest, error) {
	if req.Destination == nil || req.Destination.AccountID != req.Source.AccountID || randomID == 0 {
		return nil, mediaControlRejected("invalid_destination")
	}
	if req.ScheduleDate != 0 {
		return nil, mediaControlRejected("scheduling_unsupported")
	}
	preserveAuthor := req.PreserveAuthor == nil || *req.PreserveAuthor
	silent := req.Silent != nil && *req.Silent
	return &tg.MessagesForwardMessagesRequest{FromPeer: source, ToPeer: destination, ID: []int{messageID}, RandomID: []int64{randomID}, TopMsgID: topicID, DropAuthor: !preserveAuthor, Silent: silent}, nil
}

func forwardedRoundEcho(updates tg.UpdatesClass, randomID int64, expectedPeer ...tg.InputPeerClass) bool {
	// forwardMessages must provide a correlated remote message, not merely a
	// short send response with media and no destination/random-ID evidence.
	switch updates.(type) {
	case *tg.Updates, *tg.UpdatesCombined:
	default:
		return false
	}
	_, err := canonicalRoundSendUpdates(updates, randomID, expectedPeer...)
	return err == nil
}

func dispatchNativeRoundForward(ctx context.Context, rpc mediaControlRPC, req *MediaControlRequest, source, destination, expected tg.InputPeerClass, messageID, topicID int, randomID int64) (tg.UpdatesClass, error) {
	if err := validateMediaControlAge(req, time.Now()); err != nil {
		return nil, err
	}
	request, err := nativeForwardRequest(req, source, destination, messageID, topicID, randomID)
	if err != nil {
		return nil, err
	}
	updates, err := rpc.MessagesForwardMessages(ctx, request)
	if err != nil {
		return nil, err
	}
	if !forwardedRoundEcho(updates, randomID, expected) {
		return nil, &RoundOperationError{Code: "round_forward_echo_unknown", Certainty: "uncertain", Retriable: true}
	}
	return updates, nil
}

// checkCurrentForwardProtection refreshes source room protection at dispatch,
// independently of cached Matrix metadata. Private message-level protection is
// supplied by the freshly fetched exact Telegram message.
func (tc *TelegramClient) checkCurrentForwardProtection(ctx context.Context, authorized *authorizedMediaControl) error {
	peerType, peerID, _, err := ids.ParsePortalID(authorized.Source.ID)
	if err != nil {
		return err
	}
	switch peerType {
	case ids.PeerTypeUser:
		return nil
	case ids.PeerTypeChat:
		chats, err := tc.client.API().MessagesGetChats(ctx, []int64{peerID})
		if err != nil {
			return err
		}
		for _, raw := range chats.GetChats() {
			if chat, ok := raw.(*tg.Chat); ok && chat.ID == peerID {
				if chat.Noforwards || chat.Left || chat.Deactivated {
					return mediaControlRejected("protected_source")
				}
				return nil
			}
		}
	case ids.PeerTypeChannel:
		channel, err := tc.getSingleChannel(ctx, peerID)
		if err != nil {
			return err
		}
		if channel.Min || channel.Noforwards || channel.Left || channel.Restricted {
			return mediaControlRejected("protected_or_unknown_source")
		}
		return nil
	}
	return mediaControlRejected("source_policy_unknown")
}

func (tc *TelegramClient) forwardMediaControl(ctx context.Context, req *MediaControlRequest, authorized *authorizedMediaControl, randomID int64) error {
	if err := validateMediaControlAge(req, time.Now()); err != nil {
		return err
	}
	source, msg, err := tc.controlSourceMessage(ctx, authorized)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	if msg.Noforwards {
		return mediaControlRejected("protected_source")
	}
	if err = tc.checkCurrentForwardProtection(ctx, authorized); err != nil {
		return classifyRoundOperationError(err, false)
	}
	permission, err := tc.checkRoundPermission(ctx, authorized.Destination)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	if permission.Allowed == nil || !*permission.Allowed {
		return mediaControlRejected("destination_round_forbidden")
	}
	destination, topicID, err := tc.inputPeerForPortalID(ctx, authorized.Destination.ID)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	expected := destination
	if _, isSelf := expected.(*tg.InputPeerSelf); isSelf {
		expected = &tg.InputPeerUser{UserID: tc.telegramUserID}
	}
	updates, err := dispatchNativeRoundForward(ctx, tc.client.API(), req, source, destination, expected, msg.ID, topicID, randomID)
	if err != nil {
		return err
	}
	// Let normal remote conversion persist the echo and round marker. Dispatch
	// after this portal callback returns, avoiding a reentrant queue deadlock.
	go func() {
		updateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if err := tc.updatesManager.Handle(updateCtx, updates); err != nil {
			zerolog.Ctx(updateCtx).Warn().Msg("Failed to process native forward echo")
		}
	}()
	return nil
}
