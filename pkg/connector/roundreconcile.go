// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"crypto/sha256"
	"math"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

const (
	roundEchoFetchTimeout     = 3 * time.Second
	roundReconcileTimeout     = 5 * time.Second
	roundReconcileHistorySize = 30
	// Server dates are seconds and clocks can skew; never look before this
	// margin ahead of the recorded dispatch.
	roundReconcileClockSkew = 60
)

// roundExpectedPeer converts the dispatch peer to the peer Telegram reports on
// the resulting message.
func (tc *TelegramClient) roundExpectedPeer(peer tg.InputPeerClass) tg.InputPeerClass {
	if _, self := peer.(*tg.InputPeerSelf); self {
		return &tg.InputPeerUser{UserID: tc.telegramUserID}
	}
	return peer
}

// fetchRoundEcho reads one message by its acknowledged ID.
func (tc *TelegramClient) fetchRoundEcho(ctx context.Context, peer, expectedPeer tg.InputPeerClass, messageID int) (*tg.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, roundEchoFetchTimeout)
	defer cancel()
	request := []tg.InputMessageClass{&tg.InputMessageID{ID: messageID}}
	var result tg.MessagesMessagesClass
	var err error
	if channel, ok := peer.(*tg.InputPeerChannel); ok {
		result, err = tc.client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash}, ID: request})
	} else {
		result, err = tc.client.API().MessagesGetMessages(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	modified, ok := result.AsModified()
	if !ok {
		return nil, nil
	}
	for _, raw := range modified.GetMessages() {
		if msg, ok := raw.(*tg.Message); ok && msg.ID == messageID && controlPeerMatches(expectedPeer, "", msg.PeerID) {
			return msg, nil
		}
	}
	return nil, nil
}

// verifyAcceptedRoundEcho checks the round profile of an acknowledged send.
// It never fails the send: the message exists in Telegram regardless. When
// the RPC result carries no usable echo it makes at most one getMessages call.
// The returned reason code is "" when the echo is a verified video note.
func (tc *TelegramClient) verifyAcceptedRoundEcho(ctx context.Context, peer, expectedPeer tg.InputPeerClass, ack *roundSendAck) string {
	if ack.Message != nil {
		return roundEchoMessageReason(ack.Message)
	}
	if ack.Media != nil {
		return roundEchoMediaReason(ack.Media)
	}
	msg, err := tc.fetchRoundEcho(ctx, peer, expectedPeer, ack.MessageID)
	if err != nil {
		return "echo_fetch_failed"
	}
	if msg == nil {
		return "echo_fetch_not_found"
	}
	ack.Message = msg
	if ack.Date == 0 {
		ack.Date = msg.Date
	}
	return roundEchoMessageReason(msg)
}

type roundReconcileMatch struct {
	NotBeforeUnix int
	Size          int64
	DurationMS    int
	Width         int
	// RequireExactSize demands the uploaded byte size, used when it is not
	// known whether Telegram accepted the earlier dispatch at all. After
	// RANDOM_ID_DUPLICATE acceptance is certain and a unique shape match is
	// sufficient.
	RequireExactSize bool
}

// findAcceptedRoundMessage looks for our earlier accepted round in recent
// history: outgoing, in the destination, a durable video note of the same
// shape, sent after the recorded dispatch and not yet mapped to any Matrix
// event. Ambiguity is never guessed.
func (tc *TelegramClient) findAcceptedRoundMessage(ctx context.Context, peer, expectedPeer tg.InputPeerClass, portal *bridgev2.Portal, match roundReconcileMatch) (*tg.Message, string) {
	ctx, cancel := context.WithTimeout(ctx, roundReconcileTimeout)
	defer cancel()
	result, err := tc.client.API().MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: roundReconcileHistorySize})
	if err != nil {
		return nil, "reconcile_history_failed"
	}
	modified, ok := result.AsModified()
	if !ok {
		return nil, "reconcile_history_unavailable"
	}
	var shaped, exact []*tg.Message
	for _, raw := range modified.GetMessages() {
		msg, ok := raw.(*tg.Message)
		if !ok || !msg.Out || !controlPeerMatches(expectedPeer, "", msg.PeerID) || msg.Date < match.NotBeforeUnix || roundEchoMediaReason(msg.Media) != "" {
			continue
		}
		doc := msg.Media.(*tg.MessageMediaDocument).Document.(*tg.Document)
		var video *tg.DocumentAttributeVideo
		for _, attr := range doc.Attributes {
			if v, ok := attr.(*tg.DocumentAttributeVideo); ok {
				video = v
			}
		}
		if match.Width > 0 && video.W != match.Width {
			continue
		}
		if match.DurationMS > 0 && math.Abs(video.Duration*1000-float64(match.DurationMS)) > 1000 {
			continue
		}
		if tc.roundMessageAlreadyMapped(ctx, portal, msg.ID) {
			continue
		}
		shaped = append(shaped, msg)
		if match.Size > 0 && doc.Size == match.Size {
			exact = append(exact, msg)
		}
	}
	switch {
	case len(exact) == 1:
		return exact[0], ""
	case len(exact) > 1:
		return nil, "reconcile_ambiguous"
	case match.RequireExactSize:
		return nil, "reconcile_not_found"
	case len(shaped) == 1:
		return shaped[0], ""
	case len(shaped) > 1:
		return nil, "reconcile_ambiguous"
	default:
		return nil, "reconcile_not_found"
	}
}

func (tc *TelegramClient) roundMessageAlreadyMapped(ctx context.Context, portal *bridgev2.Portal, messageID int) bool {
	if portal == nil || tc.main == nil || tc.main.Bridge == nil || tc.main.Bridge.DB == nil {
		return false
	}
	existing, err := tc.main.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, ids.MakeMessageID(portal.PortalKey, messageID))
	// A lookup failure must not make an unmapped message look adoptable twice;
	// treat it as mapped so reconciliation does not guess.
	return err != nil || existing != nil
}

// roundMatrixResponse maps an accepted Telegram message ID like upstream does
// for ordinary media, so the later update-stream echo deduplicates by ID.
func (tc *TelegramClient) roundMatrixResponse(ctx context.Context, msg *bridgev2.MatrixMessage, tgMessageID, tgDate int, media tg.MessageMediaClass) *bridgev2.MatrixMessageResponse {
	hasher := sha256.New()
	hasher.Write([]byte(msg.Content.Body))
	hasher.Write(mediaHashID(ctx, media))
	timestamp := time.Now()
	if tgDate > 0 {
		timestamp = time.Unix(int64(tgDate), 0)
	}
	portalMeta, _ := msg.Portal.Metadata.(*PortalMetadata)
	protectionKnown := portalMeta != nil && portalMeta.ProtectedContent != nil
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        ids.MakeMessageID(msg.Portal.PortalKey, tgMessageID),
			SenderID:  tc.userID,
			Timestamp: timestamp,
			Metadata: &MessageMetadata{
				ContentHash:     hasher.Sum(nil),
				IsRound:         true,
				ProtectionKnown: protectionKnown,
				NoForwards:      protectionKnown && *portalMeta.ProtectedContent,
			},
		},
		StreamOrder: int64(tgMessageID),
	}
}

// logRoundSendOutcome writes one line per round send with phase durations and
// the outcome code. It carries no content, peer, user, event or message IDs.
func logRoundSendOutcome(log *zerolog.Logger, timings *roundSendTimings, total time.Duration, err error) {
	evt := log.Info()
	outcome := "accepted"
	if err != nil {
		evt = log.Warn()
		outcome = classifyRoundOperationError(err, false).Code
	}
	timings.addTo(evt).Int64("total_ms", total.Milliseconds()).Str("outcome", outcome).Msg("Telegram round send phases")
}

// reconcileDuplicateRoundSend handles RANDOM_ID_DUPLICATE: Telegram certainly
// accepted an earlier dispatch of this stable random ID. Map that message when
// it can be identified, otherwise report a certain, non-retriable outcome so
// neither the bridge nor the client sends it again.
func (tc *TelegramClient) reconcileDuplicateRoundSend(ctx context.Context, msg *bridgev2.MatrixMessage, peer, expectedPeer tg.InputPeerClass, previous *roundDispatchRecord, profile roundMediaProfile, timings *roundSendTimings, sendErr error) (*bridgev2.MatrixMessageResponse, error) {
	log := zerolog.Ctx(ctx)
	match := roundReconcileMatch{Size: profile.Size, DurationMS: profile.DurationMS, Width: profile.Width}
	if previous != nil {
		match.NotBeforeUnix = int(previous.DispatchedAtMS/1000) - roundReconcileClockSkew
		if previous.Size > 0 {
			match.Size = previous.Size
		}
	}
	phase := time.Now()
	found, reason := tc.findAcceptedRoundMessage(ctx, peer, expectedPeer, msg.Portal, match)
	timings.Reconcile += time.Since(phase)
	if found != nil {
		log.Info().Str("reconcile_result", "found").Msg("Telegram reported RANDOM_ID_DUPLICATE, mapped the earlier accepted round message")
		return tc.roundMatrixResponse(ctx, msg, found.ID, found.Date, found.Media), nil
	}
	log.Warn().Str("reason_code", reason).Msg("Telegram reported RANDOM_ID_DUPLICATE but the earlier round message was not identified")
	return nil, &RoundOperationError{Code: "round_already_sent_unmapped", Certainty: "certain", Retriable: false, Cause: sendErr}
}
