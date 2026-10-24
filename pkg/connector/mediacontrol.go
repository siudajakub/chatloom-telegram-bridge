// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

var MediaPlayedEventType = event.Type{Type: "pl.personal.chat.telegram.media_played", Class: event.MessageEventType}
var MediaForwardEventType = event.Type{Type: "pl.personal.chat.telegram.media_forward", Class: event.MessageEventType}
var MediaStateEventType = event.Type{Type: "pl.personal.chat.telegram.media_state", Class: event.MessageEventType}

const MediaControlMaxAge = 5 * time.Minute

type MediaControlSource struct {
	AccountID networkid.UserLoginID `json:"account_id"`
	RoomID    id.RoomID             `json:"room_id"`
	EventID   id.EventID            `json:"event_id"`
}

type MediaControlDestination struct {
	AccountID networkid.UserLoginID `json:"account_id"`
	RoomID    id.RoomID             `json:"room_id"`
}

type MediaControlRequest struct {
	Version        int                      `json:"version"`
	RequestID      string                   `json:"request_id"`
	CreatedAtMS    int64                    `json:"created_at_ms"`
	Source         MediaControlSource       `json:"source"`
	Destination    *MediaControlDestination `json:"destination,omitempty"`
	Manual         *bool                    `json:"manual,omitempty"`
	Incognito      *bool                    `json:"incognito,omitempty"`
	ExplicitGrant  *bool                    `json:"explicit_grant,omitempty"`
	PreserveAuthor *bool                    `json:"preserve_author,omitempty"`
	Silent         *bool                    `json:"silent,omitempty"`
	ScheduleDate   int64                    `json:"schedule_date,omitempty"`
}

// RoundOperationError distinguishes a positive Telegram rejection from an
// unacknowledged dispatch. An uncertain operation must retain its random ID.
type RoundOperationError struct {
	Code      string
	Certainty string
	Retriable bool
	Cause     error
}

func (e *RoundOperationError) Error() string {
	return "Telegram media operation: " + e.Code + " (" + e.Certainty + ")"
}
func (e *RoundOperationError) Unwrap() error { return e.Cause }

func mediaControlRejected(code string) error {
	return &RoundOperationError{Code: code, Certainty: "certain", Retriable: false}
}

func classifyRoundOperationError(err error, dispatched bool) *RoundOperationError {
	var typed *RoundOperationError
	if errors.As(err, &typed) {
		return typed
	}
	var media *RoundMediaError
	if errors.As(err, &media) {
		return &RoundOperationError{Code: media.Code, Certainty: "certain", Retriable: media.Retriable(), Cause: err}
	}
	if errors.Is(err, store.ErrRoundIdentityChanged) {
		return &RoundOperationError{Code: "identity_changed", Certainty: "certain", Cause: err}
	}
	if errors.Is(err, store.ErrRoundDispatchBusy) {
		return &RoundOperationError{Code: "dispatch_in_progress", Certainty: "uncertain", Retriable: true, Cause: err}
	}
	if rpc, ok := tgerr.As(err); ok {
		// Telegram already accepted an earlier dispatch of this stable random
		// ID. The operation is certainly done; retrying would only loop.
		if rpc.Type == "RANDOM_ID_DUPLICATE" {
			return &RoundOperationError{Code: rpc.Type, Certainty: "certain", Retriable: false, Cause: err}
		}
		// Server errors can leave acceptance unknown. Flood/slow mode are known
		// rejections and retry with the same ID. Paid-message errors never spend.
		if rpc.Code >= 500 {
			return &RoundOperationError{Code: rpc.Type, Certainty: "uncertain", Retriable: true, Cause: err}
		}
		if rpc.Code == 420 || rpc.Type == "SLOWMODE_WAIT" {
			return &RoundOperationError{Code: rpc.Type, Certainty: "certain", Retriable: true, Cause: err}
		}
		return &RoundOperationError{Code: rpc.Type, Certainty: "certain", Retriable: false, Cause: err}
	}
	certainty := "certain"
	if dispatched {
		certainty = "uncertain"
	}
	return &RoundOperationError{Code: "transport_unavailable", Certainty: certainty, Retriable: true, Cause: err}
}

func decodeMediaControl(evt *event.Event, now time.Time) (*MediaControlRequest, error) {
	if evt == nil || (evt.Type != MediaPlayedEventType && evt.Type != MediaForwardEventType) || evt.StateKey != nil {
		return nil, mediaControlRejected("unsupported_control")
	}
	raw, err := json.Marshal(evt.Content.Raw)
	if err != nil {
		return nil, mediaControlRejected("invalid_payload")
	}
	var req MediaControlRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		return nil, mediaControlRejected("invalid_payload")
	}
	if req.Version != 1 || req.RequestID == "" || len(req.RequestID) > 128 || req.Source.AccountID == "" || req.Source.RoomID == "" || req.Source.EventID == "" || req.Source.RoomID != evt.RoomID {
		return nil, mediaControlRejected("invalid_source")
	}
	if err = validateMediaControlAge(&req, now); err != nil {
		return nil, err
	}
	if evt.Type == MediaPlayedEventType {
		if req.Manual == nil || !*req.Manual || req.Incognito == nil || req.ExplicitGrant == nil || (*req.Incognito && !*req.ExplicitGrant) || req.Destination != nil || req.ScheduleDate != 0 {
			return nil, mediaControlRejected("playback_not_authorized")
		}
	} else {
		if req.Destination == nil || req.Destination.RoomID == "" || req.Destination.AccountID != req.Source.AccountID {
			return nil, mediaControlRejected("invalid_destination")
		}
		if req.ScheduleDate != 0 {
			return nil, mediaControlRejected("scheduling_unsupported")
		}
	}
	return &req, nil
}

func validateMediaControlAge(req *MediaControlRequest, now time.Time) error {
	if req.CreatedAtMS <= 0 || now.UnixMilli()-req.CreatedAtMS > MediaControlMaxAge.Milliseconds() || req.CreatedAtMS > now.Add(30*time.Second).UnixMilli() {
		return mediaControlRejected("stale_request")
	}
	return nil
}

// RegisterMediaControlHandlers uses the existing framework's postDecrypt
// dispatch. Custom events never enter HandleMatrixMessage or chat-read logic.
func (tc *TelegramConnector) RegisterMediaControlHandlers() {
	mx, ok := tc.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.EventProcessor == nil {
		return
	}
	mx.EventProcessor.On(MediaPlayedEventType, tc.handleMediaControlEvent)
	mx.EventProcessor.On(MediaForwardEventType, tc.handleMediaControlEvent)
	tc.mediaControlsEnabled = true
}

func validateMediaControlEnvelope(evt *event.Event, mx *matrix.Connector, br *bridgev2.Bridge) error {
	if evt == nil || !evt.Mautrix.WasEncrypted {
		return mediaControlRejected("encryption_required")
	}
	if evt.Mautrix.TrustState < mx.Config.Encryption.VerificationLevels.Send {
		return mediaControlRejected("untrusted_device")
	}
	if evt.Sender == mx.Bot.UserID || br.IsGhostMXID(evt.Sender) {
		return mediaControlRejected("bridge_echo")
	}
	if dp, ok := evt.Content.Raw[appservice.DoublePuppetKey]; ok && dp == mx.AS.DoublePuppetValue {
		return mediaControlRejected("bridge_echo")
	}
	if !br.Config.Permissions.Get(evt.Sender).SendEvents {
		return mediaControlRejected("sender_forbidden")
	}
	return nil
}

type authorizedMediaControl struct {
	Client              *TelegramClient
	Source, Destination *bridgev2.Portal
	Message             *database.Message
}

func (tc *TelegramConnector) authorizeMediaControl(ctx context.Context, evt *event.Event, req *MediaControlRequest) (*authorizedMediaControl, error) {
	user, err := tc.Bridge.GetExistingUserByMXID(ctx, evt.Sender)
	if err != nil {
		return nil, err
	}
	if user == nil || !user.Permissions.SendEvents {
		return nil, mediaControlRejected("sender_forbidden")
	}
	var login *bridgev2.UserLogin
	for _, candidate := range user.GetUserLogins() {
		if candidate.ID == req.Source.AccountID && candidate.UserMXID == evt.Sender {
			login = candidate
			break
		}
	}
	if login == nil || login.Client == nil || !login.Client.IsLoggedIn() {
		return nil, mediaControlRejected("own_login_required")
	}
	client, ok := login.Client.(*TelegramClient)
	if !ok || client.metadata.IsBot {
		return nil, mediaControlRejected("own_login_required")
	}
	source, err := tc.Bridge.GetPortalByMXID(ctx, req.Source.RoomID)
	if err != nil {
		return nil, err
	}
	if err = tc.authorizeControlPortal(ctx, evt.Sender, login, source, evt.Type); err != nil {
		return nil, err
	}
	msg, err := tc.Bridge.DB.Message.GetPartByMXID(ctx, req.Source.EventID)
	if err != nil {
		return nil, err
	}
	if msg == nil || msg.Room != source.PortalKey || msg.MXID != req.Source.EventID {
		return nil, mediaControlRejected("source_mapping_mismatch")
	}
	// From here the sender, its own login and the source mapping are known:
	// rejections are returned with this partial result so the client gets a
	// control_result instead of silence (wave 5 F13). Destination stays the
	// source until it is verified.
	known := &authorizedMediaControl{Client: client, Source: source, Destination: source, Message: msg}
	meta, ok := msg.Metadata.(*MessageMetadata)
	if !ok || !meta.IsRound || meta.TTLSeconds != 0 || meta.MediaExpired || meta.NoForwards || !meta.ProtectionKnown || (meta.MediaPolicy != nil && !meta.MediaPolicy.TransferAllowed) {
		return known, mediaControlRejected("source_media_unavailable")
	}
	result := &authorizedMediaControl{Client: client, Source: source, Destination: source, Message: msg}
	if req.Destination != nil {
		result.Destination, err = tc.Bridge.GetPortalByMXID(ctx, req.Destination.RoomID)
		if err != nil {
			return known, err
		}
		if err = tc.authorizeControlPortal(ctx, evt.Sender, login, result.Destination, event.EventMessage); err != nil {
			return known, err
		}
		if meta.NoForwards || !meta.ProtectionKnown {
			return known, mediaControlRejected("protected_or_unknown_source")
		}
	}
	return result, nil
}

// publishRejectedControl reports a rejection that happened before the ledger
// was reached, when the source mapping is known.
func publishRejectedControl(ctx context.Context, req *MediaControlRequest, known *authorizedMediaControl, err error) {
	if req == nil || known == nil || known.Client == nil || known.Source == nil || known.Message == nil {
		return
	}
	typed := classifyRoundOperationError(err, false)
	if publishErr := known.Client.publishRoundControlResult(ctx, req, known, typed); publishErr != nil {
		zerolog.Ctx(ctx).Debug().Str("media_control_error", typed.Code).Msg("Could not publish Telegram media control rejection")
	}
}

func (tc *TelegramConnector) authorizeControlPortal(ctx context.Context, sender id.UserID, login *bridgev2.UserLogin, portal *bridgev2.Portal, typ event.Type) error {
	if portal == nil || portal.MXID == "" || portal.RoomType == database.RoomTypeSpace || (portal.Receiver != "" && portal.Receiver != login.ID) {
		return mediaControlRejected("portal_forbidden")
	}
	association, err := tc.Bridge.DB.UserPortal.Get(ctx, login.UserLogin, portal.PortalKey)
	if err != nil {
		return err
	}
	if association == nil {
		return mediaControlRejected("portal_not_owned")
	}
	member, err := tc.Bridge.Matrix.GetMemberInfo(ctx, portal.MXID, sender)
	if err != nil {
		return err
	}
	if member == nil || member.Membership != event.MembershipJoin {
		return mediaControlRejected("room_membership_required")
	}
	levels, err := tc.Bridge.Matrix.GetPowerLevels(ctx, portal.MXID)
	if err != nil {
		return err
	}
	if levels == nil || levels.GetUserLevel(sender) < levels.GetEventLevel(typ) {
		return mediaControlRejected("room_permission_denied")
	}
	return nil
}

func (tc *TelegramConnector) handleMediaControlEvent(ctx context.Context, evt *event.Event) {
	mx, ok := tc.Bridge.Matrix.(*matrix.Connector)
	if !ok {
		return
	}
	err := validateMediaControlEnvelope(evt, mx, tc.Bridge)
	var req *MediaControlRequest
	if err == nil {
		req, err = decodeMediaControl(evt, time.Now())
	}
	var authorized *authorizedMediaControl
	if err == nil {
		authorized, err = tc.authorizeMediaControl(ctx, evt, req)
	}
	if err != nil {
		zerolog.Ctx(ctx).Warn().Str("media_control_error", classifyRoundOperationError(err, false).Code).Msg("Rejected Telegram media control")
		publishRejectedControl(ctx, req, authorized, err)
		return
	}
	queued := &queuedMediaControl{Key: authorized.Destination.PortalKey, Login: authorized.Client.userLogin, Run: func(queueCtx context.Context, portal *bridgev2.Portal) {
		// Revalidate after queueing: stale Incognito grants and changed membership
		// never flush later merely because a portal was busy/offline.
		if err := validateMediaControlAge(req, time.Now()); err != nil {
			publishRejectedControl(queueCtx, req, authorized, err)
			return
		}
		fresh, err := tc.authorizeMediaControl(queueCtx, evt, req)
		if err == nil && fresh.Destination.PortalKey != portal.PortalKey {
			err = mediaControlRejected("destination_mapping_changed")
		}
		if err != nil {
			publishRejectedControl(queueCtx, req, fresh, err)
		} else {
			// executeMediaControl publishes its own results.
			err = fresh.Client.executeMediaControl(queueCtx, evt, req, fresh)
		}
		if err != nil {
			zerolog.Ctx(queueCtx).Warn().Str("media_control_error", classifyRoundOperationError(err, false).Code).Msg("Telegram media control failed")
		}
	}}
	result := authorized.Client.userLogin.QueueRemoteEvent(queued)
	if result.Error != nil {
		zerolog.Ctx(ctx).Warn().Str("media_control_error", "portal_queue_failed").Msg("Telegram media control could not be queued")
	}
}

type queuedMediaControl struct {
	Key   networkid.PortalKey
	Login *bridgev2.UserLogin
	Run   func(context.Context, *bridgev2.Portal)
}

var _ bridgev2.RemotePreHandler = (*queuedMediaControl)(nil)

func (q *queuedMediaControl) GetType() bridgev2.RemoteEventType { return bridgev2.RemoteEventUnknown }
func (q *queuedMediaControl) GetPortalKey() networkid.PortalKey { return q.Key }
func (q *queuedMediaControl) AddLogContext(c zerolog.Context) zerolog.Context {
	return c.Str("event_type", "telegram_media_control")
}
func (q *queuedMediaControl) GetSender() bridgev2.EventSender {
	return bridgev2.EventSender{IsFromMe: true, SenderLogin: q.Login.ID}
}
func (q *queuedMediaControl) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	q.Run(ctx, portal)
}

func hashMediaControl(req any) (string, error) {
	raw, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func (tc *TelegramClient) operationKey(portal *bridgev2.Portal, action, requestID string) store.RoundOperationKey {
	return store.RoundOperationKey{BridgeID: string(tc.main.Bridge.ID), LoginID: string(tc.loginID), PortalID: string(portal.ID), PortalReceiver: string(portal.Receiver), Action: action, RequestID: requestID}
}

func (tc *TelegramClient) executeMediaControl(ctx context.Context, evt *event.Event, req *MediaControlRequest, authorized *authorizedMediaControl) error {
	hash, err := hashMediaControl(req)
	if err != nil {
		return err
	}
	ledger := store.NewRoundLedger(tc.main.Store.Database)
	op, err := ledger.Reserve(ctx, &store.RoundOperation{RoundOperationKey: tc.operationKey(authorized.Destination, evt.Type.Type, req.RequestID), EventID: string(evt.ID), MediaIdentity: fmt.Sprintf("%s/%s/%s", req.Source.AccountID, authorized.Source.ID, authorized.Message.ID), PayloadHash: hash})
	if err != nil {
		typed := classifyRoundOperationError(err, false)
		_ = tc.publishRoundControlResult(ctx, req, authorized, typed)
		return typed
	}
	if op.Status == "accepted" {
		return tc.publishRoundControlResult(ctx, req, authorized, nil)
	}
	if op.Status == "permanent" {
		typed := classifyRoundOperationError(mediaControlRejected(op.ErrorCode), false)
		_ = tc.publishRoundControlResult(ctx, req, authorized, typed)
		return typed
	}
	claimed, err := ledger.Claim(ctx, op, time.Now(), 2*time.Minute)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	if !claimed {
		return classifyRoundOperationError(store.ErrRoundDispatchBusy, true)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var playedState *store.RoundMediaState
	if evt.Type == MediaPlayedEventType {
		playedState, err = tc.playMediaControl(rpcCtx, req, authorized)
	} else {
		err = tc.forwardMediaControl(rpcCtx, req, authorized, op.RandomID)
		if tgerr.Is(err, "RANDOM_ID_DUPLICATE") {
			// A retried uncertain forward: Telegram already has the forward
			// with this stable random ID, so it is accepted (wave 5 F13).
			err = nil
		}
	}
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer finishCancel()
	if err != nil {
		typed := classifyRoundOperationError(err, true)
		state := "permanent"
		if typed.Certainty == "uncertain" {
			state = "uncertain"
		} else if typed.Retriable {
			state = "retry"
		}
		if dbErr := ledger.Finish(finishCtx, op, state, "", typed.Code); dbErr != nil {
			return classifyRoundOperationError(dbErr, true)
		}
		_ = tc.publishRoundControlResult(finishCtx, req, authorized, typed)
		return typed
	}
	if err = ledger.Finish(finishCtx, op, "accepted", "", ""); err != nil {
		return classifyRoundOperationError(err, true)
	}
	// Played publishes one media_state carrying both the new state and the
	// result (it used to send two events and bump the revision twice).
	return tc.publishRoundControlResultWithState(finishCtx, req, authorized, nil, playedState)
}
