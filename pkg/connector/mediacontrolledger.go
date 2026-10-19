// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-telegram/pkg/connector/store"
)

type cachedRoundSendResponse struct {
	Message     *database.Message `json:"message"`
	Metadata    *MessageMetadata  `json:"metadata"`
	StreamOrder int64             `json:"stream_order"`
	// TelegramMessageID is the accepted remote message ID (also encoded in
	// Message.ID and StreamOrder), stored explicitly for reconciliation/audit.
	TelegramMessageID int64 `json:"telegram_message_id,omitempty"`
}

// roundDispatchRecord is persisted in the ledger result column right before
// sendMedia. It holds no content or identifiers, only the uploaded file's
// shape, so an uncertain retry can find an already accepted message instead
// of uploading and sending again.
type roundDispatchRecord struct {
	Kind           string `json:"kind"`
	DispatchedAtMS int64  `json:"dispatched_at_ms"`
	Size           int64  `json:"size"`
	DurationMS     int    `json:"duration_ms"`
	Width          int    `json:"width"`
}

const roundDispatchRecordKind = "round_dispatch_v1"

func parseRoundDispatchRecord(result string) *roundDispatchRecord {
	if result == "" {
		return nil
	}
	var record roundDispatchRecord
	if err := json.Unmarshal([]byte(result), &record); err != nil || record.Kind != roundDispatchRecordKind || record.DispatchedAtMS <= 0 {
		return nil
	}
	return &record
}

// RecordRoundDispatch persists the dispatch record under the current claim.
func (tc *TelegramClient) RecordRoundDispatch(ctx context.Context, op *store.RoundOperation, profile roundMediaProfile, now time.Time) (*roundDispatchRecord, error) {
	record := &roundDispatchRecord{Kind: roundDispatchRecordKind, DispatchedAtMS: now.UnixMilli(), Size: profile.Size, DurationMS: profile.DurationMS, Width: profile.Width}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if err = store.NewRoundLedger(tc.main.Store.Database).RecordDispatch(ctx, op, string(data)); err != nil {
		return nil, classifyRoundOperationError(err, false)
	}
	return record, nil
}

// BeginRoundSend reserves identity before upload. A successful cached result
// returns without a second remote dispatch. The caller claims after upload and
// immediately before sendMedia, then caches only a positively round echo.
func (tc *TelegramClient) BeginRoundSend(ctx context.Context, msg *bridgev2.MatrixMessage) (*store.RoundOperation, *bridgev2.MatrixMessageResponse, error) {
	if msg.Event == nil || msg.Event.ID == "" || msg.Portal == nil {
		return nil, nil, mediaControlRejected("round_send_identity_missing")
	}
	options, err := parseRoundSendOptions(msg.Event.Content.Raw)
	if err != nil {
		return nil, nil, err
	}
	payloadHash, err := hashMediaControl(struct {
		PortalID, Receiver string
		Content            any
		ReplyID            string
		Options            RoundSendOptions
	}{string(msg.Portal.ID), string(msg.Portal.Receiver), msg.Content, roundReplyID(msg), options})
	if err != nil {
		return nil, nil, err
	}
	mediaIdentity, err := hashMediaControl(struct {
		URL  any
		File any
		Info any
	}{msg.Content.URL, msg.Content.File, msg.Content.Info})
	if err != nil {
		return nil, nil, err
	}
	op, err := store.NewRoundLedger(tc.main.Store.Database).Reserve(ctx, &store.RoundOperation{RoundOperationKey: tc.operationKey(msg.Portal, "round_send", string(msg.Event.ID)), EventID: string(msg.Event.ID), MediaIdentity: mediaIdentity, PayloadHash: payloadHash})
	if err != nil {
		return nil, nil, classifyRoundOperationError(err, false)
	}
	if op.Status == "permanent" {
		return op, nil, mediaControlRejected(op.ErrorCode)
	}
	if op.Status != "accepted" {
		return op, nil, nil
	}
	var cached cachedRoundSendResponse
	if err = json.Unmarshal([]byte(op.Result), &cached); err != nil || cached.Message == nil || cached.Metadata == nil || !cached.Metadata.IsRound {
		return op, nil, &RoundOperationError{Code: "round_cached_result_invalid", Certainty: "uncertain", Cause: err}
	}
	cached.Message.Metadata = cached.Metadata
	return op, &bridgev2.MatrixMessageResponse{DB: cached.Message, StreamOrder: cached.StreamOrder}, nil
}

func roundReplyID(msg *bridgev2.MatrixMessage) string {
	if msg.ReplyTo != nil {
		return string(msg.ReplyTo.ID)
	}
	return ""
}

func (tc *TelegramClient) ClaimRoundSend(ctx context.Context, op *store.RoundOperation) error {
	claimed, err := store.NewRoundLedger(tc.main.Store.Database).Claim(ctx, op, time.Now(), 5*time.Minute)
	if err != nil {
		return classifyRoundOperationError(err, false)
	}
	if !claimed {
		return classifyRoundOperationError(store.ErrRoundDispatchBusy, true)
	}
	return nil
}

func (tc *TelegramClient) FinishRoundSend(ctx context.Context, op *store.RoundOperation, response *bridgev2.MatrixMessageResponse, sendErr error, dispatched bool) error {
	if op == nil {
		return sendErr
	}
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if sendErr != nil {
		typed := classifyRoundOperationError(sendErr, dispatched)
		state := "permanent"
		if typed.Certainty == "uncertain" {
			state = "uncertain"
		} else if typed.Retriable {
			state = "retry"
		}
		// An uncertain dispatch keeps its dispatch record for reconciliation.
		result := ""
		if state == "uncertain" && parseRoundDispatchRecord(op.Result) != nil {
			result = op.Result
		}
		if err := store.NewRoundLedger(tc.main.Store.Database).Finish(finishCtx, op, state, result, typed.Code); err != nil {
			return classifyRoundOperationError(err, dispatched)
		}
		return typed
	}
	if response == nil || response.DB == nil {
		return &RoundOperationError{Code: "round_send_response_missing", Certainty: "uncertain", Retriable: true}
	}
	meta, ok := response.DB.Metadata.(*MessageMetadata)
	if !ok || !meta.IsRound {
		return &RoundOperationError{Code: "round_send_response_invalid", Certainty: "uncertain", Retriable: true}
	}
	cached, err := json.Marshal(cachedRoundSendResponse{Message: response.DB, Metadata: meta, StreamOrder: response.StreamOrder, TelegramMessageID: response.StreamOrder})
	if err != nil {
		return &RoundOperationError{Code: "round_result_persistence_failed", Certainty: "uncertain", Retriable: true, Cause: err}
	}
	if err = store.NewRoundLedger(tc.main.Store.Database).Finish(finishCtx, op, "accepted", string(cached), ""); err != nil {
		return &RoundOperationError{Code: "round_result_persistence_failed", Certainty: "uncertain", Retriable: true, Cause: fmt.Errorf("cache result: %w", err)}
	}
	return nil
}
