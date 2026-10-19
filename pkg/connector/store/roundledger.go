// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"go.mau.fi/util/dbutil"
)

var ErrRoundIdentityChanged = errors.New("round operation identity reused with different payload")
var ErrRoundDispatchBusy = errors.New("round operation dispatch is in progress")

type RoundOperationKey struct {
	BridgeID, LoginID, PortalID, PortalReceiver, Action, RequestID string
}

type RoundOperation struct {
	RoundOperationKey
	EventID, MediaIdentity, PayloadHash string
	RandomID                            int64
	Status, Result, ErrorCode           string
	ClaimToken                          string
	LeaseUntilMS, UpdatedAtMS           int64
}

type RoundLedger struct{ DB *dbutil.Database }

func NewRoundLedger(db *dbutil.Database) *RoundLedger { return &RoundLedger{DB: db} }

func (k RoundOperationKey) args() []any {
	return []any{k.BridgeID, k.LoginID, k.PortalID, k.PortalReceiver, k.Action, k.RequestID}
}

const roundOperationWhere = `bridge_id=$1 AND login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND action=$5 AND request_id=$6`

func (q *RoundLedger) Get(ctx context.Context, key RoundOperationKey) (*RoundOperation, error) {
	o := &RoundOperation{RoundOperationKey: key}
	err := q.DB.QueryRow(ctx, `SELECT event_id, media_identity, payload_hash, random_id, status, result, error_code, claim_token, lease_until_ms, updated_at_ms FROM telegram_round_operation WHERE `+roundOperationWhere, key.args()...).Scan(
		&o.EventID, &o.MediaIdentity, &o.PayloadHash, &o.RandomID, &o.Status, &o.Result, &o.ErrorCode, &o.ClaimToken, &o.LeaseUntilMS, &o.UpdatedAtMS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

// Reserve persists a nonzero Telegram random ID before any remote dispatch. Both
// the Matrix event and client request identities are immutable, including target.
func (q *RoundLedger) Reserve(ctx context.Context, candidate *RoundOperation) (*RoundOperation, error) {
	if candidate.RequestID == "" || candidate.EventID == "" || candidate.PayloadHash == "" || candidate.MediaIdentity == "" {
		return nil, fmt.Errorf("incomplete round operation identity")
	}
	if candidate.RandomID == 0 {
		var randomBytes [8]byte
		if _, err := rand.Read(randomBytes[:]); err != nil {
			return nil, err
		}
		candidate.RandomID = int64(binary.BigEndian.Uint64(randomBytes[:]) & ((1 << 63) - 1))
		if candidate.RandomID == 0 {
			candidate.RandomID = 1
		}
	}
	now := time.Now().UnixMilli()
	args := append(candidate.args(), candidate.EventID, candidate.MediaIdentity, candidate.PayloadHash, candidate.RandomID, now)
	_, err := q.DB.Exec(ctx, `INSERT INTO telegram_round_operation (bridge_id,login_id,portal_id,portal_receiver,action,request_id,event_id,media_identity,payload_hash,random_id,status,updated_at_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'prepared',$11) ON CONFLICT DO NOTHING`, args...)
	if err != nil {
		return nil, err
	}
	stored, err := q.Get(ctx, candidate.RoundOperationKey)
	if err != nil {
		return nil, err
	}
	if stored == nil || stored.MediaIdentity != candidate.MediaIdentity || stored.PayloadHash != candidate.PayloadHash {
		return nil, ErrRoundIdentityChanged
	}
	return stored, nil
}

// Claim atomically prevents concurrent dispatch. A crashed/unknown attempt can
// be retried only after its bounded RPC lease, with the previously persisted ID.
func (q *RoundLedger) Claim(ctx context.Context, o *RoundOperation, now time.Time, lease time.Duration) (bool, error) {
	var tokenBytes [16]byte
	if _, err := rand.Read(tokenBytes[:]); err != nil {
		return false, err
	}
	token := hex.EncodeToString(tokenBytes[:])
	args := append(o.args(), now.Add(lease).UnixMilli(), now.UnixMilli(), token)
	res, err := q.DB.Exec(ctx, `UPDATE telegram_round_operation SET status='dispatching',lease_until_ms=$7,updated_at_ms=$8,claim_token=$9 WHERE `+roundOperationWhere+` AND (status IN ('prepared','retry','uncertain') OR (status='dispatching' AND lease_until_ms<$8))`, args...)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 1 {
		o.LeaseUntilMS = now.Add(lease).UnixMilli()
		o.ClaimToken = token
	}
	return rows == 1, err
}

// RecordDispatch stores a dispatch record in the result column of the claimed
// operation, immediately before the remote RPC. It survives an uncertain
// finish so a retry can reconcile with Telegram instead of resending.
func (q *RoundLedger) RecordDispatch(ctx context.Context, o *RoundOperation, result string) error {
	args := append(o.args(), result, time.Now().UnixMilli(), o.LeaseUntilMS, o.ClaimToken)
	res, err := q.DB.Exec(ctx, `UPDATE telegram_round_operation SET result=$7,updated_at_ms=$8 WHERE `+roundOperationWhere+` AND status='dispatching' AND lease_until_ms=$9 AND claim_token=$10`, args...)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err == nil && rows != 1 {
		return ErrRoundDispatchBusy
	}
	if err == nil {
		o.Result = result
	}
	return err
}

func (q *RoundLedger) Finish(ctx context.Context, o *RoundOperation, status, result, errorCode string) error {
	args := append(o.args(), status, result, errorCode, time.Now().UnixMilli(), o.LeaseUntilMS, o.ClaimToken)
	res, err := q.DB.Exec(ctx, `UPDATE telegram_round_operation SET status=$7,result=$8,error_code=$9,lease_until_ms=0,updated_at_ms=$10 WHERE `+roundOperationWhere+` AND status='dispatching' AND lease_until_ms=$11 AND claim_token=$12`, args...)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err == nil && rows != 1 {
		return ErrRoundDispatchBusy
	}
	return err
}

type RoundMediaState struct {
	BridgeID, LoginID, PortalID, PortalReceiver, EventID string
	Revision                                             int64
	MediaUnread                                          *bool
	PlayedAtMS                                           int64
	Expired                                              bool
}

func (q *RoundLedger) GetMediaState(ctx context.Context, bridgeID, loginID, portalID, receiver, eventID string) (*RoundMediaState, error) {
	state := &RoundMediaState{BridgeID: bridgeID, LoginID: loginID, PortalID: portalID, PortalReceiver: receiver, EventID: eventID}
	err := q.DB.QueryRow(ctx, `SELECT revision,media_unread,played_at_ms,expired FROM telegram_round_media_state WHERE bridge_id=$1 AND login_id=$2 AND portal_id=$3 AND portal_receiver=$4 AND event_id=$5`, bridgeID, loginID, portalID, receiver, eventID).Scan(&state.Revision, &state.MediaUnread, &state.PlayedAtMS, &state.Expired)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return state, err
}

// PutMediaState allocates the revision in SQL, independent of event cache or a
// restart. Callers publish only this returned revision after a successful RPC.
func (q *RoundLedger) PutMediaState(ctx context.Context, state *RoundMediaState) (*RoundMediaState, error) {
	// Content played and expiry cannot be reversed by a stale history snapshot
	// or a control ACK read in a different portal's queue. Return SQL's actual
	// merged values so the published revision never contradicts durable state.
	err := q.DB.QueryRow(ctx, `INSERT INTO telegram_round_media_state (bridge_id,login_id,portal_id,portal_receiver,event_id,revision,media_unread,played_at_ms,expired) VALUES ($1,$2,$3,$4,$5,1,$6,$7,$8) ON CONFLICT (bridge_id,login_id,portal_id,portal_receiver,event_id) DO UPDATE SET revision=telegram_round_media_state.revision+1,media_unread=CASE WHEN telegram_round_media_state.media_unread=false THEN false ELSE $6 END,played_at_ms=CASE WHEN telegram_round_media_state.played_at_ms>0 THEN telegram_round_media_state.played_at_ms ELSE $7 END,expired=(telegram_round_media_state.expired OR $8) RETURNING revision,media_unread,played_at_ms,expired`, state.BridgeID, state.LoginID, state.PortalID, state.PortalReceiver, state.EventID, state.MediaUnread, state.PlayedAtMS, state.Expired).Scan(&state.Revision, &state.MediaUnread, &state.PlayedAtMS, &state.Expired)
	return state, err
}
