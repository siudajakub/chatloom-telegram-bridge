// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/util/exsync"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

// F5: the hash stored for an own sent round equals the hash the incoming
// conversion computes for the same Telegram message, so its first remote
// edit is ignored instead of re-uploading the video.
func TestRoundSendHashMatchesIncomingConversion(t *testing.T) {
	matrix := &intakeMatrix{}
	bridge := &bridgev2.Bridge{Matrix: matrix}
	portal := intakePortal(ids.PeerTypeUser, nil)
	portal.Bridge = bridge
	tc := &TelegramClient{main: &TelegramConnector{Bridge: bridge, useDirectMedia: true}, client: &telegram.Client{}, telegramUserID: 456}
	echo := &tg.Message{ID: 7, Out: true}
	echo.SetMedia(intakeDocument(true, nil))
	converted, err := tc.convertToMatrix(context.Background(), portal, nil, echo)
	require.NoError(t, err)
	incoming := converted.Parts[0].DBMetadata.(*MessageMetadata).ContentHash
	msg := &bridgev2.MatrixMessage{MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
		Portal: portal, Content: syntheticRoundContent(true),
	}}
	sent := tc.roundMatrixResponse(context.Background(), msg, echo.ID, 100, echo.Message, echo.Media)
	require.Equal(t, incoming, sent.DB.Metadata.(*MessageMetadata).ContentHash)
	// The Matrix filename is not part of the hash any more.
	require.NotEqual(t, roundContentHash(context.Background(), msg.Content.Body, echo.Media), incoming)
}

// F7: a DM chat-info sync performs no users.getFullUser.
func TestDMChatInfoDoesNotCallGetFullUser(t *testing.T) {
	rpc := &protectionRPC{}
	initialized, done := exsync.NewEvent(), exsync.NewEvent()
	initialized.Set()
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "456"}}
	tc := &TelegramClient{
		main: &TelegramConnector{}, client: rpc.client(), userLogin: login, loginID: login.ID, telegramUserID: 456,
		clientInitialized: initialized, clientDone: done,
		metadata: &UserLoginMetadata{Session: UserLoginSession{AuthKey: bytes.Repeat([]byte{0x42}, 256)}},
	}
	info, err := tc.getDMChatInfo(789)
	require.NoError(t, err)
	portal := intakePortal(ids.PeerTypeUser, nil)
	require.True(t, info.ExtraUpdates(context.Background(), portal))
	require.Zero(t, rpc.calls.Load())
	require.False(t, *getPortalProtection(portal))
	require.Nil(t, getRoundPermission(portal, "456"), "permission stays unknown until the lazy refresh")
}

// F7: a permission confirmed within a minute skips the first check.
func TestFreshAllowedRoundPermission(t *testing.T) {
	portal := intakePortal(ids.PeerTypeChat, nil)
	now := time.Now()
	require.Nil(t, freshAllowedRoundPermission(portal, "456", now))
	allowed, denied := true, false
	setRoundPermission(portal, "456", &RoundPermission{Allowed: &allowed, CheckedAtMS: now.Add(-30 * time.Second).UnixMilli()})
	require.NotNil(t, freshAllowedRoundPermission(portal, "456", now))
	setRoundPermission(portal, "456", &RoundPermission{Allowed: &allowed, CheckedAtMS: now.Add(-2 * time.Minute).UnixMilli()})
	require.Nil(t, freshAllowedRoundPermission(portal, "456", now))
	setRoundPermission(portal, "456", &RoundPermission{Allowed: &denied, CheckedAtMS: now.UnixMilli()})
	require.Nil(t, freshAllowedRoundPermission(portal, "456", now), "a cached denial is re-checked")
}

// F10: crash leftovers are swept, live and foreign entries are kept.
func TestSweepRoundTempFiles(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-2 * time.Hour)
	for _, name := range []string{"telegram-round-abc", "telegram-round-download-1", "other-file"} {
		path := filepath.Join(dir, name)
		if name == "telegram-round-abc" {
			require.NoError(t, os.Mkdir(path, 0700))
			require.NoError(t, os.WriteFile(filepath.Join(path, "round.mp4"), []byte("x"), 0600))
		} else {
			require.NoError(t, os.WriteFile(path, []byte("x"), 0600))
		}
		require.NoError(t, os.Chtimes(path, old, old))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "telegram-round-live"), []byte("x"), 0600))
	require.Equal(t, 2, sweepRoundTempFiles(dir, time.Hour, time.Now()))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{"other-file", "telegram-round-live"}, names)
}

func TestRoundTempDirFromEnv(t *testing.T) {
	defer func() { roundTempDirectory = "" }()
	dir := t.TempDir()
	t.Setenv(roundTempDirEnv, dir)
	require.Equal(t, dir, roundTempDirFromEnv())
	require.Equal(t, dir, roundTempDir())
	t.Setenv(roundTempDirEnv, filepath.Join(dir, "missing"))
	require.Empty(t, roundTempDirFromEnv())
}

// TF-04: a tool that floods stdout is stopped at the limit instead of
// running until its deadline.
func TestRoundToolOutputLimitStopsChild(t *testing.T) {
	script := filepath.Join(t.TempDir(), "flood.sh")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nhead -c 2000000 /dev/zero\nexec sleep 30\n"), 0700))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	_, err := runRoundToolLimited(ctx, 1024*1024, script)
	require.ErrorIs(t, err, errRoundToolOutputLimit)
	require.Less(t, time.Since(start), 10*time.Second)
}

// F14: the accepted-result write is retried; a lost claim is not.
func TestFinishRoundLedgerWithRetry(t *testing.T) {
	saved := roundLedgerRetryDelay
	roundLedgerRetryDelay = time.Millisecond
	defer func() { roundLedgerRetryDelay = saved }()
	calls := 0
	err := finishRoundLedgerWithRetry(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("synthetic database is locked")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	calls = 0
	err = finishRoundLedgerWithRetry(context.Background(), func(context.Context) error {
		calls++
		return store.ErrRoundDispatchBusy
	})
	require.ErrorIs(t, err, store.ErrRoundDispatchBusy)
	require.Equal(t, 1, calls)
}

// F14: a round accepted by Telegram is reported as sent even if its ledger
// result cannot be stored.
func TestFinishRoundSendPersistenceFailureStillSent(t *testing.T) {
	saved := roundLedgerRetryDelay
	roundLedgerRetryDelay = time.Millisecond
	defer func() { roundLedgerRetryDelay = saved }()
	container, raw := integrationStore(t, filepath.Join(t.TempDir(), "ledger.db"))
	tc := &TelegramClient{main: &TelegramConnector{Store: container}}
	op := &store.RoundOperation{RoundOperationKey: store.RoundOperationKey{BridgeID: "b", LoginID: "l", PortalID: "p", Action: "round_send", RequestID: "$e"}, EventID: "$e", MediaIdentity: "m", PayloadHash: "h"}
	require.NoError(t, raw.Close()) // every write fails
	response := &bridgev2.MatrixMessageResponse{DB: &database.Message{ID: "7", MXID: id.EventID("$e"), Metadata: &MessageMetadata{IsRound: true}}, StreamOrder: 7}
	require.NoError(t, tc.FinishRoundSend(context.Background(), op, response, nil, true))
}
