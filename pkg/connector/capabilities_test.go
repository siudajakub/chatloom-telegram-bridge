// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/util/exsync"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
)

type capsBot struct {
	bridgev2.MatrixAPI
	sends atomic.Int64
}

func (b *capsBot) SendState(context.Context, id.RoomID, event.Type, string, *event.Content, time.Time) (*mautrix.RespSendEvent, error) {
	b.sends.Add(1)
	return &mautrix.RespSendEvent{EventID: "$synthetic-state"}, nil
}

func capabilityFixture(t *testing.T) (*TelegramClient, *bridgev2.Portal, *capsBot) {
	t.Helper()
	bot := &capsBot{}
	bridge := &bridgev2.Bridge{ID: "synthetic-bridge", Bot: bot, Config: &bridgeconfig.BridgeConfig{}}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "456", UserMXID: "@synthetic:example.invalid"}, Bridge: bridge}
	portal := intakePortal(ids.PeerTypeChat, ptr.Ptr(false))
	portal.Bridge = bridge
	portal.BridgeID = bridge.ID
	portal.Metadata.(*PortalMetadata).AllowedReactions = []string{}
	initialized, done := exsync.NewEvent(), exsync.NewEvent()
	initialized.Set()
	rpc := &protectionRPC{}
	tc := &TelegramClient{
		main:   &TelegramConnector{Bridge: bridge, maxFileSize: roundMaxFileBytes},
		client: rpc.client(), clientInitialized: initialized, clientDone: done,
		userLogin: login, loginID: login.ID, telegramUserID: 456,
		metadata:  &UserLoginMetadata{Session: UserLoginSession{AuthKey: bytes.Repeat([]byte{0x42}, 256)}},
		appConfig: map[string]any{"upload_max_fileparts_default": float64(4000)},
	}
	login.Client = tc
	return tc, portal, bot
}

func TestGetCapabilitiesHasNoSideEffects(t *testing.T) {
	tc, portal, bot := capabilityFixture(t)
	var refreshes atomic.Int64
	tc.capabilityRefreshFunc = func(*bridgev2.Portal) { refreshes.Add(1) }
	for range 5 {
		caps := tc.GetCapabilities(context.Background(), portal)
		require.NotNil(t, caps)
	}
	require.Zero(t, bot.sends.Load(), "GetCapabilities must not send state")
	require.Eventually(t, func() bool { return refreshes.Load() == 1 }, time.Second, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.EqualValues(t, 1, refreshes.Load(), "the refresh is throttled per portal")
}

func TestCapabilityRefreshNeverQueuesSynchronously(t *testing.T) {
	tc, portal, _ := capabilityFixture(t)
	release := make(chan struct{})
	defer close(release)
	tc.capabilityRefreshFunc = func(*bridgev2.Portal) { <-release }
	start := time.Now()
	tc.GetCapabilities(context.Background(), portal)
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestMediaCapabilitiesPublishedOnlyOnChange(t *testing.T) {
	if !roundToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not installed: round availability is always denied")
	}
	tc, portal, bot := capabilityFixture(t)
	allowed := &RoundPermission{Allowed: ptr.Ptr(true), Reason: "allowed", CheckedAtMS: time.Now().UnixMilli()}
	setRoundPermission(portal, "456", allowed)
	tc.publishMediaCapabilitiesIfChanged(context.Background(), portal)
	tc.publishMediaCapabilitiesIfChanged(context.Background(), portal)
	require.EqualValues(t, 1, bot.sends.Load())
	setRoundPermission(portal, "456", &RoundPermission{Allowed: ptr.Ptr(false), Reason: "round_video_forbidden", CheckedAtMS: time.Now().UnixMilli()})
	tc.publishMediaCapabilitiesIfChanged(context.Background(), portal)
	tc.publishMediaCapabilitiesIfChanged(context.Background(), portal)
	require.EqualValues(t, 2, bot.sends.Load())
}

func TestRoundAvailabilityStableAfterFiveMinutes(t *testing.T) {
	if !roundToolsAvailable() {
		t.Skip("ffmpeg/ffprobe not installed")
	}
	tc, portal, _ := capabilityFixture(t)
	tc.appConfigFetched = time.Now().Add(-6 * time.Minute)
	setRoundPermission(portal, "456", &RoundPermission{Allowed: ptr.Ptr(true), Reason: "allowed", CheckedAtMS: time.Now().UnixMilli()})
	allowed, permission, limit := tc.roundAvailability(context.Background(), portal)
	require.NotNil(t, allowed)
	require.True(t, *allowed)
	require.Equal(t, "allowed", permission)
	require.NotNil(t, limit)
}

func TestStickerCapsAcceptBridgedFormats(t *testing.T) {
	sticker := fileCaps[event.CapMsgSticker]
	require.Equal(t, event.CapLevelFullySupported, sticker.GetMimeSupport("application/x-tgsticker"))
	require.Equal(t, event.CapLevelPartialSupport, sticker.GetMimeSupport("image/gif"))
	require.Equal(t, event.CapLevelFullySupported, premiumFileCaps[event.CapMsgSticker].GetMimeSupport("application/x-tgsticker"))
	// GIF messages are unchanged.
	require.Equal(t, event.CapLevelPartialSupport, fileCaps[event.CapMsgGIF].GetMimeSupport("image/gif"))
	_, capsVersion := (&TelegramConnector{}).GetBridgeInfoVersion()
	require.Equal(t, 16, capsVersion)
	tc, portal, _ := capabilityFixture(t)
	tc.capabilityRefreshFunc = func(*bridgev2.Portal) {}
	caps := tc.GetCapabilities(context.Background(), portal)
	require.True(t, strings.HasPrefix(caps.ID, "fi.mau.telegram.capabilities.2026_10_22.round.v2"), "a new static ID makes bridgev2 resend room features")
}
