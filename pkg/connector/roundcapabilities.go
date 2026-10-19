// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/json"
	"os/exec"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
)

var telegramMediaCapabilitiesEvent = event.Type{Type: "pl.personal.chat.telegram.media_capabilities", Class: event.StateEventType}

type RoundEncodingProfile struct {
	ID               string `json:"id"`
	Container        string `json:"container"`
	VideoCodec       string `json:"video_codec"`
	AudioCodec       string `json:"audio_codec"`
	PixelFormat      string `json:"pixel_format"`
	Square           bool   `json:"square"`
	SanitizedCorners bool   `json:"sanitized_corners"`
	MaxDurationMS    int    `json:"max_duration_ms"`
	MaxDimensionPX   int    `json:"max_dimension_px"`
	MaxSizeBytes     *int64 `json:"max_size_bytes"`
}

type TelegramMediaCapabilities struct {
	Version              int                  `json:"version"`
	AccountID            string               `json:"account_id"`
	MatrixUserID         string               `json:"matrix_user_id"`
	RoomID               string               `json:"room_id"`
	BridgeStateKey       string               `json:"bridge_state_key"`
	RoundVideo           *bool                `json:"round_video"`
	RoundVideoPermission string               `json:"round_video_permission"`
	Profile              RoundEncodingProfile `json:"profile"`
	PlaybackReceipts     bool                 `json:"playback_receipts"`
	NativeForward        bool                 `json:"native_forward"`
	RoundRecordingAction bool                 `json:"round_recording_action"`
	LazyTTLMedia         bool                 `json:"lazy_ttl_media"`
	ProtectedContent     bool                 `json:"protected_content"`
	SilentSend           bool                 `json:"silent_send"`
	ScheduledSend        bool                 `json:"scheduled_send"`
	ScheduleMode         string               `json:"schedule_mode"`
	Controls             struct {
		PlayedEventType  string `json:"played_event_type"`
		ForwardEventType string `json:"forward_event_type"`
		StateEventType   string `json:"state_event_type"`
		MaxRequestAgeMS  int    `json:"max_request_age_ms"`
	} `json:"controls"`
}

func telegramMediaStateKey(portal *bridgev2.Portal) string {
	if portal.Bridge.Config.NoBridgeInfoStateKey {
		return ""
	}
	if provider, ok := portal.Bridge.Matrix.(bridgev2.MatrixConnectorWithBridgeIdentifier); ok {
		return provider.GetUniqueBridgeID()
	}
	return string(portal.BridgeID)
}

func roundToolsAvailable() bool {
	_, first := exec.LookPath("ffmpeg")
	_, second := exec.LookPath("ffprobe")
	return first == nil && second == nil
}

func roundFileCapability(allowed *bool, limit *int64) *event.FileFeatures {
	level := event.CapLevelRejected
	if allowed != nil && *allowed && limit != nil && *limit > 0 {
		level = event.CapLevelFullySupported
	}
	capability := &event.FileFeatures{
		MimeTypes: map[string]event.CapabilitySupportLevel{"video/mp4": level},
		Caption:   event.CapLevelRejected,
		MaxWidth:  roundMaxDimension, MaxHeight: roundMaxDimension,
		MaxDuration: ptr.Ptr(jsontime.S(60 * time.Second)), ViewOnce: false,
	}
	if limit != nil {
		capability.MaxSize = *limit
	}
	return capability
}

func (tc *TelegramClient) roundAvailability(ctx context.Context, portal *bridgev2.Portal) (allowed *bool, permission string, limit *int64) {
	permission = "unknown"
	if portal == nil || tc.userLogin == nil || !tc.IsLoggedIn() {
		return
	}
	if !roundToolsAvailable() {
		return ptr.Ptr(false), "denied", nil
	}
	// Capability publication is allowed during room creation. It must never
	// start an RPC while initial client setup is holding connection locks.
	tc.appConfigLock.Lock()
	value, err := roundUploadLimitFromConfig(tc.appConfig, tc.isPremiumCache.Load())
	if !tc.appConfigFetched.IsZero() && time.Since(tc.appConfigFetched) > 5*time.Minute {
		err = roundError("upload_limit_unknown", nil)
	}
	tc.appConfigLock.Unlock()
	if err == nil && tc.main.maxFileSize > 0 {
		value = min(value, tc.main.maxFileSize)
		limit = &value
	}
	if state := getRoundPermission(portal, string(tc.userLogin.ID)); state != nil && state.Allowed != nil && state.CheckedAtMS > 0 && time.Since(time.UnixMilli(state.CheckedAtMS)) >= 0 && time.Since(time.UnixMilli(state.CheckedAtMS)) <= 5*time.Minute {
		if !*state.Allowed {
			return ptr.Ptr(false), "denied", limit
		}
		if limit != nil {
			return ptr.Ptr(true), "allowed", limit
		}
	}
	return nil, permission, limit
}

func makeMediaCapabilities(accountID, roomID, stateKey string, allowed *bool, permission string, limit *int64) *TelegramMediaCapabilities {
	caps := &TelegramMediaCapabilities{Version: 1, AccountID: accountID, RoomID: roomID, BridgeStateKey: stateKey, RoundVideo: allowed, RoundVideoPermission: permission, ScheduleMode: "unsupported"}
	caps.Profile = RoundEncodingProfile{ID: "pl.personal.chat.telegram.round_video.v1", Container: "mp4", VideoCodec: "h264", AudioCodec: "aac", PixelFormat: "yuv420p", Square: true, SanitizedCorners: true, MaxDurationMS: roundMaxDurationMS, MaxDimensionPX: roundMaxDimension, MaxSizeBytes: limit}
	caps.Controls.PlayedEventType = "pl.personal.chat.telegram.media_played"
	caps.Controls.ForwardEventType = "pl.personal.chat.telegram.media_forward"
	caps.Controls.StateEventType = "pl.personal.chat.telegram.media_state"
	caps.Controls.MaxRequestAgeMS = 300000
	return caps
}

func (tc *TelegramClient) publishMediaCapabilities(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" || tc.userLogin == nil || !tc.IsLoggedIn() {
		return
	}
	if portal.Receiver != "" && portal.Receiver != tc.userLogin.ID {
		return
	}
	if portal.CapState.Source != "" && portal.CapState.Source != tc.userLogin.ID {
		return
	}
	allowed, permission, limit := tc.roundAvailability(ctx, portal)
	stateKey := telegramMediaStateKey(portal)
	content := makeMediaCapabilities(string(tc.userLogin.ID), string(portal.MXID), stateKey, allowed, permission, limit)
	content.MatrixUserID = string(tc.userLogin.UserMXID)
	content.SilentSend = true
	if tc.main.mediaControlsEnabled {
		if mx, ok := tc.main.Bridge.Matrix.(*matrix.Connector); ok && mx.Crypto != nil && mx.AS != nil && mx.AS.StateStore != nil {
			encrypted, stateErr := mx.AS.StateStore.IsEncrypted(ctx, portal.MXID)
			content.PlaybackReceipts = stateErr == nil && encrypted
			content.NativeForward = content.PlaybackReceipts
		}
	}
	// Use JSON raw content so explicit false/null remain visible to clients.
	data, err := json.Marshal(content)
	if err != nil {
		return
	}
	var raw map[string]any
	if json.Unmarshal(data, &raw) != nil {
		return
	}
	_, err = tc.main.Bridge.Bot.SendState(ctx, portal.MXID, telegramMediaCapabilitiesEvent, stateKey, &event.Content{Raw: raw}, time.Time{})
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Failed to publish Telegram media capability state")
	}
}
