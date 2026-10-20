// SPDX-License-Identifier: AGPL-3.0-or-later
package connector

import (
	"encoding/json"
	"maunium.net/go/mautrix/event"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
)

func TestRoundCapabilityFrozenV1(t *testing.T) {
	allowed := true
	limit := int64(16777216)
	capability := roundFileCapability(&allowed, &limit)
	if capability.MimeTypes["video/mp4"] != event.CapLevelFullySupported || capability.Caption != event.CapLevelRejected || capability.MaxWidth != 640 || capability.MaxHeight != 640 || capability.MaxDuration.Get() != 60*time.Second || capability.MaxSize != limit || len(capability.ViewLimitedTypes) != 0 {
		t.Fatal("file cap mismatch")
	}
	caps := makeMediaCapabilities("synthetic-login", "!synthetic:example.invalid", "synthetic-bridge", &allowed, "allowed", &limit)
	caps.MatrixUserID = "@synthetic-owner:example.invalid"
	data, err := json.Marshal(caps)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err = json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["version"] != float64(1) || raw["round_video"] != true || raw["round_video_permission"] != "allowed" || raw["account_id"] != "synthetic-login" || raw["matrix_user_id"] != "@synthetic-owner:example.invalid" || raw["bridge_state_key"] != "synthetic-bridge" {
		t.Fatal("identity contract mismatch")
	}
	for _, feature := range []string{"playback_receipts", "native_forward", "round_recording_action", "lazy_ttl_media", "protected_content", "silent_send", "scheduled_send"} {
		if value, ok := raw[feature]; !ok || value != false {
			t.Fatalf("unsupported feature %s not explicit false", feature)
		}
	}
	profile := raw["profile"].(map[string]any)
	if profile["max_duration_ms"] != float64(60000) || profile["max_dimension_px"] != float64(640) || profile["max_size_bytes"] != float64(limit) || profile["sanitized_corners"] != true {
		t.Fatal("profile units mismatch")
	}
	if raw["schedule_mode"] != "unsupported" {
		t.Fatal("schedule advertised")
	}
}

type roundIdentityMatrix struct{ bridgev2.MatrixConnector }

func (roundIdentityMatrix) GetUniqueBridgeID() string { return "synthetic-provider" }
func TestRoundCapabilityStateKeyMatchesSDK(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{BridgeID: "synthetic-fallback"}, Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{}}}
	if key := telegramMediaStateKey(portal); key != "synthetic-fallback" {
		t.Fatalf("fallback key %s", key)
	}
	portal.Bridge.Matrix = roundIdentityMatrix{}
	if key := telegramMediaStateKey(portal); key != "synthetic-provider" {
		t.Fatalf("provider key %s", key)
	}
	portal.Bridge.Config.NoBridgeInfoStateKey = true
	if key := telegramMediaStateKey(portal); key != "" {
		t.Fatalf("configured empty key %s", key)
	}
}

func TestRoundUnknownCapabilitiesFailClosed(t *testing.T) {
	for _, allowed := range []*bool{nil, new(bool)} {
		capability := roundFileCapability(allowed, nil)
		if capability.MimeTypes["video/mp4"] != event.CapLevelRejected {
			t.Fatal("unknown capability allows fallback")
		}
	}
	caps := makeMediaCapabilities("account", "room", "bridge", nil, "unknown", nil)
	data, _ := json.Marshal(caps)
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	if value, ok := raw["round_video"]; !ok || value != nil {
		t.Fatal("unknown round capability not explicit null")
	}
	if raw["profile"].(map[string]any)["max_size_bytes"] != nil {
		t.Fatal("unknown size not null")
	}
}
