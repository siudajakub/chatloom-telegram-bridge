// SPDX-License-Identifier: AGPL-3.0-or-later
package connector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func TestRoundSendOptionsDefaultsAndStrictTypes(t *testing.T) {
	for _, raw := range []map[string]any{nil, {}, {"unrelated_matrix_field": true}} {
		options, err := parseRoundSendOptions(raw)
		if err != nil || options.Version != 1 || options.Silent || options.ScheduleDate != 0 {
			t.Fatalf("wrong default: %+v %v", options, err)
		}
	}
	for _, version := range []any{1, int64(1), float64(1), json.Number("1")} {
		options, err := parseRoundSendOptions(map[string]any{roundSendOptionsKey: map[string]any{"version": version, "silent": true, "schedule_date": float64(0)}})
		if err != nil || !options.Silent || options.Version != 1 || options.ScheduleDate != 0 {
			t.Fatalf("valid JSON types rejected: %+v %v", options, err)
		}
	}
	for _, test := range []struct {
		name  string
		value any
		code  string
	}{
		{"null", nil, "invalid_send_options"},
		{"boolean", true, "invalid_send_options"},
		{"string", "silent", "invalid_send_options"},
		{"missing version", map[string]any{"silent": true}, "unsupported_send_options_version"},
		{"wrong version", map[string]any{"version": float64(2)}, "unsupported_send_options_version"},
		{"version string", map[string]any{"version": "1"}, "unsupported_send_options_version"},
		{"version boolean", map[string]any{"version": true}, "unsupported_send_options_version"},
		{"silent string", map[string]any{"version": 1, "silent": "true"}, "invalid_silent_option"},
		{"silent integer", map[string]any{"version": 1, "silent": float64(1)}, "invalid_silent_option"},
		{"silent null", map[string]any{"version": 1, "silent": nil}, "invalid_silent_option"},
		{"schedule string", map[string]any{"version": 1, "schedule_date": "0"}, "invalid_schedule_date"},
		{"schedule fractional", map[string]any{"version": 1, "schedule_date": float64(0.5)}, "invalid_schedule_date"},
		{"schedule boolean", map[string]any{"version": 1, "schedule_date": false}, "invalid_schedule_date"},
		{"schedule nan", map[string]any{"version": 1, "schedule_date": math.NaN()}, "invalid_schedule_date"},
		{"schedule infinity", map[string]any{"version": 1, "schedule_date": math.Inf(1)}, "invalid_schedule_date"},
		{"schedule overflow", map[string]any{"version": 1, "schedule_date": float64(math.MaxInt64)}, "invalid_schedule_date"},
		{"schedule positive", map[string]any{"version": 1, "schedule_date": int64(1791792000)}, "scheduled_send_not_supported"},
		{"schedule negative", map[string]any{"version": 1, "schedule_date": int64(-1)}, "scheduled_send_not_supported"},
		{"paid stars", map[string]any{"version": 1, "allow_paid_stars": 0}, "unsupported_send_option"},
		{"paid floodskip", map[string]any{"version": 1, "allow_paid_floodskip": false}, "unsupported_send_option"},
		{"unknown", map[string]any{"version": 1, "extra": nil}, "unsupported_send_option"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseRoundSendOptions(map[string]any{roundSendOptionsKey: test.value})
			var rejection *RoundOperationError
			if !errors.As(err, &rejection) || rejection.Code != test.code || rejection.Certainty != "certain" || rejection.Retriable {
				t.Fatalf("invalid options lost typed certainty: %v", err)
			}
		})
	}
}

func TestRoundSilentOptionRealTLRequest(t *testing.T) {
	for _, silent := range []bool{false, true} {
		options, err := parseRoundSendOptions(map[string]any{roundSendOptionsKey: map[string]any{"version": float64(1), "silent": silent, "schedule_date": float64(0)}})
		if err != nil {
			t.Fatal(err)
		}
		stub := &roundRPCStub{testing: t}
		_, err = tg.NewClient(stub).MessagesSendMedia(context.Background(), &tg.MessagesSendMediaRequest{
			Peer: &tg.InputPeerSelf{}, RandomID: 12345, Silent: options.Silent, ScheduleDate: int(options.ScheduleDate),
			Media: makeRoundUploadedDocument(&tg.InputFile{ID: 1, Parts: 1, Name: "round.mp4", MD5Checksum: "synthetic"}, &tg.InputFile{ID: 2, Parts: 1, Name: "thumbnail.jpg", MD5Checksum: "synthetic"}, roundMediaProfile{DurationMS: 1000, Width: 320, Height: 320}),
		})
		if err != nil {
			t.Fatal(err)
		}
		if stub.calls != 1 || stub.request.Silent != silent || stub.request.ScheduleDate != 0 || stub.request.AllowPaidStars != 0 || stub.request.AllowPaidFloodskip {
			t.Fatal("real TL changed silent/schedule/paid policy")
		}
	}
}
