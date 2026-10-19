// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"encoding/json"
	"math"
	"strconv"
)

const roundSendOptionsKey = "pl.personal.chat.telegram.send_options"

// RoundSendOptions is included in the durable send identity before dispatch.
// V1 supports silent delivery. A nonzero schedule is explicitly unsupported;
// it must never silently become an immediate send.
type RoundSendOptions struct {
	Version      int   `json:"version"`
	Silent       bool  `json:"silent"`
	ScheduleDate int64 `json:"schedule_date"`
}

func parseRoundSendOptions(raw map[string]any) (RoundSendOptions, error) {
	options := RoundSendOptions{Version: 1}
	value, present := raw[roundSendOptionsKey]
	if !present {
		return options, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return options, mediaControlRejected("invalid_send_options")
	}
	for field := range fields {
		switch field {
		case "version", "silent", "schedule_date":
		default:
			return options, mediaControlRejected("unsupported_send_option")
		}
	}
	version, ok := roundOptionInteger(fields["version"])
	if !ok || version != 1 {
		return options, mediaControlRejected("unsupported_send_options_version")
	}
	if silent, present := fields["silent"]; present {
		options.Silent, ok = silent.(bool)
		if !ok {
			return options, mediaControlRejected("invalid_silent_option")
		}
	}
	if schedule, present := fields["schedule_date"]; present {
		options.ScheduleDate, ok = roundOptionInteger(schedule)
		if !ok {
			return options, mediaControlRejected("invalid_schedule_date")
		}
		if options.ScheduleDate != 0 {
			return options, mediaControlRejected("scheduled_send_not_supported")
		}
	}
	return options, nil
}

// JSON decoding commonly stores numbers as float64; accepting only exact
// finite integers keeps the wire strict while supporting native test/builders.
func roundOptionInteger(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	case json.Number:
		parsed, err := strconv.ParseInt(string(number), 10, 64)
		return parsed, err == nil
	case float64:
		if math.IsNaN(number) || math.IsInf(number, 0) || number != math.Trunc(number) || number < math.MinInt64 || number >= math.MaxInt64 {
			return 0, false
		}
		return int64(number), true
	default:
		return 0, false
	}
}
