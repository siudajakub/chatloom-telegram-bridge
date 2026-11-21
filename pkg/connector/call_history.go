// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"fmt"
	"time"

	"go.mau.fi/util/exfmt"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func callHistoryPart(body string, video bool, status, direction string, duration *int) *bridgev2.ConvertedMessagePart {
	callType := event.BeeperActionMessageCallTypeVoice
	if video {
		callType = event.BeeperActionMessageCallTypeVideo
	}
	fields := map[string]any{
		"call_type": callType,
		"status":    status,
		"direction": direction,
	}
	if duration != nil {
		fields["duration_seconds"] = *duration
	}
	return &bridgev2.ConvertedMessagePart{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{
			MsgType: event.MsgNotice,
			Body:    body,
			BeeperActionMessage: &event.BeeperActionMessage{
				Type:     event.BeeperActionMessageCall,
				CallType: callType,
			},
		},
		Extra: map[string]any{"com.chatloom.call": fields},
	}
}

func telegramPhoneCall(msg *tg.MessageService, action *tg.MessageActionPhoneCall) *bridgev2.ConvertedMessage {
	if ttl, _ := msg.GetTTLPeriod(); ttl > 0 {
		return nil
	}

	direction := "incoming"
	if msg.GetOut() {
		direction = "outgoing"
	}
	status := "unknown"
	outcome := ""
	switch action.Reason.(type) {
	case *tg.PhoneCallDiscardReasonMissed:
		status, outcome = "missed", " missed"
	case *tg.PhoneCallDiscardReasonDisconnect:
		status, outcome = "disconnected", " disconnected"
	case *tg.PhoneCallDiscardReasonHangup:
		status, outcome = "ended", " ended"
	case *tg.PhoneCallDiscardReasonBusy:
		status, outcome = "rejected", " rejected"
	}
	body := "Call"
	if action.Video {
		body = "Video call"
	}
	body += outcome
	var duration *int
	if seconds, ok := action.GetDuration(); ok && seconds >= 0 {
		duration = &seconds
		if seconds > 0 {
			body += fmt.Sprintf(" (%s)", exfmt.Duration(time.Duration(seconds)*time.Second))
		}
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{
			callHistoryPart(body, action.Video, status, direction, duration),
		},
	}
}
