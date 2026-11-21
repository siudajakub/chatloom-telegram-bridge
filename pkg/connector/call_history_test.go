// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"testing"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func TestTelegramPhoneCallHistory(t *testing.T) {
	action := &tg.MessageActionPhoneCall{Video: true, CallID: 123}
	action.SetReason(&tg.PhoneCallDiscardReasonHangup{})
	action.SetDuration(62)
	msg := &tg.MessageService{}
	msg.SetOut(true)
	part := telegramPhoneCall(msg, action).Parts[0]
	fields := part.Extra["com.chatloom.call"].(map[string]any)
	if fields["status"] != "ended" ||
		fields["duration_seconds"] != 62 || fields["direction"] != "outgoing" ||
		part.Content.BeeperActionMessage.CallType != event.BeeperActionMessageCallTypeVideo {
		t.Fatal("incorrect Telegram call result metadata")
	}
	msg.SetTTLPeriod(60)
	if telegramPhoneCall(msg, action) != nil {
		t.Fatal("expiring call entries should stay outside the call bridge")
	}
}

func TestTelegramPhoneCallMissingReasonAndDuration(t *testing.T) {
	action := &tg.MessageActionPhoneCall{}
	fields := telegramPhoneCall(&tg.MessageService{}, action).Parts[0].Extra["com.chatloom.call"].(map[string]any)
	if fields["status"] != "unknown" || fields["duration_seconds"] != nil {
		t.Fatal("absent outcome or duration must remain unknown")
	}
}
