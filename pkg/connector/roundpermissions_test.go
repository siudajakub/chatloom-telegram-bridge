package connector

import (
	"testing"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func TestRoundPermissionChangesAndDistinctBans(t *testing.T) {
	now := time.Unix(100, 0)
	cases := []struct {
		name string
		raw  tg.ChatClass
		want *bool
	}{
		{"round ban", &tg.Channel{Megagroup: true, DefaultBannedRights: tg.ChatBannedRights{SendRoundvideos: true}}, boolPtr(false)},
		{"ordinary videos ban permits round", &tg.Channel{Megagroup: true, DefaultBannedRights: tg.ChatBannedRights{SendVideos: true}}, boolPtr(true)},
		{"self ban", &tg.Channel{Megagroup: true, BannedRights: tg.ChatBannedRights{SendMessages: true}}, boolPtr(false)},
		{"expired self ban", &tg.Channel{Megagroup: true, BannedRights: tg.ChatBannedRights{SendRoundvideos: true, UntilDate: 99}}, boolPtr(true)},
		{"broadcast no publishing", &tg.Channel{Broadcast: true}, boolPtr(false)},
		{"broadcast publishing", &tg.Channel{Broadcast: true, AdminRights: tg.ChatAdminRights{PostMessages: true}}, boolPtr(true)},
		{"left creator", &tg.Channel{Creator: true, Left: true}, boolPtr(false)},
		{"min unknown", &tg.Channel{Min: true}, nil},
		{"forbidden unknown", &tg.ChannelForbidden{}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := roundPermissionFromChat(c.raw, now)
			if (p.Allowed == nil) != (c.want == nil) || (p.Allowed != nil && *p.Allowed != *c.want) {
				t.Fatalf("permission %v, expected %v", p, c.want)
			}
		})
	}
}

func boolPtr(v bool) *bool { return &v }
