package connector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func TestMapTelegramStatus(t *testing.T) {
	now := time.Unix(1_761_400_000, 0)
	ms := now.UnixMilli()

	p, ok := mapTelegramStatus(&tg.UserStatusOnline{Expires: int(now.Unix()) + 300}, now)
	require.True(t, ok)
	require.Equal(t, chatloomPresence{Presence: "online", LastActiveTS: ms, Precision: "exact", Source: "telegram"}, p)

	p, ok = mapTelegramStatus(&tg.UserStatusOnline{Expires: int(now.Unix()) - 60}, now)
	require.True(t, ok)
	require.Equal(t, "offline", p.Presence)
	require.Equal(t, ms-60_000, p.LastActiveTS)

	p, ok = mapTelegramStatus(&tg.UserStatusOffline{WasOnline: int(now.Unix()) - 600}, now)
	require.True(t, ok)
	require.Equal(t, chatloomPresence{Presence: "offline", LastActiveTS: ms - 600_000, Precision: "exact", Source: "telegram"}, p)

	for status, precision := range map[tg.UserStatusClass]string{
		&tg.UserStatusRecently{}:  "recently",
		&tg.UserStatusLastWeek{}:  "week",
		&tg.UserStatusLastMonth{}: "month",
	} {
		p, ok = mapTelegramStatus(status, now)
		require.True(t, ok)
		require.Equal(t, "offline", p.Presence)
		require.Equal(t, precision, p.Precision)
	}

	_, ok = mapTelegramStatus(&tg.UserStatusEmpty{}, now)
	require.False(t, ok)
	_, ok = mapTelegramStatus(nil, now)
	require.False(t, ok)
	_, ok = mapTelegramStatus(&tg.UserStatusOffline{}, now)
	require.False(t, ok)
}

func TestPresenceThrottle(t *testing.T) {
	var th presenceThrottle
	now := time.Unix(1_761_400_000, 0)
	online := chatloomPresence{Presence: "online", LastActiveTS: now.UnixMilli(), Precision: "exact", Source: "telegram"}
	offline := func(at time.Time) chatloomPresence {
		return chatloomPresence{Presence: "offline", LastActiveTS: at.UnixMilli(), Precision: "exact", Source: "telegram"}
	}
	write := func(p chatloomPresence, at time.Time) (bool, time.Duration) {
		ok, retry := th.decide(42, p, at)
		if ok {
			th.done(42, p, at, true)
		}
		return ok, retry
	}

	// First observation is written.
	ok, _ := write(online, now)
	require.True(t, ok)
	// The same online status inside the refresh window is not.
	later := online
	later.LastActiveTS += 30_000
	ok, retry := write(later, now.Add(30*time.Second))
	require.False(t, ok)
	require.Zero(t, retry)
	// A transition inside the minute is deferred, not dropped.
	ok, retry = write(offline(now.Add(40*time.Second)), now.Add(40*time.Second))
	require.False(t, ok)
	require.Equal(t, 20*time.Second, retry)
	pending, has := th.takePending(42)
	require.True(t, has)
	require.Equal(t, "offline", pending.Presence)
	ok, _ = write(pending, now.Add(time.Minute))
	require.True(t, ok)
	// A new exact last-seen time is a change; coarse observation times are not.
	ok, _ = write(offline(now.Add(5*time.Minute)), now.Add(6*time.Minute))
	require.True(t, ok)
	recently := chatloomPresence{Presence: "offline", LastActiveTS: 1, Precision: "recently", Source: "telegram"}
	ok, _ = write(recently, now.Add(8*time.Minute))
	require.True(t, ok)
	recently.LastActiveTS = 2
	ok, _ = write(recently, now.Add(10*time.Minute))
	require.False(t, ok)
	// Online is refreshed while Telegram keeps reporting it.
	ok, _ = write(online, now.Add(12*time.Minute))
	require.True(t, ok)
	ok, _ = write(online, now.Add(14*time.Minute))
	require.False(t, ok)
	ok, _ = write(online, now.Add(15*time.Minute))
	require.True(t, ok)

	// A failed write is retried on the next observation.
	ok, _ = th.decide(7, online, now)
	require.True(t, ok)
	th.done(7, online, now, false)
	ok, _ = th.decide(7, online, now.Add(time.Second))
	require.True(t, ok)
}
