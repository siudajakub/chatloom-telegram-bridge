package connector

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	up "go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix"

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

var testNow = time.Unix(1_761_400_000, 0)

func testOnline(at time.Time) chatloomPresence {
	return chatloomPresence{Presence: "online", LastActiveTS: at.UnixMilli(), Precision: "exact", Source: "telegram"}
}

func testOffline(at time.Time) chatloomPresence {
	return chatloomPresence{Presence: "offline", LastActiveTS: at.UnixMilli(), Precision: "exact", Source: "telegram"}
}

func TestPresenceThrottle(t *testing.T) {
	var th presenceThrottle
	now := testNow
	online := testOnline(now)
	write := func(p chatloomPresence, at time.Time) (bool, time.Duration) {
		ok, retry := th.decide(42, p, at)
		if ok {
			th.finish(42, p, at, presenceWritten)
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
	ok, retry = write(testOffline(now.Add(40*time.Second)), now.Add(40*time.Second))
	require.False(t, ok)
	require.Equal(t, 20*time.Second, retry)
	pending, has := th.takePending(42)
	require.True(t, has)
	require.Equal(t, "offline", pending.Presence)
	ok, _ = write(pending, now.Add(time.Minute))
	require.True(t, ok)
	// A new exact last-seen time is a change; coarse observation times are not.
	ok, _ = write(testOffline(now.Add(5*time.Minute)), now.Add(6*time.Minute))
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
}

func TestPresenceThrottleInFlight(t *testing.T) {
	var th presenceThrottle
	ok, _ := th.decide(1, testOnline(testNow), testNow)
	require.True(t, ok)
	// A change while the write runs is kept and retried after the window.
	ok, retry := th.decide(1, testOffline(testNow), testNow.Add(time.Second))
	require.False(t, ok)
	require.Equal(t, presenceMinInterval, retry)
	th.finish(1, testOnline(testNow), testNow.Add(2*time.Second), presenceWritten)
	pending, has := th.takePending(1)
	require.True(t, has)
	require.Equal(t, "offline", pending.Presence)
	ok, _ = th.decide(1, pending, testNow.Add(time.Minute+2*time.Second))
	require.True(t, ok)
}

func TestPresenceThrottleFailureBackoff(t *testing.T) {
	var th presenceThrottle
	// A transient failure backs this contact off for a minute instead of
	// retrying on every update.
	ok, _ := th.decide(7, testOnline(testNow), testNow)
	require.True(t, ok)
	require.Equal(t, presenceNoticeNone, th.finish(7, testOnline(testNow), testNow, presenceFailed))
	for _, d := range []time.Duration{time.Second, 30 * time.Second, 59 * time.Second} {
		ok, retry := th.decide(7, testOnline(testNow), testNow.Add(d))
		require.False(t, ok, d)
		require.Zero(t, retry, d)
	}
	ok, _ = th.decide(7, testOnline(testNow), testNow.Add(time.Minute))
	require.True(t, ok)
	th.finish(7, testOnline(testNow), testNow.Add(time.Minute), presenceWritten)

	// A refusal (4xx) backs only this contact off for an hour.
	ok, _ = th.decide(8, testOnline(testNow), testNow)
	require.True(t, ok)
	require.Equal(t, presenceNoticeNone, th.finish(8, testOnline(testNow), testNow, presenceRejected))
	ok, _ = th.decide(8, testOffline(testNow), testNow.Add(30*time.Minute))
	require.False(t, ok)
	ok, _ = th.decide(9, testOnline(testNow), testNow.Add(time.Second))
	require.True(t, ok, "other contacts are not paused by one refusal")
	th.finish(9, testOnline(testNow), testNow.Add(time.Second), presenceWritten)
	ok, _ = th.decide(8, testOffline(testNow), testNow.Add(time.Hour))
	require.True(t, ok)
}

func TestPresenceThrottleGlobalPause(t *testing.T) {
	var th presenceThrottle
	// Repeated refusals with no success between pause every contact once.
	notices := []presenceNotice{}
	for uid := int64(1); uid <= presenceRejectionsPause; uid++ {
		ok, _ := th.decide(uid, testOnline(testNow), testNow)
		require.True(t, ok)
		notices = append(notices, th.finish(uid, testOnline(testNow), testNow, presenceRejected))
	}
	require.Equal(t, []presenceNotice{presenceNoticeNone, presenceNoticeNone, presenceNoticeRejected}, notices)
	require.True(t, th.paused(testNow.Add(59*time.Minute)))
	ok, retry := th.decide(100, testOnline(testNow), testNow.Add(time.Minute))
	require.False(t, ok)
	require.Zero(t, retry)
	require.False(t, th.paused(testNow.Add(presenceRejectedPause)))
	ok, _ = th.decide(100, testOnline(testNow), testNow.Add(presenceRejectedPause))
	require.True(t, ok)
	// The first accepted write is reported once, and resets the refusal count.
	require.Equal(t, presenceNoticeAccepted, th.finish(100, testOnline(testNow), testNow.Add(presenceRejectedPause), presenceWritten))
	ok, _ = th.decide(101, testOnline(testNow), testNow.Add(presenceRejectedPause))
	require.True(t, ok)
	require.Equal(t, presenceNoticeNone, th.finish(101, testOnline(testNow), testNow.Add(presenceRejectedPause), presenceWritten))

	// A success between refusals keeps the writes going.
	var th2 presenceThrottle
	for uid := int64(1); uid <= 2*presenceRejectionsPause; uid++ {
		ok, _ := th2.decide(uid, testOnline(testNow), testNow)
		require.True(t, ok)
		o := presenceRejected
		if uid%presenceRejectionsPause == 0 {
			o = presenceWritten
		}
		require.NotEqual(t, presenceNoticeRejected, th2.finish(uid, testOnline(testNow), testNow, o))
	}
	require.False(t, th2.paused(testNow))

	// Rate limit and a missing double puppet pause too, reported once each.
	var th3 presenceThrottle
	ok, _ = th3.decide(1, testOnline(testNow), testNow)
	require.True(t, ok)
	ok, _ = th3.decide(2, testOnline(testNow), testNow)
	require.True(t, ok)
	require.Equal(t, presenceNoticeRateLimited, th3.finish(1, testOnline(testNow), testNow, presenceRateLimited))
	require.Equal(t, presenceNoticeNone, th3.finish(2, testOnline(testNow), testNow, presenceRateLimited))
	require.True(t, th3.paused(testNow.Add(59*time.Second)))
	require.False(t, th3.paused(testNow.Add(presenceRateLimitPause)))
	at := testNow.Add(2 * time.Minute)
	ok, _ = th3.decide(3, testOnline(at), at)
	require.True(t, ok)
	require.Equal(t, presenceNoticeNoPuppet, th3.finish(3, testOnline(at), at, presenceNoPuppet))
	require.True(t, th3.paused(at.Add(presenceNoPuppetPause-time.Second)))
}

func TestPresenceThrottleNoPortalAndSweep(t *testing.T) {
	var th presenceThrottle
	// A contact without a DM is remembered like a write (no lookup per update)
	// but is not reported as an accepted write.
	ok, _ := th.decide(5, testOnline(testNow), testNow)
	require.True(t, ok)
	require.Equal(t, presenceNoticeNone, th.finish(5, testOnline(testNow), testNow, presenceNoPortal))
	ok, _ = th.decide(5, testOnline(testNow), testNow.Add(time.Minute))
	require.False(t, ok)

	// Idle contacts are forgotten; busy ones are kept.
	for uid := int64(10); uid < 20; uid++ {
		ok, _ = th.decide(uid, testOnline(testNow), testNow)
		require.True(t, ok)
		th.finish(uid, testOnline(testNow), testNow, presenceNoPortal)
	}
	ok, _ = th.decide(30, testOnline(testNow), testNow)
	require.True(t, ok) // stays in flight
	require.Len(t, th.entries, 12)
	ok, _ = th.decide(10, testOffline(testNow), testNow.Add(presenceEntryTTL+time.Minute))
	require.True(t, ok)
	require.Len(t, th.entries, 2, "entry 30 is in flight, entry 10 was just observed")
}

func TestPresenceQueueLimit(t *testing.T) {
	var th presenceThrottle
	for i := 0; i < presenceMaxQueued; i++ {
		require.True(t, th.enqueue())
	}
	require.False(t, th.enqueue())
	th.dequeue()
	require.True(t, th.enqueue())
	require.Equal(t, presenceMaxConcurrent, cap(th.slotChan()))
}

func TestClassifyPresenceError(t *testing.T) {
	httpErr := func(status int, errcode string) error {
		e := mautrix.HTTPError{Response: &http.Response{StatusCode: status}}
		if errcode != "" {
			e.RespError = &mautrix.RespError{ErrCode: errcode, StatusCode: status}
		}
		return fmt.Errorf("wrapped: %w", e)
	}
	for _, c := range []struct {
		err     error
		want    presenceOutcome
		status  int
		errcode string
	}{
		{nil, presenceWritten, 0, ""},
		{presenceSkipped("x"), presenceNoPuppet, 0, ""},
		{fmt.Errorf("w: %w", presenceSkipped("x")), presenceNoPuppet, 0, ""},
		{httpErr(429, "M_LIMIT_EXCEEDED"), presenceRateLimited, 429, "M_LIMIT_EXCEEDED"},
		{httpErr(403, "M_FORBIDDEN"), presenceRejected, 403, "M_FORBIDDEN"},
		{httpErr(404, "M_UNRECOGNIZED"), presenceRejected, 404, "M_UNRECOGNIZED"},
		{httpErr(400, ""), presenceRejected, 400, ""},
		{httpErr(408, ""), presenceFailed, 408, ""},
		{httpErr(502, ""), presenceFailed, 502, ""},
		{errors.New("dial tcp: connection refused"), presenceFailed, 0, ""},
		{mautrix.HTTPError{WrappedError: errors.New("timeout")}, presenceFailed, 0, ""},
	} {
		o, status, errcode := classifyPresenceError(c.err)
		require.Equal(t, c.want, o, "%v", c.err)
		require.Equal(t, c.status, status, "%v", c.err)
		require.Equal(t, c.errcode, errcode, "%v", c.err)
	}
}

// bbctl writes config.yaml from its own template on every start, without
// chatloom_presence; the bridge's config upgrade must turn it on by default
// (and write it back), while an explicit false still wins.
func TestChatloomPresenceConfigDefault(t *testing.T) {
	_, _, upgrader := (&TelegramConnector{}).GetConfig()
	base, ok := upgrader.(up.BaseUpgrader)
	require.True(t, ok)

	out, err := up.DoBytes([]byte("bridge_communities: false\nanimated_sticker:\n    target: webp\n    convert_from_webm: true\n"), base)
	require.NoError(t, err)
	require.Contains(t, string(out), "\nchatloom_presence: true\n")
	var cfg TelegramConfig
	require.NoError(t, yaml.Unmarshal(out, &cfg))
	require.True(t, cfg.ChatloomPresence)
	require.False(t, cfg.BridgeCommunities)

	out, err = up.DoBytes([]byte("chatloom_presence: false\n"), base)
	require.NoError(t, err)
	cfg = TelegramConfig{}
	require.NoError(t, yaml.Unmarshal(out, &cfg))
	require.False(t, cfg.ChatloomPresence)
}
