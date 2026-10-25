// mautrix-telegram - A Matrix-Telegram puppeting bridge.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

// ChatLoom fork: contact presence for 1:1 portals.
//
// Telegram pushes updateUserStatus and carries User.status on every user
// entity, but upstream ignores both. Beeper's homeserver does not deliver
// ghost m.presence to clients, so the status is written as private room
// account data (com.chatloom.presence) through the user's double puppet:
// it never enters the timeline and the contact cannot see it.
//
// Writes are throttled per contact: only on a change (presence, precision or
// an exact last-seen time), at most once per presenceMinInterval (a change
// inside the window is written when it ends), plus an "online" refresh every
// presenceOnlineRefresh while Telegram keeps reporting the contact online,
// because ChatLoom treats an online value older than ~5 minutes as stale.
//
// Production guards: at most presenceMaxConcurrent writes run at once (the
// first chat sync after a start reports every contact), a failed write backs
// that contact off, and a homeserver that rate-limits or keeps refusing the
// account data, or a missing double puppet, pauses every write for a while.
// Each pause is logged once (warn, no identifiers), as is the first write the
// homeserver accepted, so a deploy can tell from the log whether it works.

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

const (
	chatloomPresenceType  = "com.chatloom.presence"
	presenceMinInterval   = time.Minute
	presenceOnlineRefresh = 3 * time.Minute
	presenceWriteTimeout  = 30 * time.Second

	// Concurrency: writes (portal lookup + HTTP) running at once, writes that
	// may wait for a slot (more are dropped as failed), and how long they wait.
	presenceMaxConcurrent = 2
	presenceMaxQueued     = 256
	presenceQueueWait     = 2 * time.Minute

	// A contact whose write the homeserver refused (4xx) is skipped this long;
	// this many refusals in a row (no success between) pause every contact.
	presenceRejectedBackoff = time.Hour
	presenceRejectionsPause = 3
	presenceRejectedPause   = time.Hour
	presenceRateLimitPause  = time.Minute
	presenceNoPuppetPause   = 6 * time.Hour

	// Contacts not observed for presenceEntryTTL are forgotten (bounded memory).
	presenceEntryTTL   = time.Hour
	presenceSweepEvery = 10 * time.Minute
)

// chatloomPresence is the room account data schema read by ChatLoom.
type chatloomPresence struct {
	Presence     string `json:"presence"`       // "online" or "offline"
	LastActiveTS int64  `json:"last_active_ts"` // ms; see mapTelegramStatus
	Precision    string `json:"precision"`      // exact, recently, week, month
	Source       string `json:"source"`
}

// changeKey identifies what the app would show; coarse buckets ignore the
// observation time.
func (p chatloomPresence) changeKey() chatloomPresence {
	if p.Presence == "online" || p.Precision != "exact" {
		p.LastActiveTS = 0
	}
	p.Source = ""
	return p
}

// mapTelegramStatus maps a Telegram user status to the ChatLoom schema.
// Online carries the observation time, exact offline the last-seen time and
// coarse buckets the observation time (never shown as a time). No status
// (hidden entirely or unknown) is no data: ok is false.
func mapTelegramStatus(status tg.UserStatusClass, now time.Time) (chatloomPresence, bool) {
	p := chatloomPresence{Presence: "offline", Precision: "exact", Source: "telegram", LastActiveTS: now.UnixMilli()}
	switch s := status.(type) {
	case *tg.UserStatusOnline:
		expires := time.Unix(int64(s.Expires), 0)
		if s.Expires > 0 && !expires.After(now) {
			// An online status that already expired reads as last seen then.
			p.LastActiveTS = expires.UnixMilli()
		} else {
			p.Presence = "online"
		}
	case *tg.UserStatusOffline:
		if s.WasOnline <= 0 {
			return chatloomPresence{}, false
		}
		was := time.Unix(int64(s.WasOnline), 0)
		if was.After(now) {
			was = now
		}
		p.LastActiveTS = was.UnixMilli()
	case *tg.UserStatusRecently:
		p.Precision = "recently"
	case *tg.UserStatusLastWeek:
		p.Precision = "week"
	case *tg.UserStatusLastMonth:
		p.Precision = "month"
	default:
		return chatloomPresence{}, false
	}
	return p, true
}

// presenceOutcome is the result of one write attempt.
type presenceOutcome int

const (
	presenceWritten     presenceOutcome = iota // homeserver accepted the account data
	presenceNoPortal                           // no bridged 1:1 portal for this contact: nothing to write
	presenceFailed                             // transient: network, timeout, 5xx, no free slot, paused
	presenceRejected                           // 4xx (not 408/429): refused for this room or type
	presenceRateLimited                        // 429
	presenceNoPuppet                           // double puppeting unavailable
)

// presenceNotice is a global state change the caller logs once.
type presenceNotice int

const (
	presenceNoticeNone presenceNotice = iota
	presenceNoticeAccepted
	presenceNoticeRejected
	presenceNoticeRateLimited
	presenceNoticeNoPuppet
)

type presenceEntry struct {
	written   chatloomPresence
	writtenAt time.Time
	retryAt   time.Time // backoff after a failed write
	lastSeen  time.Time
	pending   *chatloomPresence
	timer     *time.Timer
	inFlight  bool
}

// presenceThrottle decides per contact when a status is written. It is pure
// bookkeeping; the caller performs the write and reports the outcome. The
// zero value is ready to use.
type presenceThrottle struct {
	mu          sync.Mutex
	entries     map[int64]*presenceEntry
	pausedUntil time.Time
	rejections  int
	accepted    bool
	lastSweep   time.Time
	slots       chan struct{}
	queued      atomic.Int32
}

// decide returns whether p should be written now. When the change is
// throttled it is kept as pending and retryAfter > 0 says when to try again.
func (t *presenceThrottle) decide(userID int64, p chatloomPresence, now time.Time) (write bool, retryAfter time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.Before(t.pausedUntil) {
		return false, 0
	}
	if t.entries == nil {
		t.entries = make(map[int64]*presenceEntry)
	}
	if now.Sub(t.lastSweep) >= presenceSweepEvery {
		t.sweepLocked(now)
	}
	e := t.entries[userID]
	if e == nil {
		e = &presenceEntry{}
		t.entries[userID] = e
	}
	e.lastSeen = now
	if e.inFlight {
		e.pending = &p
		return false, presenceMinInterval
	}
	if now.Before(e.retryAt) {
		// Backing off after a failure: the next observation after it retries.
		e.pending = nil
		return false, 0
	}
	changed := e.writtenAt.IsZero() || e.written.changeKey() != p.changeKey()
	refresh := !changed && p.Presence == "online" && now.Sub(e.writtenAt) >= presenceOnlineRefresh
	if !changed && !refresh {
		e.pending = nil
		return false, 0
	}
	if wait := presenceMinInterval - now.Sub(e.writtenAt); !e.writtenAt.IsZero() && wait > 0 {
		e.pending = &p
		return false, wait
	}
	e.pending = nil
	e.inFlight = true
	return true, 0
}

// sweepLocked forgets idle contacts. Caller holds t.mu.
func (t *presenceThrottle) sweepLocked(now time.Time) {
	t.lastSweep = now
	for userID, e := range t.entries {
		if !e.inFlight && e.pending == nil && e.timer == nil && now.Sub(e.lastSeen) >= presenceEntryTTL {
			delete(t.entries, userID)
		}
	}
}

// paused reports whether every write is currently paused.
func (t *presenceThrottle) paused(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return now.Before(t.pausedUntil)
}

// finish records the outcome of a write decided by decide and returns a
// global state change for the caller to log (once per change).
func (t *presenceThrottle) finish(userID int64, p chatloomPresence, now time.Time, o presenceOutcome) presenceNotice {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e := t.entries[userID]; e != nil {
		e.inFlight = false
		switch o {
		case presenceWritten, presenceNoPortal:
			e.written, e.writtenAt, e.retryAt = p, now, time.Time{}
		case presenceRejected:
			e.retryAt = now.Add(presenceRejectedBackoff)
		default:
			e.retryAt = now.Add(presenceMinInterval)
		}
	}
	pause := func(d time.Duration) bool {
		if now.Before(t.pausedUntil) {
			return false
		}
		t.pausedUntil = now.Add(d)
		return true
	}
	switch o {
	case presenceWritten:
		t.rejections = 0
		if !t.accepted {
			t.accepted = true
			return presenceNoticeAccepted
		}
	case presenceRejected:
		t.rejections++
		if t.rejections >= presenceRejectionsPause && pause(presenceRejectedPause) {
			t.rejections = 0
			return presenceNoticeRejected
		}
	case presenceRateLimited:
		if pause(presenceRateLimitPause) {
			return presenceNoticeRateLimited
		}
	case presenceNoPuppet:
		if pause(presenceNoPuppetPause) {
			return presenceNoticeNoPuppet
		}
	}
	return presenceNoticeNone
}

// armRetry arms at most one retry timer per contact.
func (t *presenceThrottle) armRetry(userID int64, after time.Duration, fire func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[userID]
	if e == nil || e.timer != nil {
		return
	}
	e.timer = time.AfterFunc(after, func() {
		t.mu.Lock()
		e.timer = nil
		t.mu.Unlock()
		fire()
	})
}

// takePending returns the latest throttled status, if any.
func (t *presenceThrottle) takePending(userID int64) (chatloomPresence, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[userID]
	if e == nil || e.pending == nil {
		return chatloomPresence{}, false
	}
	p := *e.pending
	e.pending = nil
	return p, true
}

// enqueue reserves a place for one write goroutine; false means too many wait.
func (t *presenceThrottle) enqueue() bool {
	if t.queued.Add(1) > presenceMaxQueued {
		t.queued.Add(-1)
		return false
	}
	return true
}

func (t *presenceThrottle) dequeue() { t.queued.Add(-1) }

func (t *presenceThrottle) slotChan() chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.slots == nil {
		t.slots = make(chan struct{}, presenceMaxConcurrent)
	}
	return t.slots
}

func (t *presenceThrottle) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, e := range t.entries {
		if e.timer != nil {
			e.timer.Stop()
			e.timer = nil
		}
	}
}

// classifyPresenceError maps a write error to an outcome, with the HTTP
// status and Matrix errcode when the homeserver answered.
func classifyPresenceError(err error) (o presenceOutcome, status int, errcode string) {
	if err == nil {
		return presenceWritten, 0, ""
	}
	var skipped presenceSkipped
	if errors.As(err, &skipped) {
		return presenceNoPuppet, 0, ""
	}
	var httpErr mautrix.HTTPError
	if errors.As(err, &httpErr) && httpErr.Response != nil {
		status = httpErr.Response.StatusCode
		if httpErr.RespError != nil {
			errcode = httpErr.RespError.ErrCode
		}
		switch {
		case status == http.StatusTooManyRequests:
			return presenceRateLimited, status, errcode
		case status >= 400 && status < 500 && status != http.StatusRequestTimeout:
			return presenceRejected, status, errcode
		}
		return presenceFailed, status, errcode
	}
	return presenceFailed, 0, ""
}

// onUserStatus handles updateUserStatus and User.status seen on entities.
func (tc *TelegramClient) onUserStatus(ctx context.Context, userID int64, status tg.UserStatusClass) {
	if !tc.main.Config.ChatloomPresence || userID == tc.telegramUserID || status == nil {
		return
	}
	p, ok := mapTelegramStatus(status, time.Now())
	if !ok {
		return
	}
	tc.offerPresence(ctx, userID, p)
}

func (tc *TelegramClient) offerPresence(ctx context.Context, userID int64, p chatloomPresence) {
	// The update handler's context may be cancelled before a retry or the
	// write runs; keep only its values (the logger).
	ctx = context.WithoutCancel(ctx)
	write, retry := tc.presence.decide(userID, p, time.Now())
	if retry > 0 {
		tc.presence.armRetry(userID, retry, func() {
			if pending, ok := tc.presence.takePending(userID); ok {
				tc.offerPresence(ctx, userID, pending)
			}
		})
	}
	if !write {
		return
	}
	if !tc.presence.enqueue() {
		tc.presence.finish(userID, p, time.Now(), presenceFailed)
		return
	}
	go func() {
		defer tc.presence.dequeue()
		slots := tc.presence.slotChan()
		select {
		case slots <- struct{}{}:
		case <-time.After(presenceQueueWait):
			tc.presence.finish(userID, p, time.Now(), presenceFailed)
			return
		}
		defer func() { <-slots }()
		if tc.presence.paused(time.Now()) {
			// Paused while this write waited for a slot: do not send it.
			tc.presence.finish(userID, p, time.Now(), presenceFailed)
			return
		}
		wctx, cancel := context.WithTimeout(ctx, presenceWriteTimeout)
		defer cancel()
		wrote, err := tc.writePresence(wctx, userID, p)
		o, status, errcode := classifyPresenceError(err)
		if err == nil && !wrote {
			o = presenceNoPortal
		}
		notice := tc.presence.finish(userID, p, time.Now(), o)
		log := zerolog.Ctx(ctx)
		switch notice {
		case presenceNoticeAccepted:
			log.Info().Msg("ChatLoom presence: first contact status written as room account data")
		case presenceNoticeRejected:
			log.Warn().Int("status_code", status).Str("errcode", errcode).
				Dur("pause", presenceRejectedPause).
				Msg("ChatLoom presence: homeserver refused the room account data; writes paused")
		case presenceNoticeRateLimited:
			log.Warn().Dur("pause", presenceRateLimitPause).
				Msg("ChatLoom presence: homeserver rate limit; writes paused")
		case presenceNoticeNoPuppet:
			log.Warn().Err(err).Dur("pause", presenceNoPuppetPause).
				Msg("ChatLoom presence: double puppeting unavailable; writes paused")
		}
		if err != nil {
			log.Debug().Err(err).Msg("Failed to write contact presence")
		}
	}()
}

type presenceSkipped string

func (e presenceSkipped) Error() string { return string(e) }

// writePresence puts the status into the 1:1 portal's room account data as
// the user (double puppet). Contacts without a bridged DM are skipped
// (wrote is false, no error).
func (tc *TelegramClient) writePresence(ctx context.Context, userID int64, p chatloomPresence) (wrote bool, err error) {
	portal, err := tc.main.Bridge.GetExistingPortalByKey(ctx, tc.makePortalKeyFromID(ids.PeerTypeUser, userID, 0))
	if err != nil {
		return false, err
	}
	if portal == nil || portal.MXID == "" {
		return false, nil
	}
	return true, setRoomAccountDataAsUser(ctx, tc.userLogin.User, portal.MXID, chatloomPresenceType, &p)
}

func setRoomAccountDataAsUser(ctx context.Context, user *bridgev2.User, roomID id.RoomID, eventType string, content any) error {
	dp := user.DoublePuppet(ctx)
	if dp == nil {
		return presenceSkipped("double puppeting is not available")
	}
	intent, ok := dp.(*matrix.ASIntent)
	if !ok || !intent.Matrix.IsCustomPuppet {
		return presenceSkipped("double puppet does not support room account data")
	}
	return intent.Matrix.SetRoomAccountData(ctx, roomID, eventType, content)
}
