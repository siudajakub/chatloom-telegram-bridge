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

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
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

type presenceEntry struct {
	written   chatloomPresence
	writtenAt time.Time
	pending   *chatloomPresence
	timer     *time.Timer
	inFlight  bool
}

// presenceThrottle decides per contact when a status is written. It is pure
// bookkeeping; the caller performs the write and reports the outcome.
type presenceThrottle struct {
	mu      sync.Mutex
	entries map[int64]*presenceEntry
}

// decide returns whether p should be written now. When the change is
// throttled it is kept as pending and retryAfter > 0 says when to try again.
func (t *presenceThrottle) decide(userID int64, p chatloomPresence, now time.Time) (write bool, retryAfter time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries == nil {
		t.entries = make(map[int64]*presenceEntry)
	}
	e := t.entries[userID]
	if e == nil {
		e = &presenceEntry{}
		t.entries[userID] = e
	}
	if e.inFlight {
		e.pending = &p
		return false, presenceMinInterval
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

// done records the outcome of a write decided by decide.
func (t *presenceThrottle) done(userID int64, p chatloomPresence, now time.Time, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[userID]
	if e == nil {
		return
	}
	e.inFlight = false
	if ok {
		e.written = p
		e.writtenAt = now
	}
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
	write, retry := tc.presence.decide(userID, p, time.Now())
	if retry > 0 {
		log := zerolog.Ctx(ctx).With().Logger()
		tc.presence.armRetry(userID, retry, func() {
			if pending, ok := tc.presence.takePending(userID); ok {
				tc.offerPresence(log.WithContext(tc.clientCtx), userID, pending)
			}
		})
	}
	if !write {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), presenceWriteTimeout)
		defer cancel()
		err := tc.writePresence(ctx, userID, p)
		var skipped presenceSkipped
		// A missing double puppet will not fix itself per update: record it
		// like a write so it is retried on the next change only.
		tc.presence.done(userID, p, time.Now(), err == nil || errors.As(err, &skipped))
		if err != nil {
			zerolog.Ctx(ctx).Debug().Err(err).Msg("Failed to write contact presence")
		}
	}()
}

type presenceSkipped string

func (e presenceSkipped) Error() string { return string(e) }

// writePresence puts the status into the 1:1 portal's room account data as
// the user (double puppet). Contacts without a bridged DM are skipped.
func (tc *TelegramClient) writePresence(ctx context.Context, userID int64, p chatloomPresence) error {
	portal, err := tc.main.Bridge.GetExistingPortalByKey(ctx, tc.makePortalKeyFromID(ids.PeerTypeUser, userID, 0))
	if err != nil {
		return err
	}
	if portal == nil || portal.MXID == "" {
		// Not an error: remember the value so group members do not cost a
		// lookup on every update.
		return nil
	}
	return setRoomAccountDataAsUser(ctx, tc.userLogin.User, portal.MXID, chatloomPresenceType, &p)
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
