// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/util/ptr"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// protectionFreshFor is how long a known chat/channel noforwards value may
// authorize a media transfer without another RPC. Telegram pushes chat and
// channel entities with updates, so most values are refreshed for free.
const protectionFreshFor = 60 * time.Second

// protectionStaleMaxAge bounds the last known value used when Telegram cannot
// be asked (RPC error, FLOOD_WAIT back-off, chat missing from the response).
// J approved this fallback on 2026-10-22 (wave 5 F1 ASK part). Older or
// timestamp-less values are unknown, which blocks media but keeps text.
const protectionStaleMaxAge = 24 * time.Hour

// protectionStaleOnErrorEnv set to false (0/false) turns that fallback off,
// restoring "unknown on every lookup failure". It is on by default.
const protectionStaleOnErrorEnv = "TELEGRAM_PROTECTION_STALE_ON_ERROR"

// envFlag reads a boolean environment variable with a default.
func envFlag(name string, fallback bool) bool {
	value, err := strconv.ParseBool(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}

type protectionKey struct {
	peerType ids.PeerType
	peerID   int64
}

type protectionEntry struct {
	value bool
	at    time.Time
}

// protectionCache is per login. Its size is bounded by the number of chats
// and channels the account sees, like the portal table.
type protectionCache struct {
	entries sync.Map // protectionKey -> protectionEntry
	flight  singleflight.Group
	// blockedUntil is a unix-nano deadline after a FLOOD_WAIT on the
	// protection RPC. No further RPC is issued before it passes.
	blockedUntil atomic.Int64
}

func (c *protectionCache) remember(key protectionKey, value bool, at time.Time) {
	c.entries.Store(key, protectionEntry{value: value, at: at})
}

func (c *protectionCache) lookup(key protectionKey) (protectionEntry, bool) {
	raw, ok := c.entries.Load(key)
	if !ok {
		return protectionEntry{}, false
	}
	return raw.(protectionEntry), true
}

func (c *protectionCache) fresh(key protectionKey, now time.Time) (*bool, bool) {
	entry, ok := c.lookup(key)
	if !ok || now.Sub(entry.at) > protectionFreshFor || now.Before(entry.at.Add(-time.Minute)) {
		return nil, false
	}
	return ptr.Ptr(entry.value), true
}

// getPortalProtection reads the last known chat protection of a portal under
// the metadata lock (wave 5 F6).
func getPortalProtection(portal *bridgev2.Portal) *bool {
	value, _ := getPortalProtectionAt(portal)
	return value
}

// getPortalProtectionAt also returns when the value was last confirmed (zero
// for values stored before wave 5).
func getPortalProtectionAt(portal *bridgev2.Portal) (*bool, time.Time) {
	if portal == nil {
		return nil, time.Time{}
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil {
		return nil, time.Time{}
	}
	meta.roundPermissionLock.RLock()
	defer meta.roundPermissionLock.RUnlock()
	if meta.ProtectedContent == nil {
		return nil, time.Time{}
	}
	var at time.Time
	if meta.ProtectionCheckedAtMS > 0 {
		at = time.UnixMilli(meta.ProtectionCheckedAtMS)
	}
	return ptr.Ptr(*meta.ProtectedContent), at
}

// setPortalProtection stores a known value. Unknown never overwrites a known
// value: a failed or incomplete lookup is not evidence of a policy change.
func setPortalProtection(portal *bridgev2.Portal, value *bool) {
	if portal == nil || value == nil {
		return
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil {
		return
	}
	meta.roundPermissionLock.Lock()
	defer meta.roundPermissionLock.Unlock()
	meta.ProtectedContent = ptr.Ptr(*value)
	meta.ProtectionCheckedAtMS = time.Now().UnixMilli()
}

func protectionFromChatEntity(raw tg.ChatClass) (protectionKey, *bool) {
	switch chat := raw.(type) {
	case *tg.Chat:
		return protectionKey{ids.PeerTypeChat, chat.ID}, ptr.Ptr(chat.Noforwards)
	case *tg.Channel:
		if chat.Min {
			// Min constructors do not carry a reliable noforwards flag.
			return protectionKey{ids.PeerTypeChannel, chat.ID}, nil
		}
		return protectionKey{ids.PeerTypeChannel, chat.ID}, ptr.Ptr(chat.Noforwards)
	}
	return protectionKey{}, nil
}

// rememberProtectionFromChats feeds every full chat/channel constructor of an
// RPC response or update into the cache.
func (tc *TelegramClient) rememberProtectionFromChats(chats []tg.ChatClass) {
	now := time.Now()
	for _, raw := range chats {
		if key, value := protectionFromChatEntity(raw); value != nil {
			tc.protection.remember(key, *value, now)
		}
	}
}

// rememberProtectionFromEntities uses the entities that arrive with an update.
// They are as fresh as an RPC for the message carried by that update.
func (tc *TelegramClient) rememberProtectionFromEntities(e tg.Entities) {
	now := time.Now()
	for _, chat := range e.Chats {
		if chat != nil {
			tc.protection.remember(protectionKey{ids.PeerTypeChat, chat.ID}, chat.Noforwards, now)
		}
	}
	for _, channel := range e.Channels {
		if channel != nil && !channel.Min {
			tc.protection.remember(protectionKey{ids.PeerTypeChannel, channel.ID}, channel.Noforwards, now)
		}
	}
}

// rememberPortalProtection records a known value for both the portal metadata
// and the per-login cache. nil is ignored.
func (tc *TelegramClient) rememberPortalProtection(portal *bridgev2.Portal, value *bool) {
	if portal == nil || value == nil {
		return
	}
	setPortalProtection(portal, value)
	if peerType, peerID, _, err := ids.ParsePortalID(portal.ID); err == nil && peerType != ids.PeerTypeUser {
		tc.protection.remember(protectionKey{peerType, peerID}, *value, time.Now())
	}
}

func (tc *TelegramClient) protectionStaleOnError() bool {
	return tc.main != nil && tc.main.protectionStaleOnError
}

// protectionFor returns the chat protection to apply to one conversion:
// a fresh known value, the result of at most one in-flight RPC per peer, or
// nil (unknown). The returned error is only for logging.
func (tc *TelegramClient) protectionFor(ctx context.Context, portal *bridgev2.Portal) (*bool, error) {
	peerType, peerID, _, err := ids.ParsePortalID(portal.ID)
	if err != nil {
		return nil, err
	}
	if peerType == ids.PeerTypeUser {
		// A private chat has no chat-level noforwards flag.
		setPortalProtection(portal, ptr.Ptr(false))
		return ptr.Ptr(false), nil
	}
	key := protectionKey{peerType, peerID}
	if value, ok := tc.protection.fresh(key, time.Now()); ok {
		setPortalProtection(portal, value)
		return value, nil
	}
	if until := tc.protection.blockedUntil.Load(); until > 0 && time.Now().UnixNano() < until {
		return tc.protectionFallback(key, portal), errProtectionFloodWait
	}
	result, err, _ := tc.protection.flight.Do(string(peerType)+":"+strconv.FormatInt(peerID, 10), func() (any, error) {
		// A flight that started after another one stored a value reuses it.
		if value, ok := tc.protection.fresh(key, time.Now()); ok {
			return value, nil
		}
		// The flight is shared, so one caller's cancellation must not fail
		// the others; the RPC has its own bound.
		rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		value, err := tc.fetchSourceProtection(rpcCtx, peerType, peerID)
		if err != nil {
			if wait, ok := tgerr.AsFloodWait(err); ok {
				tc.protection.blockedUntil.Store(time.Now().Add(wait).UnixNano())
			}
			return nil, err
		}
		if value == nil {
			return nil, errProtectionMissing
		}
		tc.protection.remember(key, *value, time.Now())
		return value, nil
	})
	if err != nil {
		return tc.protectionFallback(key, portal), err
	}
	value := result.(*bool)
	setPortalProtection(portal, value)
	return ptr.Ptr(*value), nil
}

// protectionFallback is used when no fresh value is available: the last known
// value no older than protectionStaleMaxAge (cache first, then the persisted
// portal value), otherwise unknown. With TELEGRAM_PROTECTION_STALE_ON_ERROR=0
// it is always unknown.
func (tc *TelegramClient) protectionFallback(key protectionKey, portal *bridgev2.Portal) *bool {
	if !tc.protectionStaleOnError() {
		return nil
	}
	now := time.Now()
	if entry, ok := tc.protection.lookup(key); ok && now.Sub(entry.at) <= protectionStaleMaxAge && !now.Before(entry.at.Add(-time.Minute)) {
		return ptr.Ptr(entry.value)
	}
	if value, at := getPortalProtectionAt(portal); value != nil && !at.IsZero() && now.Sub(at) <= protectionStaleMaxAge && !now.Before(at.Add(-time.Minute)) {
		return value
	}
	return nil
}

type protectionError string

func (e protectionError) Error() string { return string(e) }

const (
	errProtectionFloodWait protectionError = "content protection lookup is rate limited"
	errProtectionMissing   protectionError = "content protection is missing from the Telegram response"
)

type protectionDecisionKey struct{}

// withProtectionDecision carries the protection value chosen for one
// conversion to every nested media guard, so they all agree with it and none
// of them reads a value that a concurrent refresh is changing.
func withProtectionDecision(ctx context.Context, value *bool) context.Context {
	if value != nil {
		value = ptr.Ptr(*value)
	}
	return context.WithValue(ctx, protectionDecisionKey{}, protectionDecisionValue{value: value})
}

type protectionDecisionValue struct{ value *bool }

func protectionDecisionFrom(ctx context.Context) (*bool, bool) {
	if ctx == nil {
		return nil, false
	}
	decision, ok := ctx.Value(protectionDecisionKey{}).(protectionDecisionValue)
	return decision.value, ok
}

// stripCustomEmojiEntities keeps the fallback emoji text but drops custom
// emoji documents, which are media that must not be exported while protection
// is unknown.
func stripCustomEmojiEntities(entities []tg.MessageEntityClass) []tg.MessageEntityClass {
	var out []tg.MessageEntityClass
	for _, entity := range entities {
		if _, ok := entity.(*tg.MessageEntityCustomEmoji); ok {
			continue
		}
		out = append(out, entity)
	}
	return out
}

// protectionLookupReason is a log code without identifiers or content.
func protectionLookupReason(err error) string {
	switch {
	case err == nil:
		return ""
	case err == errProtectionFloodWait:
		return "rate_limited"
	case err == errProtectionMissing:
		return "missing_from_response"
	case tgerr.Is(err, tgerr.FloodWaitErrors...):
		return "flood_wait"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	}
	if _, ok := tgerr.As(err); ok {
		return "rpc_error"
	}
	return "unavailable"
}
