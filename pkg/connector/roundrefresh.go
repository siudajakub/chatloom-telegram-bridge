package connector

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// roundRefreshSlots bounds concurrent capability refresh RPCs. After a caps
// version change bridgev2 recomputes capabilities for every portal at once.
var roundRefreshSlots = make(chan struct{}, 2)

const roundCapabilityRefreshInterval = 5 * time.Minute

// queueRoundCapabilityRefresh is called from GetCapabilities, which can run
// inside the portal's own event loop. It never queues into that loop and
// never blocks: the throttled refresh runs in its own goroutine (wave 5 F3;
// re-queueing from inside the loop could deadlock or stall the portal).
func (tc *TelegramClient) queueRoundCapabilityRefresh(portal *bridgev2.Portal) {
	if portal == nil || portal.MXID == "" || !tc.IsLoggedIn() {
		return
	}
	if portal.Receiver != "" && portal.Receiver != tc.loginID {
		return
	}
	if portal.CapState.Source != "" && portal.CapState.Source != tc.loginID {
		return
	}
	now := time.Now()
	if blocked := tc.roundRefreshBlockedAt.Load(); blocked > 0 && now.UnixNano() < blocked {
		// Telegram asked to wait (FLOOD_WAIT): no refresh until it passes.
		return
	}
	value, loaded := tc.roundCapabilityRefresh.LoadOrStore(portal.PortalKey, now.Add(roundCapabilityRefreshInterval))
	if loaded {
		if now.Before(value.(time.Time)) {
			return
		}
		if !tc.roundCapabilityRefresh.CompareAndSwap(portal.PortalKey, value, now.Add(roundCapabilityRefreshInterval)) {
			return
		}
	}
	run := tc.capabilityRefreshFunc
	if run == nil {
		run = tc.refreshRoundCapabilities
	}
	go run(portal)
}

// refreshRoundCapabilities re-reads the upload limit and the round permission,
// then updates room features and the media capability state when they
// changed. It runs outside the portal loop with its own timeout, like
// bridgev2's own UpdateCapabilities calls from ResendBridgeInfo.
func (tc *TelegramClient) refreshRoundCapabilities(portal *bridgev2.Portal) {
	log := zerolog.Nop()
	if tc.userLogin != nil {
		log = tc.userLogin.Log.With().Str("action", "round_capability_refresh").Logger()
	}
	ctx := log.WithContext(context.Background())
	roundRefreshSlots <- struct{}{}
	defer func() { <-roundRefreshSlots }()
	if !tc.IsLoggedIn() {
		tc.roundCapabilityRefresh.Delete(portal.PortalKey)
		return
	}
	if blocked := tc.roundRefreshBlockedAt.Load(); blocked > 0 && time.Now().UnixNano() < blocked {
		tc.roundCapabilityRefresh.Delete(portal.PortalKey)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, _ = tc.roundUploadLimit(ctx)
	if _, err := tc.checkRoundPermission(ctx, portal); err != nil {
		if wait, ok := tgerr.AsFloodWait(err); ok {
			tc.roundRefreshBlockedAt.Store(time.Now().Add(wait).UnixNano())
			// Retry this portal after the wait instead of in 5 minutes.
			tc.roundCapabilityRefresh.Delete(portal.PortalKey)
			log.Warn().Int64("wait_seconds", int64(wait/time.Second)).Msg("Round capability refresh rate limited by Telegram")
		}
	}
	portal.UpdateCapabilities(ctx, tc.userLogin, false)
	tc.publishMediaCapabilitiesIfChanged(ctx, portal)
}
