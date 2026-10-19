package connector

import (
	"context"
	"time"

	"maunium.net/go/mautrix/bridgev2"
)

// queueRoundCapabilityRefresh prevents capability generation from blocking
// gotd's initialization/update callbacks. It refreshes once in the portal queue.
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
	value, loaded := tc.roundCapabilityRefresh.LoadOrStore(portal.PortalKey, now.Add(5*time.Minute))
	if loaded {
		if now.Before(value.(time.Time)) {
			return
		}
		if !tc.roundCapabilityRefresh.CompareAndSwap(portal.PortalKey, value, now.Add(5*time.Minute)) {
			return
		}
	}
	result := tc.userLogin.QueueRemoteEvent(&queuedMediaControl{Key: portal.PortalKey, Login: tc.userLogin, Run: func(ctx context.Context, p *bridgev2.Portal) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, _ = tc.roundUploadLimit(ctx)
		_, _ = tc.checkRoundPermission(ctx, p)
		p.UpdateCapabilities(ctx, tc.userLogin, false)
		tc.publishMediaCapabilities(ctx, p)
	}})
	if result.Error != nil {
		tc.roundCapabilityRefresh.Delete(portal.PortalKey)
	}
}
