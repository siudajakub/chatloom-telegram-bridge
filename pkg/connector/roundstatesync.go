package connector

import (
	"context"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/connector/store"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

func roundStateEligible(msg *database.Message) bool {
	meta, ok := msg.Metadata.(*MessageMetadata)
	return ok && meta.IsRound && meta.ProtectionKnown && !meta.NoForwards && meta.TTLSeconds == 0 && !meta.MediaExpired && (meta.MediaPolicy == nil || meta.MediaPolicy.TransferAllowed)
}

func (tc *TelegramClient) initializeRoundMediaState(ctx context.Context, portal *bridgev2.Portal, remote *tg.Message) {
	if remote == nil || !isRoundMedia(remote.Media) {
		return
	}
	parts, err := tc.main.Bridge.DB.Message.GetAllPartsByID(ctx, tc.loginID, ids.GetMessageIDFromMessage(remote))
	if err != nil {
		return
	}
	ledger := store.NewRoundLedger(tc.main.Store.Database)
	wasRead, err := ledger.WasContentsRead(ctx, string(tc.main.Bridge.ID), string(tc.loginID), string(ids.GetMessageIDFromMessage(remote)))
	if err != nil {
		return
	}
	for _, part := range parts {
		if part.Room != portal.PortalKey || !roundStateEligible(part) {
			continue
		}
		state, err := ledger.GetMediaState(ctx, string(tc.main.Bridge.ID), string(tc.loginID), string(portal.ID), string(portal.Receiver), string(part.MXID))
		if err != nil {
			continue
		}
		unread := remote.MediaUnread && !wasRead
		if state != nil {
			// Never resurrect played from stale history snapshots.
			if state.MediaUnread != nil && !*state.MediaUnread {
				unread = false
			}
			if state.MediaUnread != nil && *state.MediaUnread == unread {
				_ = tc.sendRoundMediaState(ctx, portal, state, "")
				continue
			}
		}
		_ = tc.PublishRoundMediaState(ctx, portal, part, &unread, false, "")
	}
}

// onRoundContentsRead handles contents-read only. History/chat reads do not
// call it and no read RPC is generated here.
func (tc *TelegramClient) onRoundContentsRead(ctx context.Context, channelID int64, remoteIDs []int) error {
	ledger := store.NewRoundLedger(tc.main.Store.Database)
	for _, remoteID := range remoteIDs {
		if remoteID <= 0 {
			continue
		}
		messageID := ids.MakeMessageID(channelID, remoteID)
		if err := ledger.RememberContentsRead(ctx, string(tc.main.Bridge.ID), string(tc.loginID), string(messageID)); err != nil {
			return err
		}
		parts, err := tc.main.Bridge.DB.Message.GetAllPartsByID(ctx, tc.loginID, messageID)
		if err != nil {
			return err
		}
		for _, part := range parts {
			if !roundStateEligible(part) {
				continue
			}
			mapped := part
			res := tc.userLogin.QueueRemoteEvent(&queuedMediaControl{Key: part.Room, Login: tc.userLogin, Run: func(qctx context.Context, p *bridgev2.Portal) {
				fresh, err := tc.main.Bridge.DB.Message.GetPartByMXID(qctx, mapped.MXID)
				if err != nil || fresh == nil || fresh.Room != p.PortalKey || !roundStateEligible(fresh) {
					return
				}
				unread := false
				meta := fresh.Metadata.(*MessageMetadata)
				meta.MediaUnread = &unread
				if tc.main.Bridge.DB.Message.Update(qctx, fresh) != nil {
					return
				}
				_ = tc.PublishRoundMediaState(qctx, p, fresh, &unread, false, "")
			}})
			if res.Error != nil {
				return res.Error
			}
		}
	}
	return nil
}
