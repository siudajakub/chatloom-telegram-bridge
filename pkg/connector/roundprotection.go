package connector

import (
	"context"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
)

// refreshRoundSourceProtection intentionally does not use a cached false to
// authorize exporting media. Policy can change while a room is idle.
func (tc *TelegramClient) refreshRoundSourceProtection(ctx context.Context, portal *bridgev2.Portal) error {
	meta := portal.Metadata.(*PortalMetadata)
	meta.ProtectedContent = nil
	peerType, peerID, _, err := ids.ParsePortalID(portal.ID)
	if err != nil {
		return err
	}
	if peerType == ids.PeerTypeUser {
		meta.ProtectedContent = ptr.Ptr(false)
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if tc.client == nil {
		return nil
	}
	var response tg.MessagesChatsClass
	switch peerType {
	case ids.PeerTypeChat:
		response, err = tc.client.API().MessagesGetChats(ctx, []int64{peerID})
	case ids.PeerTypeChannel:
		var accessHash int64
		accessHash, err = tc.ScopedStore.GetAccessHash(ctx, ids.PeerTypeChannel, peerID)
		if err == nil {
			response, err = tc.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: peerID, AccessHash: accessHash}})
		}
	}
	if err != nil {
		return err
	}
	if response != nil {
		meta.ProtectedContent = mediaProtectionFromChats(peerType, peerID, response.GetChats())
	}
	return nil
}
