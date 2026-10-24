package connector

import (
	"context"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/util/ptr"
)

// fetchSourceProtection asks Telegram for the current chat/channel
// noforwards flag. Callers go through protectionFor, which bounds it to one
// in-flight RPC per peer, reuses values younger than protectionFreshFor and
// backs off after FLOOD_WAIT. It never waits for a flood wait itself.
func (tc *TelegramClient) fetchSourceProtection(ctx context.Context, peerType ids.PeerType, peerID int64) (*bool, error) {
	if peerType == ids.PeerTypeUser {
		return ptr.Ptr(false), nil
	}
	if tc.client == nil {
		return nil, errProtectionMissing
	}
	var response tg.MessagesChatsClass
	var err error
	switch peerType {
	case ids.PeerTypeChat:
		response, err = tc.client.API().MessagesGetChats(ctx, []int64{peerID})
	case ids.PeerTypeChannel:
		if tc.ScopedStore == nil {
			return nil, errProtectionMissing
		}
		var accessHash int64
		accessHash, err = tc.ScopedStore.GetAccessHash(ctx, ids.PeerTypeChannel, peerID)
		if err == nil {
			response, err = tc.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: peerID, AccessHash: accessHash}})
		}
	default:
		return nil, errProtectionMissing
	}
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, nil
	}
	tc.rememberProtectionFromChats(response.GetChats())
	return mediaProtectionFromChats(peerType, peerID, response.GetChats()), nil
}
