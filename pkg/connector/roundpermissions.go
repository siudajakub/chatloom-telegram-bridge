package connector

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/mautrix-telegram/pkg/connector/ids"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
)

// RoundPermission is account specific. Missing or stale values are unknown.
type RoundPermission struct {
	Allowed     *bool  `json:"allowed"`
	Reason      string `json:"reason"`
	CheckedAtMS int64  `json:"checked_at_ms"`
}

func roundBanActive(rights tg.ChatBannedRights, now time.Time) bool {
	return (rights.UntilDate == 0 || int64(rights.UntilDate) > now.Unix()) &&
		(rights.ViewMessages || rights.SendMessages || rights.SendMedia || rights.SendRoundvideos)
}

func roundPermissionFromChat(raw tg.ChatClass, now time.Time) *RoundPermission {
	p := &RoundPermission{CheckedAtMS: now.UnixMilli(), Reason: "unknown"}
	allow := func() *RoundPermission { p.Allowed = ptr.Ptr(true); p.Reason = "allowed"; return p }
	deny := func(reason string) *RoundPermission { p.Allowed = ptr.Ptr(false); p.Reason = reason; return p }
	switch c := raw.(type) {
	case *tg.Chat:
		if c.Left || c.Deactivated {
			return deny("not_member")
		}
		if c.Creator || !c.AdminRights.Zero() {
			return allow()
		}
		if roundBanActive(c.DefaultBannedRights, now) {
			return deny("round_video_forbidden")
		}
		return allow()
	case *tg.Channel:
		if c.Min {
			return p
		}
		if c.Left || c.Restricted {
			return deny("not_member_or_restricted")
		}
		if c.Broadcast {
			if c.Creator || c.AdminRights.PostMessages {
				return allow()
			}
			return deny("post_messages_forbidden")
		}
		if c.Creator || !c.AdminRights.Zero() {
			return allow()
		}
		if roundBanActive(c.BannedRights, now) || roundBanActive(c.DefaultBannedRights, now) {
			return deny("round_video_forbidden")
		}
		return allow()
	default:
		return p
	}
}

func setRoundPermission(portal *bridgev2.Portal, login string, permission *RoundPermission) {
	meta := portal.Metadata.(*PortalMetadata)
	meta.roundPermissionLock.Lock()
	defer meta.roundPermissionLock.Unlock()
	if meta.RoundPermissions == nil {
		meta.RoundPermissions = make(map[string]*RoundPermission)
	}
	meta.RoundPermissions[login] = permission
}

func getRoundPermission(portal *bridgev2.Portal, login string) *RoundPermission {
	meta := portal.Metadata.(*PortalMetadata)
	meta.roundPermissionLock.RLock()
	defer meta.roundPermissionLock.RUnlock()
	p := meta.RoundPermissions[login]
	if p == nil {
		return nil
	}
	clone := *p
	if p.Allowed != nil {
		clone.Allowed = ptr.Ptr(*p.Allowed)
	}
	return &clone
}

// checkRoundPermission refreshes the remote policy at dispatch time. Cached
// capabilities are hints and cannot authorize a send after permissions change.
func (tc *TelegramClient) checkRoundPermission(ctx context.Context, portal *bridgev2.Portal) (*RoundPermission, error) {
	p := &RoundPermission{CheckedAtMS: time.Now().UnixMilli(), Reason: "unknown"}
	defer func() { setRoundPermission(portal, string(tc.userLogin.ID), p) }()
	if !tc.IsLoggedIn() {
		return p, errors.New("round permission requires an authenticated login")
	}
	peerType, peerID, topicID, err := ids.ParsePortalID(portal.ID)
	if err != nil {
		return p, err
	}
	if topicID == ids.TopicIDSpaceRoom {
		p.Allowed = ptr.Ptr(false)
		p.Reason = "space"
		return p, nil
	}
	peer, _, err := tc.inputPeerForPortalID(ctx, portal.ID)
	if err != nil {
		return p, err
	}
	switch peerType {
	case ids.PeerTypeUser:
		portal.Metadata.(*PortalMetadata).ProtectedContent = ptr.Ptr(false)
		input, err := tc.getInputUser(ctx, peerID)
		if err != nil {
			return p, err
		}
		full, err := tc.client.API().UsersGetFullUser(ctx, input)
		if err != nil {
			return p, err
		}
		p.Allowed = ptr.Ptr(!full.FullUser.VoiceMessagesForbidden && !full.FullUser.Blocked)
		if *p.Allowed {
			p.Reason = "allowed"
		} else {
			p.Reason = "voice_messages_forbidden_or_blocked"
		}
	case ids.PeerTypeChat:
		chats, err := tc.client.API().MessagesGetChats(ctx, []int64{peerID})
		if err != nil {
			return p, err
		}
		portal.Metadata.(*PortalMetadata).ProtectedContent = mediaProtectionFromChats(peerType, peerID, chats.GetChats())
		for _, raw := range chats.GetChats() {
			if raw.GetID() == peerID {
				p = roundPermissionFromChat(raw, time.Now())
				break
			}
		}
	case ids.PeerTypeChannel:
		channel, ok := peer.(*tg.InputPeerChannel)
		if !ok {
			return p, errors.New("round permission peer is not a channel")
		}
		chats, err := tc.client.API().ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash}})
		if err != nil {
			return p, err
		}
		portal.Metadata.(*PortalMetadata).ProtectedContent = mediaProtectionFromChats(peerType, peerID, chats.GetChats())
		for _, raw := range chats.GetChats() {
			if raw.GetID() == peerID {
				p = roundPermissionFromChat(raw, time.Now())
				break
			}
		}
		if topicID > 0 && p.Allowed != nil && *p.Allowed {
			topics, err := tc.client.API().MessagesGetForumTopicsByID(ctx, &tg.MessagesGetForumTopicsByIDRequest{Peer: peer, Topics: []int{topicID}})
			if err != nil {
				p.Allowed = nil
				p.Reason = "unknown_topic"
				return p, err
			}
			found := false
			for _, topic := range topics.Topics {
				if t, ok := topic.(*tg.ForumTopic); ok && t.ID == topicID {
					found = true
					if t.Closed {
						p.Allowed = ptr.Ptr(false)
						p.Reason = "topic_closed"
					}
				}
			}
			if !found {
				p.Allowed = nil
				p.Reason = "unknown_topic"
			}
		}
	}
	return p, nil
}
