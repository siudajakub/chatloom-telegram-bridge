package connector

import (
	"fmt"
	"math"
	"reflect"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

// roundSendAck is the parsed result of a round sendMedia/forward RPC.
//
// Like upstream's ordinary media path, an UpdateMessageID carrying our random
// ID (or UpdateShortSentMessage.ID) is Telegram's certain acceptance: the
// message exists under MessageID. The echoed message itself is optional,
// because Telegram may deliver updateNewMessage separately through the update
// stream or getDifference.
type roundSendAck struct {
	MessageID int
	Date      int
	// Message is the echoed message from the RPC result for the acknowledged
	// ID and destination peer, when present and unambiguous.
	Message *tg.Message
	// Media is the echoed media of an UpdateShortSentMessage.
	Media tg.MessageMediaClass
	// AckReason explains a missing acknowledgement (MessageID == 0).
	AckReason string
	// EchoReason explains why Message is nil although MessageID is known.
	EchoReason string
	// UpdateTypes lists TL type names in the result, for diagnostics only.
	UpdateTypes []string
}

func roundUpdateTypeName(update any) string {
	t := reflect.TypeOf(update)
	if t == nil {
		return "nil"
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}

// parseRoundSendResult extracts the certain acknowledgement and, when present,
// the echoed message. It never fails because of the echo's media profile.
func parseRoundSendResult(updates tg.UpdatesClass, randomID int64, expectedPeer tg.InputPeerClass) roundSendAck {
	var ack roundSendAck
	var all []tg.UpdateClass
	switch u := updates.(type) {
	case *tg.UpdateShortSentMessage:
		ack.UpdateTypes = []string{roundUpdateTypeName(u)}
		if u.ID <= 0 {
			ack.AckReason = "ack_invalid_id"
			return ack
		}
		ack.MessageID, ack.Date = u.ID, u.Date
		ack.Media = u.Media
		if ack.Media == nil {
			ack.EchoReason = "echo_absent"
		}
		return ack
	case *tg.Updates:
		all, ack.Date = u.Updates, u.Date
	case *tg.UpdatesCombined:
		all, ack.Date = u.Updates, u.Date
	default:
		ack.UpdateTypes = []string{roundUpdateTypeName(updates)}
		ack.AckReason = "response_type_unsupported"
		return ack
	}
	for _, update := range all {
		ack.UpdateTypes = append(ack.UpdateTypes, roundUpdateTypeName(update))
		if v, ok := update.(*tg.UpdateMessageID); ok && v.RandomID == randomID {
			if v.ID <= 0 {
				ack.AckReason = "ack_invalid_id"
				ack.MessageID = 0
				return ack
			}
			if ack.MessageID != 0 && ack.MessageID != v.ID {
				ack.AckReason = "ack_conflict"
				ack.MessageID = 0
				return ack
			}
			ack.MessageID = v.ID
		}
	}
	if ack.MessageID == 0 {
		ack.AckReason = "ack_missing_random_id"
		return ack
	}
	var candidates []*tg.Message
	peerMismatch := false
	for _, update := range all {
		var candidate tg.MessageClass
		switch v := update.(type) {
		case *tg.UpdateNewMessage:
			candidate = v.Message
		case *tg.UpdateNewChannelMessage:
			candidate = v.Message
		}
		msg, ok := candidate.(*tg.Message)
		if !ok || msg.ID != ack.MessageID {
			continue
		}
		if expectedPeer != nil && !controlPeerMatches(expectedPeer, "", msg.PeerID) {
			peerMismatch = true
			continue
		}
		candidates = append(candidates, msg)
	}
	switch {
	case len(candidates) == 1:
		ack.Message = candidates[0]
		if ack.Message.Date > 0 {
			ack.Date = ack.Message.Date
		}
	case len(candidates) > 1:
		ack.EchoReason = "echo_ambiguous"
	case peerMismatch:
		ack.EchoReason = "echo_peer_mismatch"
	default:
		ack.EchoReason = "echo_absent"
	}
	return ack
}

// roundEchoMessageReason checks an echoed message: "" means it is an outgoing
// video note matching our profile, otherwise a reason code.
func roundEchoMessageReason(msg *tg.Message) string {
	if msg == nil {
		return "echo_absent"
	}
	if !msg.Out {
		return "echo_not_outgoing"
	}
	return roundEchoMediaReason(msg.Media)
}

// roundEchoMediaReason returns "" when the media is a durable MP4 video note.
func roundEchoMediaReason(media tg.MessageMediaClass) string {
	docMedia, ok := media.(*tg.MessageMediaDocument)
	if !ok {
		return "echo_media_missing"
	}
	if docMedia.TTLSeconds != 0 {
		return "echo_ttl_media"
	}
	doc, ok := docMedia.Document.(*tg.Document)
	if !ok || doc.MimeType != "video/mp4" {
		return "echo_not_mp4"
	}
	count := 0
	for _, attr := range doc.Attributes {
		switch a := attr.(type) {
		case *tg.DocumentAttributeAnimated:
			return "echo_animated"
		case *tg.DocumentAttributeVideo:
			count++
			switch {
			case !a.RoundMessage:
				return "echo_round_flag_missing"
			case a.W != a.H || a.W < 2 || a.W > roundMaxDimension:
				return "echo_dimensions"
			case a.Duration <= 0 || a.Duration > 60 || math.IsNaN(a.Duration) || math.IsInf(a.Duration, 0):
				return "echo_duration"
			}
		}
	}
	if count != 1 {
		return "echo_video_attribute_count"
	}
	return ""
}

// canonicalRoundSendUpdates is the strict form used by native forwarding,
// where normal remote conversion later persists the echo. It requires a
// certain acknowledgement. An echoed message is optional (it may arrive via
// the update stream), but when present it must be an outgoing video note in
// the destination. Unrelated batched updates are removed.
func canonicalRoundSendUpdates(updates tg.UpdatesClass, randomID int64, expectedPeer ...tg.InputPeerClass) (tg.UpdatesClass, error) {
	var peer tg.InputPeerClass
	if len(expectedPeer) > 0 {
		peer = expectedPeer[0]
	}
	ack := parseRoundSendResult(updates, randomID, peer)
	if ack.MessageID == 0 {
		return nil, fmt.Errorf("round response not acknowledged: %s", ack.AckReason)
	}
	if short, ok := updates.(*tg.UpdateShortSentMessage); ok {
		if reason := roundEchoMediaReason(short.Media); reason != "" {
			return nil, fmt.Errorf("round response echo rejected: %s", reason)
		}
		return short, nil
	}
	filtered := []tg.UpdateClass{&tg.UpdateMessageID{ID: ack.MessageID, RandomID: randomID}}
	switch ack.EchoReason {
	case "":
		var reason string
		if peer != nil {
			reason = roundEchoMessageReason(ack.Message)
		} else {
			reason = roundEchoMediaReason(ack.Message.Media)
		}
		if reason != "" {
			return nil, fmt.Errorf("round response echo rejected: %s", reason)
		}
		var all []tg.UpdateClass
		switch u := updates.(type) {
		case *tg.Updates:
			all = u.Updates
		case *tg.UpdatesCombined:
			all = u.Updates
		}
		for _, update := range all {
			switch v := update.(type) {
			case *tg.UpdateNewMessage:
				if v.Message == tg.MessageClass(ack.Message) {
					filtered = append(filtered, update)
				}
			case *tg.UpdateNewChannelMessage:
				if v.Message == tg.MessageClass(ack.Message) {
					filtered = append(filtered, update)
				}
			}
		}
	case "echo_absent":
		// Acknowledged; the message itself arrives through the update stream.
	default:
		return nil, fmt.Errorf("round response echo rejected: %s", ack.EchoReason)
	}
	return &tg.Updates{Updates: filtered, Date: ack.Date}, nil
}

// TTL/protected media and send options need their own implemented protocol.
// A request for one must never silently become an ordinary durable video note.
func rejectUnsupportedRoundOptions(raw, info map[string]any) error {
	for _, fields := range []map[string]any{raw, info} {
		for _, key := range []string{"fi.mau.telegram.ttl_seconds", "ttl_seconds", "view_once", "pl.personal.chat.telegram.media_policy", "com.beeper.view_limited"} {
			if _, present := fields[key]; present {
				return roundError("restricted_options_unsupported", nil)
			}
		}
	}
	return nil
}
