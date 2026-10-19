package connector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func roundEchoMedia(round bool) tg.MessageMediaClass {
	return &tg.MessageMediaDocument{Document: &tg.Document{MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{RoundMessage: round, W: 640, H: 640, Duration: 1.25}}}}
}

// realisticRoundEchoMedia mirrors what Telegram returns for a sent video note:
// Video and Round flags on the media, streaming/preload/codec details on the
// attribute and, for third-party clients, an optional filename attribute.
func realisticRoundEchoMedia(filename bool) tg.MessageMediaClass {
	video := &tg.DocumentAttributeVideo{RoundMessage: true, SupportsStreaming: true, W: 384, H: 384, Duration: 9.97}
	video.SetPreloadPrefixSize(65536)
	video.SetVideoCodec("h264")
	attrs := []tg.DocumentAttributeClass{video}
	if filename {
		attrs = append(attrs, &tg.DocumentAttributeFilename{FileName: "round.mp4"})
	}
	media := &tg.MessageMediaDocument{Video: true, Round: true, Document: &tg.Document{ID: 99, Size: 1_250_000, MimeType: "video/mp4", Attributes: attrs}}
	return media
}

func TestRoundSendResultAcknowledgementShapes(t *testing.T) {
	dm := &tg.InputPeerUser{UserID: 777}
	echo := func(id int, peer tg.PeerClass, out bool, media tg.MessageMediaClass) *tg.Message {
		return &tg.Message{ID: id, Date: 100, Out: out, PeerID: peer, Media: media}
	}
	for _, test := range []struct {
		name       string
		response   tg.UpdatesClass
		peer       tg.InputPeerClass
		ackID      int
		ackReason  string
		hasEcho    bool
		echoReason string
	}{
		{"ack only (updateMessageID, no updateNewMessage)", &tg.Updates{Date: 50, Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
		}}, dm, 3, "", false, "echo_absent"},
		{"ack only with read outbox and foreign updates", &tg.Updates{Date: 50, Updates: []tg.UpdateClass{
			&tg.UpdateReadHistoryOutbox{Peer: &tg.PeerUser{UserID: 777}, MaxID: 2},
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateUserStatus{UserID: 777, Status: &tg.UserStatusOnline{}},
		}}, dm, 3, "", false, "echo_absent"},
		{"realistic DM updates with echo", &tg.Updates{Date: 50, Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateReadHistoryOutbox{Peer: &tg.PeerUser{UserID: 777}, MaxID: 2},
			&tg.UpdateNewMessage{Message: echo(3, &tg.PeerUser{UserID: 777}, true, realisticRoundEchoMedia(false)), Pts: 10, PtsCount: 1},
		}}, dm, 3, "", true, ""},
		{"combined updates", &tg.UpdatesCombined{Date: 50, Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewMessage{Message: echo(3, &tg.PeerUser{UserID: 777}, true, roundEchoMedia(true))},
		}}, dm, 3, "", true, ""},
		{"saved messages", &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewMessage{Message: echo(3, &tg.PeerUser{UserID: 456}, true, roundEchoMedia(true))},
		}}, &tg.InputPeerUser{UserID: 456}, 3, "", true, ""},
		{"supergroup channel message", &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewChannelMessage{Message: echo(3, &tg.PeerChannel{ChannelID: 5}, true, roundEchoMedia(true))},
		}}, &tg.InputPeerChannel{ChannelID: 5}, 3, "", true, ""},
		{"short sent message", &tg.UpdateShortSentMessage{ID: 3, Date: 60, Media: roundEchoMedia(true)}, dm, 3, "", false, ""},
		{"short sent message without media", &tg.UpdateShortSentMessage{ID: 3, Date: 60}, dm, 3, "", false, "echo_absent"},
		{"foreign random ID only", &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 3, RandomID: 43}}}, dm, 0, "ack_missing_random_id", false, ""},
		{"conflicting acknowledgements", &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 3, RandomID: 42}, &tg.UpdateMessageID{ID: 4, RandomID: 42}}}, dm, 0, "ack_conflict", false, ""},
		{"too long", &tg.UpdatesTooLong{}, dm, 0, "response_type_unsupported", false, ""},
		{"echo for other ID", &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewMessage{Message: echo(4, &tg.PeerUser{UserID: 777}, true, roundEchoMedia(true))},
		}}, dm, 3, "", false, "echo_absent"},
		{"echo in other peer", &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewChannelMessage{Message: echo(3, &tg.PeerChannel{ChannelID: 9}, true, roundEchoMedia(true))},
		}}, dm, 3, "", false, "echo_peer_mismatch"},
		{"ambiguous echo", &tg.Updates{Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: 3, RandomID: 42},
			&tg.UpdateNewMessage{Message: echo(3, &tg.PeerUser{UserID: 777}, true, roundEchoMedia(true))},
			&tg.UpdateNewMessage{Message: echo(3, &tg.PeerUser{UserID: 777}, true, roundEchoMedia(true))},
		}}, dm, 3, "", false, "echo_ambiguous"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ack := parseRoundSendResult(test.response, 42, test.peer)
			require.Equal(t, test.ackID, ack.MessageID)
			require.Equal(t, test.ackReason, ack.AckReason)
			require.Equal(t, test.hasEcho, ack.Message != nil)
			if test.ackID != 0 {
				require.Equal(t, test.echoReason, ack.EchoReason)
			}
			require.NotEmpty(t, ack.UpdateTypes)
		})
	}
}

func TestRoundEchoProfileReasons(t *testing.T) {
	animated := realisticRoundEchoMedia(false).(*tg.MessageMediaDocument)
	animated.Document.(*tg.Document).Attributes = append(animated.Document.(*tg.Document).Attributes, &tg.DocumentAttributeAnimated{})
	for _, test := range []struct {
		name   string
		msg    *tg.Message
		reason string
	}{
		{"realistic", &tg.Message{Out: true, Media: realisticRoundEchoMedia(false)}, ""},
		{"realistic with filename", &tg.Message{Out: true, Media: realisticRoundEchoMedia(true)}, ""},
		{"downgraded to ordinary video", &tg.Message{Out: true, Media: roundEchoMedia(false)}, "echo_round_flag_missing"},
		{"incoming", &tg.Message{Media: roundEchoMedia(true)}, "echo_not_outgoing"},
		{"no media", &tg.Message{Out: true}, "echo_media_missing"},
		{"ttl", &tg.Message{Out: true, Media: &tg.MessageMediaDocument{TTLSeconds: 5, Document: &tg.Document{MimeType: "video/mp4"}}}, "echo_ttl_media"},
		{"animated", &tg.Message{Out: true, Media: animated}, "echo_animated"},
		{"oversize", &tg.Message{Out: true, Media: &tg.MessageMediaDocument{Document: &tg.Document{MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{RoundMessage: true, W: 1280, H: 1280, Duration: 1}}}}}, "echo_dimensions"},
		{"zero duration", &tg.Message{Out: true, Media: &tg.MessageMediaDocument{Document: &tg.Document{MimeType: "video/mp4", Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeVideo{RoundMessage: true, W: 384, H: 384}}}}}, "echo_duration"},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.reason, roundEchoMessageReason(test.msg))
		})
	}
}

func TestRoundCanonicalForwardAcceptsAckOnly(t *testing.T) {
	canonical, err := canonicalRoundSendUpdates(&tg.Updates{Date: 7, Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 3, RandomID: 42}, &tg.UpdateReadHistoryOutbox{}}}, 42, &tg.InputPeerChat{ChatID: 1})
	require.NoError(t, err)
	filtered := canonical.(*tg.Updates)
	require.Len(t, filtered.Updates, 1)
	require.Equal(t, 3, filtered.Updates[0].(*tg.UpdateMessageID).ID)
	// A present echo is still checked strictly on the forward path.
	_, err = canonicalRoundSendUpdates(&tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateMessageID{ID: 3, RandomID: 42},
		&tg.UpdateNewMessage{Message: &tg.Message{ID: 3, Out: true, PeerID: &tg.PeerChat{ChatID: 1}, Media: roundEchoMedia(false)}},
	}}, 42, &tg.InputPeerChat{ChatID: 1})
	require.Error(t, err)
	_, err = canonicalRoundSendUpdates(&tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateMessageID{ID: 3, RandomID: 43}}}, 42)
	require.Error(t, err)
}
