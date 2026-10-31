// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/connector/waveform"
	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func TestTelegramVoiceUploadMIME(t *testing.T) {
	for _, test := range []struct {
		name, input, expected string
	}{
		{"opus parameter", "audio/ogg; codecs=opus", "audio/ogg"},
		{"quoted parameter", `audio/ogg; codecs="opus"`, "audio/ogg"},
		{"multiple parameters", "audio/ogg; codecs=opus; rate=48000", "audio/ogg"},
		{"case and whitespace", " Audio/Ogg ; CODECS=Opus ", "audio/ogg"},
		{"plain ogg", "audio/ogg", "audio/ogg"},
		{"plain mp4", "audio/mp4", "audio/mp4"},
		{"mp4 parameter", "audio/mp4; codecs=mp4a.40.2", "audio/mp4"},
		{"plain mp3", "audio/mpeg", "audio/mpeg"},
		{"mp3 case", "AUDIO/MPEG", "audio/mpeg"},
		{"unknown audio", "audio/flac; rate=48000", "audio/flac; rate=48000"},
		{"unknown alias", "audio/m4a", "audio/m4a"},
		{"video declaration", "video/mp4", "video/mp4"},
		{"application declaration", "application/ogg", "application/ogg"},
		{"malformed type", "not a mime type", "not a mime type"},
		{"malformed parameter", "audio/ogg; codecs", "audio/ogg; codecs"},
		{"conflicting parameters", "audio/ogg; codecs=opus; codecs=vorbis", "audio/ogg; codecs=opus; codecs=vorbis"},
		{"empty declaration", "", "application/octet-stream"},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, voice := range []bool{false, true} {
				content := &event.MessageEventContent{
					MsgType: event.MsgAudio,
					Info:    &event.FileInfo{MimeType: test.input},
				}
				if voice {
					content.MSC3245Voice = &event.MSC3245Voice{}
				}
				got := makeTelegramUploadedDocument(nil, content, test.input, nil)
				want := test.expected
				if !voice && test.input != "" {
					want = test.input
				}
				require.Equal(t, want, got.MimeType, "voice=%t", voice)
				require.Equal(t, test.input, content.Info.MimeType, "source MIME must remain unchanged")
			}
		})
	}
	content := &event.MessageEventContent{
		MsgType:      event.MsgFile,
		Info:         &event.FileInfo{MimeType: "audio/ogg; codecs=opus"},
		MSC3245Voice: &event.MSC3245Voice{},
	}
	require.Equal(t, content.Info.MimeType, makeTelegramUploadedDocument(nil, content, content.Info.MimeType, nil).MimeType,
		"a marker on another message type must not change its MIME")
}

func TestTelegramUploadedDocumentConvertedMIME(t *testing.T) {
	// Use the upload's effective MIME after conversion, even when the source
	// content has no info or still describes the original file.
	for _, originalInfo := range []*event.FileInfo{nil, {MimeType: "image/webp"}} {
		content := &event.MessageEventContent{MsgType: event.MsgImage, Info: originalInfo}
		document := makeTelegramUploadedDocument(nil, content, "image/jpeg", nil)
		require.Equal(t, "image/jpeg", document.MimeType)
		require.Same(t, originalInfo, content.Info)
		if originalInfo != nil {
			require.Equal(t, "image/webp", originalInfo.MimeType)
		}
	}
}

func TestTelegramAudioSendRequestTL(t *testing.T) {
	for _, test := range []struct {
		name, marker, mime, expectedMIME string
		voice                            bool
	}{
		{"voice ogg", `,"org.matrix.msc3245.voice":{}`, "audio/ogg; codecs=opus", "audio/ogg", true},
		{"voice mp4", `,"org.matrix.msc3245.voice":{}`, "audio/mp4", "audio/mp4", true},
		{"voice mp3", `,"org.matrix.msc3245.voice":{}`, "audio/mpeg", "audio/mpeg", true},
		{"ordinary audio", "", "audio/ogg; codecs=opus", "audio/ogg; codecs=opus", false},
		{"null marker", `,"org.matrix.msc3245.voice":null`, "audio/ogg; codecs=opus", "audio/ogg; codecs=opus", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Parse the Matrix wire shape, including the legacy empty-object marker.
			payload := `{"msgtype":"m.audio","body":"voice_message_synthetic.ogg","info":{"duration":12345,"mimetype":"` + test.mime + `"},"org.matrix.msc1767.audio":{"duration":12345,"waveform":[0,128,512,1024]}` + test.marker + `}`
			var content event.Content
			require.NoError(t, json.Unmarshal([]byte(payload), &content))
			require.NoError(t, content.ParseRaw(event.EventMessage))
			message := content.AsMessage()
			samples := append([]int(nil), message.MSC1767Audio.Waveform...)
			attributes := []tg.DocumentAttributeClass{
				&tg.DocumentAttributeFilename{FileName: message.Body},
				makeTelegramAudioAttribute(message),
			}
			media := makeTelegramUploadedDocument(&tg.InputFile{
				ID: 1, Parts: 1, Name: message.Body, MD5Checksum: "synthetic",
			}, message, message.Info.MimeType, attributes)
			request := &tg.MessagesSendMediaRequest{
				Peer:     &tg.InputPeerSelf{},
				Media:    media,
				RandomID: 123,
				ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: 456},
			}
			var buffer bin.Buffer
			require.NoError(t, request.Encode(&buffer))
			var decoded tg.MessagesSendMediaRequest
			require.NoError(t, decoded.Decode(&buffer))
			document, ok := decoded.Media.(*tg.InputMediaUploadedDocument)
			require.True(t, ok)
			require.Equal(t, test.expectedMIME, document.MimeType)
			require.False(t, document.ForceFile)
			require.False(t, document.Flags.Has(4))
			require.Len(t, document.Attributes, 2)
			require.Equal(t, message.Body, document.Attributes[0].(*tg.DocumentAttributeFilename).FileName)
			audio, ok := document.Attributes[1].(*tg.DocumentAttributeAudio)
			require.True(t, ok)
			require.Equal(t, test.voice, audio.Voice)
			require.Equal(t, test.voice, audio.Flags.Has(10), "voice must survive the actual TL request codec")
			require.Equal(t, 12, audio.Duration)
			require.Equal(t, waveform.Encode(samples), audio.Waveform)
			require.True(t, audio.Flags.Has(2))
			require.Empty(t, audio.Title)
			require.Empty(t, audio.Performer)
			require.Equal(t, int64(123), decoded.RandomID)
			require.Equal(t, 456, decoded.ReplyTo.(*tg.InputReplyToMessage).ReplyToMsgID)
			require.Equal(t, test.mime, message.Info.MimeType)
			require.Equal(t, samples, message.MSC1767Audio.Waveform)
		})
	}
}

func TestTelegramAudioAttributeWithoutMetadata(t *testing.T) {
	content := &event.MessageEventContent{MsgType: event.MsgAudio, MSC3245Voice: &event.MSC3245Voice{}}
	attribute := makeTelegramAudioAttribute(content)
	require.True(t, attribute.Voice)
	require.Zero(t, attribute.Duration)
	require.Nil(t, attribute.Waveform)
	content.MSC1767Audio = &event.MSC1767Audio{}
	require.Nil(t, makeTelegramAudioAttribute(content).Waveform)
}
