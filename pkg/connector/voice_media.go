// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"cmp"
	"mime"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/connector/waveform"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

func makeTelegramAudioAttribute(content *event.MessageEventContent) *tg.DocumentAttributeAudio {
	attribute := &tg.DocumentAttributeAudio{
		Voice:    content.MSC3245Voice != nil,
		Duration: content.GetInfo().Duration / 1000,
	}
	if content.MSC1767Audio != nil && len(content.MSC1767Audio.Waveform) > 0 {
		attribute.Waveform = waveform.Encode(content.MSC1767Audio.Waveform)
	}
	return attribute
}

func makeTelegramUploadedDocument(upload tg.InputFileClass, content *event.MessageEventContent, mimeType string, attributes []tg.DocumentAttributeClass) *tg.InputMediaUploadedDocument {
	if content.MsgType == event.MsgAudio && content.MSC3245Voice != nil {
		// Match TDLib's canonical voice MIME types. Removing parameters from a
		// known type does not convert the file: preserve unknown or malformed
		// declarations instead of relabelling their bytes as Ogg.
		if parsed, _, err := mime.ParseMediaType(mimeType); err == nil {
			switch parsed {
			case "audio/ogg", "audio/mpeg", "audio/mp4":
				mimeType = parsed
			}
		}
	}
	return &tg.InputMediaUploadedDocument{
		File:       upload,
		MimeType:   cmp.Or(mimeType, "application/octet-stream"),
		Attributes: attributes,
	}
}
