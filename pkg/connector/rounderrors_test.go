// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

func requireHumanText(t *testing.T, code, text string) {
	t.Helper()
	require.NotEmpty(t, text, code)
	require.NotContains(t, text, "_", code)
	require.NotContains(t, text, "{", code)
	require.NotContains(t, text, "}", code)
	require.True(t, strings.HasSuffix(text, "."), code)
}

func TestRoundUserMessagesAreHuman(t *testing.T) {
	for _, code := range roundErrorCodes {
		for _, wait := range []int{0, 1, 30} {
			requireHumanText(t, code, roundUserMessage(code, wait))
		}
	}
	require.Equal(t, "Telegram asks you to wait 30 seconds before sending again.", roundUserMessage("FLOOD_WAIT", 30))
	require.Equal(t, "Telegram asks you to wait a moment before sending again.", roundUserMessage("SLOWMODE_WAIT", 0))
}

// Every code literal of the package must be in roundErrorCodes, so a new code
// cannot reach a user as a raw string. Run from the package directory.
func TestRoundErrorCodesCoverSource(t *testing.T) {
	pattern := regexp.MustCompile(`(?:roundError|mediaControlRejected)\("([A-Za-z_]+)"|Code: +"([A-Za-z_]+)"`)
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	seen := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		for _, match := range pattern.FindAllStringSubmatch(string(data), -1) {
			code := match[1] + match[2]
			seen++
			require.True(t, slices.Contains(roundErrorCodes, code), "code %q in %s has no entry in roundErrorCodes", code, file)
		}
	}
	require.Greater(t, seen, 50)
}

func TestHumaniseRoundPreClaimErrors(t *testing.T) {
	tc := &TelegramClient{}
	for _, code := range roundMediaErrorCodes {
		status := bridgev2.WrapErrorInStatus(tc.roundTransferError(roundError(code, errors.New("synthetic cause with /path and mxc://example.invalid/x"))))
		requireHumanText(t, code, status.Message)
		require.Equal(t, code, status.InternalError.Error(), "internal_error carries only the code")
		if roundRetriableMediaCodes[code] {
			require.Equal(t, event.MessageStatusRetriable, status.Status, code)
		} else {
			require.Equal(t, event.MessageStatusFail, status.Status, code)
			require.Equal(t, event.MessageStatusUnsupported, status.ErrorReason, code)
			require.True(t, status.IsCertain, code)
		}
		// The original typed error is still reachable for classification.
		var typed *RoundMediaError
		require.ErrorAs(t, status, &typed)
	}
	for _, code := range []string{"duration_limit", "invalid_video_profile", "invalid_stream_count", "file_size_limit", "truncated_encode"} {
		status := bridgev2.WrapErrorInStatus(tc.roundTransferError(roundError(code, nil)))
		require.Equal(t, event.MessageStatusFail, status.Status, code)
	}
	upload := bridgev2.WrapErrorInStatus(tc.roundTransferError(roundUploadError(errors.New("synthetic network"))))
	require.Equal(t, event.MessageStatusRetriable, upload.Status)
	require.Equal(t, "upload_failed", upload.InternalError.Error())
	flood := bridgev2.WrapErrorInStatus(tc.roundTransferError(roundUploadError(tgerr.New(420, "FLOOD_WAIT_17"))))
	require.Equal(t, event.MessageStatusRetriable, flood.Status)
	require.Equal(t, "Telegram asks you to wait 17 seconds before sending again.", flood.Message)
	require.Equal(t, "FLOOD_WAIT", flood.InternalError.Error())
}

func TestHumaniseSendErrorTexts(t *testing.T) {
	tc := &TelegramClient{}
	for _, test := range []struct {
		err     error
		message string
	}{
		{&RoundOperationError{Code: "round_already_sent_unmapped", Certainty: "certain"}, "This video message was already sent to Telegram."},
		{&RoundOperationError{Code: "transport_unavailable", Certainty: "uncertain", Retriable: true}, roundMessageUnconfirmed},
		{&RoundOperationError{Code: "dispatch_in_progress", Certainty: "uncertain", Retriable: true}, "This video message is still being sent."},
		{classifyRoundOperationError(tgerr.New(400, "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN"), true), "You can't send video messages in this chat."},
		{classifyRoundOperationError(tgerr.New(420, "SLOWMODE_WAIT_9"), true), "Telegram asks you to wait 9 seconds before sending again."},
		// Ordinary (non-round) sends.
		{tgerr.New(420, "FLOOD_WAIT_30"), "Telegram asks you to wait 30 seconds before sending again."},
		{tgerr.New(400, "SYNTHETIC_UNMAPPED_ERROR"), "Telegram didn't accept this message."},
	} {
		status := bridgev2.WrapErrorInStatus(tc.humaniseSendError(test.err))
		require.Equal(t, test.message, status.Message)
		require.NotContains(t, status.Message, "{")
	}
	// Humanising twice keeps the classification.
	once := tc.humaniseSendError(&RoundOperationError{Code: "duration_limit", Certainty: "certain"})
	twice := bridgev2.WrapErrorInStatus(tc.humaniseSendError(once))
	require.Equal(t, "Video messages can be up to 60 seconds long.", twice.Message)
	require.Equal(t, event.MessageStatusFail, twice.Status)
}

func TestHumaniseTelegramMessageFillsSeconds(t *testing.T) {
	text := humaniseTelegramMessage(tgerr.New(420, "SLOWMODE_WAIT_5"))
	require.Equal(t, "Telegram asks you to wait 5 seconds before sending again.", text)
	require.NotContains(t, humaniseTelegramMessage(tgerr.New(420, "FLOOD_PREMIUM_WAIT_0")), "{")
	require.NotContains(t, humaniseTelegramMessage(tgerr.New(420, "2FA_CONFIRM_WAIT_X")), "{")
	// Upstream (#1119) fills its own templates now.
	require.Equal(t, "Part 3 of the file is missing from storage", humaniseTelegramMessage(tgerr.New(400, "FILE_PART_3_MISSING")))
	// ...but still leaves "{minutes}" and empty texts in a few entries.
	require.Equal(t, "Telegram didn't accept this message.", humaniseTelegramMessage(tgerr.New(400, "PREVIOUS_CHAT_IMPORT_ACTIVE_WAIT_XMIN")))
	require.Equal(t, "Telegram didn't accept this message.", humaniseTelegramMessage(tgerr.New(400, "PREMIUM_CURRENTLY_UNAVAILABLE")))
	// Server errors are hidden upstream.
	require.Equal(t, "Telegram is having internal issues, please try again later.", humaniseTelegramMessage(tgerr.New(500, "SYNTHETIC_INTERNAL")))
}
