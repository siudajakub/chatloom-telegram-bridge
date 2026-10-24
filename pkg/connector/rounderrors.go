// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"slices"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-telegram/pkg/connector/humanise"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// roundRetriableMediaCodes are pre-dispatch failures that can succeed when the
// same message is sent again. Every other RoundMediaError is permanent for
// this media (profile, size, duration, marker).
var roundRetriableMediaCodes = map[string]bool{
	"upload_limit_unknown":  true,
	"encode_timeout":        true,
	"transfer_timeout":      true,
	"temporary_storage":     true,
	"media_download_failed": true,
	"upload_failed":         true,
}

// Retriable reports whether sending the same round again may work.
func (e *RoundMediaError) Retriable() bool {
	return e != nil && roundRetriableMediaCodes[e.Code]
}

// roundMediaErrorCodes are the RoundMediaError codes (pre-dispatch
// preparation of a round send).
var roundMediaErrorCodes = []string{
	"caption_not_supported", "duration_limit", "editing_not_supported", "encode_failed", "encode_timeout",
	"file_size_limit", "invalid_audio_profile", "invalid_container", "invalid_event_type", "invalid_frame_rate",
	"invalid_marker", "invalid_media_url", "invalid_message_type", "invalid_orientation", "invalid_pixel_format",
	"invalid_profile", "invalid_stream_count", "invalid_thumbnail", "invalid_video_profile", "media_decrypt_failed",
	"media_download_failed", "media_download_unavailable", "probe_failed", "temporary_storage", "thumbnail_failed",
	"thumbnail_size_limit", "transfer_timeout", "truncated_encode", "unreadable_file", "unsanitized_thumbnail",
	"unsupported_stream", "upload_failed", "upload_limit_unknown",
}

// roundErrorCodes lists every fork error code and the Telegram RPC types the
// fork classifies. rounderrors_test.go checks that each has a human text and
// that every code literal in the package source is listed here.
var roundErrorCodes = append(slices.Clone(roundMediaErrorCodes),
	// Send options.
	"document_fallback_forbidden", "invalid_schedule_date", "invalid_send_options", "invalid_silent_option",
	"restricted_options_unsupported", "scheduled_send_not_supported", "scheduling_unsupported",
	"unsupported_send_option", "unsupported_send_options_version",
	// Operation ledger and dispatch (RoundOperationError).
	"dispatch_in_progress", "identity_changed", "round_account_binding_mismatch", "round_ack_missing",
	"round_already_sent_unmapped", "round_cached_result_invalid", "round_forward_echo_unknown",
	"round_permission_denied", "round_permission_unknown", "round_result_persistence_failed",
	"round_send_identity_missing", "round_send_response_invalid", "round_send_response_missing",
	"transport_unavailable",
	// Media controls (played / forward).
	"bridge_echo", "contents_read_rejected", "destination_mapping_changed", "destination_round_forbidden",
	"encryption_required", "incoming_media_required", "invalid_destination", "invalid_payload", "invalid_source",
	"not_round_media", "own_login_required", "playback_not_authorized", "portal_forbidden", "portal_not_owned",
	"protected_or_unknown_source", "protected_source", "room_membership_required", "room_permission_denied",
	"sender_forbidden", "source_mapping_mismatch", "source_media_unavailable", "source_policy_unknown",
	"stale_request", "ttl_media_unavailable", "unsupported_control", "untrusted_device",
	// Telegram RPC error types.
	"CHAT_ADMIN_REQUIRED", "CHAT_RESTRICTED", "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN", "CHAT_WRITE_FORBIDDEN",
	"FLOOD_PREMIUM_WAIT", "FLOOD_WAIT", "RANDOM_ID_DUPLICATE", "SLOWMODE_WAIT", "USER_BANNED_IN_CHANNEL",
	"USER_IS_BLOCKED", "VOICE_MESSAGES_FORBIDDEN", "YOU_BLOCKED_USER", "round_forbidden",
)

const roundMessageUnknown = "Telegram didn't accept this video message."
const roundMessageUnconfirmed = "Telegram didn't confirm this video message. It may still appear in the chat."

// roundUserMessage is the sentence shown in the Matrix message status for a
// fork code (wave 5 TF-03, fork side of BR-15). It never contains a code,
// an underscore or a placeholder. waitSeconds is Telegram's FLOOD_WAIT /
// SLOWMODE_WAIT argument (0 when unknown).
func roundUserMessage(code string, waitSeconds int) string {
	switch code {
	case "encode_timeout", "transfer_timeout", "temporary_storage", "media_download_failed", "upload_failed",
		"upload_limit_unknown", "transport_unavailable", "round_permission_unknown", "round_cached_result_invalid",
		"round_send_response_invalid", "round_send_response_missing", "round_result_persistence_failed":
		return "Telegram couldn't process this video message. Try sending it again."
	case "media_download_unavailable", "media_decrypt_failed", "invalid_media_url", "unreadable_file":
		return "This video message couldn't be read. Record it again."
	case "dispatch_in_progress":
		return "This video message is still being sent."
	case "round_ack_missing", "round_forward_echo_unknown":
		return roundMessageUnconfirmed
	case "duration_limit":
		return "Video messages can be up to 60 seconds long."
	case "file_size_limit", "thumbnail_size_limit":
		return "This video message is too large for Telegram."
	case "probe_failed", "encode_failed", "truncated_encode", "unsupported_stream", "thumbnail_failed",
		"unsanitized_thumbnail", "invalid_audio_profile", "invalid_container", "invalid_frame_rate",
		"invalid_orientation", "invalid_pixel_format", "invalid_profile", "invalid_stream_count",
		"invalid_thumbnail", "invalid_video_profile", "invalid_marker", "invalid_message_type", "invalid_event_type",
		"document_fallback_forbidden":
		return "This video can't be sent as a Telegram video message."
	case "caption_not_supported":
		return "Telegram video messages can't have a caption."
	case "editing_not_supported":
		return "Telegram video messages can't be edited."
	case "restricted_options_unsupported", "scheduled_send_not_supported", "scheduling_unsupported",
		"invalid_schedule_date":
		return "Scheduled and view-once video messages aren't supported yet."
	case "invalid_send_options", "invalid_silent_option", "unsupported_send_option", "unsupported_send_options_version":
		return "These send options aren't supported for Telegram video messages."
	case "round_permission_denied", "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN", "round_forbidden", "destination_round_forbidden":
		return "You can't send video messages in this chat."
	case "VOICE_MESSAGES_FORBIDDEN":
		return "This person doesn't accept voice and video messages."
	case "CHAT_WRITE_FORBIDDEN", "CHAT_RESTRICTED", "CHAT_ADMIN_REQUIRED", "USER_BANNED_IN_CHANNEL":
		return "You can't send messages in this chat."
	case "YOU_BLOCKED_USER":
		return "You blocked this person on Telegram."
	case "USER_IS_BLOCKED":
		return "This person can't receive your messages on Telegram."
	case "FLOOD_WAIT", "FLOOD_PREMIUM_WAIT", "SLOWMODE_WAIT":
		return telegramWaitMessage(waitSeconds)
	case "RANDOM_ID_DUPLICATE", "round_already_sent_unmapped":
		return "This video message was already sent to Telegram."
	case "identity_changed", "round_send_identity_missing":
		return "This video message changed while it was being sent. Send it again as a new message."
	case "round_account_binding_mismatch", "own_login_required", "portal_not_owned":
		return "This chat is connected through a different Telegram account."
	case "stale_request":
		return "This request expired. Try again."
	case "protected_source", "protected_or_unknown_source", "source_policy_unknown":
		return "This video message is protected and can only be opened in Telegram."
	case "ttl_media_unavailable", "source_media_unavailable", "not_round_media", "source_mapping_mismatch",
		"contents_read_rejected", "incoming_media_required":
		return "This video message is no longer available in Telegram."
	case "encryption_required", "untrusted_device", "bridge_echo", "sender_forbidden", "playback_not_authorized",
		"portal_forbidden", "room_membership_required", "room_permission_denied", "invalid_destination",
		"invalid_payload", "invalid_source", "unsupported_control", "destination_mapping_changed":
		return "This action isn't available for this video message."
	}
	return roundMessageUnknown
}

func telegramWaitMessage(waitSeconds int) string {
	switch {
	case waitSeconds <= 0:
		return "Telegram asks you to wait a moment before sending again."
	case waitSeconds == 1:
		return "Telegram asks you to wait 1 second before sending again."
	}
	return "Telegram asks you to wait " + strconv.Itoa(waitSeconds) + " seconds before sending again."
}

// telegramWaitSeconds extracts FLOOD_WAIT_N / SLOWMODE_WAIT_N from an error
// chain.
func telegramWaitSeconds(err error) int {
	if rpc, ok := tgerr.As(err); ok && rpc.Argument > 0 {
		return rpc.Argument
	}
	return 0
}

// humaniseTelegramMessage wraps upstream humanise.Error for message statuses:
// waits get a sentence with the real seconds, upstream "{seconds}" templates
// are filled in, and an unmapped Telegram RPC error gets a sentence instead
// of "rpc error code …" (fork side of BR-15).
func humaniseTelegramMessage(err error) string {
	if tgerr.Is(err, "FLOOD_WAIT", "FLOOD_PREMIUM_WAIT", "SLOWMODE_WAIT") {
		return telegramWaitMessage(telegramWaitSeconds(err))
	}
	text := humanise.Error(err)
	if strings.Contains(text, "{seconds}") {
		if seconds := telegramWaitSeconds(err); seconds > 0 {
			text = strings.ReplaceAll(text, "{seconds}", strconv.Itoa(seconds))
		} else {
			text = strings.ReplaceAll(text, "{seconds} seconds", "a moment")
			text = strings.ReplaceAll(text, "{seconds}", "a moment")
		}
	}
	if _, isRPC := tgerr.As(err); isRPC && text == err.Error() {
		return "Telegram didn't accept this message."
	}
	return text
}

// roundStatusCause is the message status internal error of a fork failure:
// its text is the bare code (no Go error text in internal_error), and it
// unwraps to the original error so classification still sees it.
type roundStatusCause struct {
	code  string
	cause error
}

func (e *roundStatusCause) Error() string { return e.code }
func (e *roundStatusCause) Unwrap() error { return e.cause }

var _ error = (*roundStatusCause)(nil)
