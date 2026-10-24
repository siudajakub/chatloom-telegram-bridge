// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-telegram/pkg/gotd/constant"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram/uploader"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

const (
	roundMessageMarker = "fi.mau.telegram.round_message"
	roundCapabilityKey = "pl.personal.chat.telegram.round_video"
	roundMaxDurationMS = 60_000
	roundMaxDimension  = 640
	// A local resource ceiling, not a Telegram server limit. It is always
	// intersected with the server's current upload limit before any upload.
	roundMaxFileBytes      int64 = 64 * 1024 * 1024
	roundMaxThumbnailBytes int64 = 200 * 1024
)

var roundEncodeSlots = make(chan struct{}, 2)
var roundTransferSlots = make(chan struct{}, 2)

// RoundMediaError describes a pre-dispatch rejection: no sendMedia RPC has
// occurred and retrying as ordinary video/document is never permitted.
type RoundMediaError struct {
	Code string
	Err  error
}

func (e *RoundMediaError) Error() string      { return "round video " + e.Code }
func (e *RoundMediaError) Unwrap() error      { return e.Err }
func roundError(code string, err error) error { return &RoundMediaError{Code: code, Err: err} }

// isRoundMessage deliberately does not infer roundness from filenames,
// dimensions, or truthy values. false means ordinary media; a malformed marker
// is an error even on a non-video event.
func isRoundMessage(content *event.MessageEventContent) (bool, error) {
	if content == nil || content.Info == nil {
		return false, nil
	}
	value, exists := content.Info.Extra[roundMessageMarker]
	if !exists {
		return false, nil
	}
	round, ok := value.(bool)
	if !ok {
		return false, roundError("invalid_marker", nil)
	}
	if !round {
		return false, nil
	}
	if content.MsgType != event.MsgVideo {
		return false, roundError("invalid_message_type", nil)
	}
	if content.Info.MimeType != "video/mp4" || content.Info.MauGIF || content.MSC3245Voice != nil {
		return false, roundError("invalid_profile", nil)
	}
	// A normal media body is its filename. Additional body/HTML is a caption,
	// which Telegram video notes cannot carry.
	if content.FormattedBody != "" || (content.FileName != "" && content.Body != content.FileName) {
		return false, roundError("caption_not_supported", nil)
	}
	return true, nil
}

type roundMediaProfile struct {
	DurationMS int
	Width      int
	Height     int
	Size       int64
	// AudioPassthrough is true when the single audio stream is already AAC-LC
	// mono/stereo at a standard rate and can be stream-copied unchanged.
	AudioPassthrough bool
}
type preparedRoundMedia struct {
	Path          string
	ThumbnailPath string
	Profile       roundMediaProfile
	Timings       roundSendTimings
	directory     string
}

// roundSendTimings records per-phase wall-clock durations of a round send.
// It never carries content, peer, user, event or message identifiers.
type roundSendTimings struct {
	Permission, DownloadDecrypt, HashVerify, Probe, Encode, FinalProbe, Thumbnail, Upload, Send, Verify, Reconcile time.Duration
}

func (t *roundSendTimings) merge(other roundSendTimings) {
	t.Probe += other.Probe
	t.Encode += other.Encode
	t.FinalProbe += other.FinalProbe
	t.Thumbnail += other.Thumbnail
}

func (t *roundSendTimings) addTo(evt *zerolog.Event) *zerolog.Event {
	return evt.
		Int64("permission_ms", t.Permission.Milliseconds()).
		Int64("download_decrypt_ms", t.DownloadDecrypt.Milliseconds()).
		Int64("hash_verify_ms", t.HashVerify.Milliseconds()).
		Int64("probe_ms", t.Probe.Milliseconds()).
		Int64("encode_ms", t.Encode.Milliseconds()).
		Int64("final_probe_ms", t.FinalProbe.Milliseconds()).
		Int64("thumbnail_ms", t.Thumbnail.Milliseconds()).
		Int64("upload_ms", t.Upload.Milliseconds()).
		Int64("send_media_ms", t.Send.Milliseconds()).
		Int64("verify_ms", t.Verify.Milliseconds()).
		Int64("reconcile_ms", t.Reconcile.Milliseconds())
}

func (m *preparedRoundMedia) Close() {
	if m != nil {
		_ = os.RemoveAll(m.directory)
	}
}

type roundProbe struct {
	Streams []struct {
		CodecName        string `json:"codec_name"`
		CodecType        string `json:"codec_type"`
		Profile          string `json:"profile"`
		Channels         int    `json:"channels"`
		SampleRate       string `json:"sample_rate"`
		PixelFormat      string `json:"pix_fmt"`
		Width            int    `json:"width"`
		Height           int    `json:"height"`
		Duration         string `json:"duration"`
		AverageFrameRate string `json:"avg_frame_rate"`
		SideData         []struct {
			Rotation int `json:"rotation"`
		} `json:"side_data_list"`
	} `json:"streams"`
	Format struct {
		Name     string `json:"format_name"`
		Duration string `json:"duration"`
	} `json:"format"`
}

// boundedRoundOutput must not embed bytes.Buffer: exec copies stdout with
// io.Copy, which would use the promoted Buffer.ReadFrom and bypass Write, so
// the limit was never applied before wave 5 (found by the TF-04 test).
type boundedRoundOutput struct {
	buf   bytes.Buffer
	limit int
	// onLimit stops the tool as soon as its output exceeds the limit, so it
	// does not keep running until the preparation deadline (wave 5 TF-04).
	onLimit  func()
	exceeded bool
}

var errRoundToolOutputLimit = errors.New("media tool output limit")

func (b *boundedRoundOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buf.Len() {
		b.exceeded = true
		if b.onLimit != nil {
			b.onLimit()
		}
		return 0, errRoundToolOutputLimit
	}
	return b.buf.Write(p)
}

func (b *boundedRoundOutput) Bytes() []byte { return b.buf.Bytes() }

// runRoundTool neither logs untrusted media metadata nor includes ffmpeg's
// stderr (which can contain filenames/metadata) in user-visible errors.
func runRoundTool(ctx context.Context, binary string, args ...string) ([]byte, error) {
	return runRoundToolLimited(ctx, 1024*1024, binary, args...)
}

func runRoundToolLimited(ctx context.Context, limit int, binary string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin = nil
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	output := &boundedRoundOutput{limit: limit, onLimit: cancel}
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		if output.exceeded {
			return nil, errRoundToolOutputLimit
		}
		return nil, err
	}
	return output.Bytes(), nil
}

func probeRoundMedia(ctx context.Context, path string, maxBytes int64, final bool) (roundMediaProfile, error) {
	var profile roundMediaProfile
	stat, err := os.Stat(path)
	if err != nil {
		return profile, roundError("unreadable_file", err)
	}
	if !stat.Mode().IsRegular() || stat.Size() <= 0 || stat.Size() > maxBytes {
		return profile, roundError("file_size_limit", nil)
	}
	f, err := os.Open(path)
	if err != nil {
		return profile, roundError("unreadable_file", err)
	}
	var signature [12]byte
	_, err = io.ReadFull(f, signature[:])
	_ = f.Close()
	if err != nil || string(signature[4:8]) != "ftyp" {
		return profile, roundError("invalid_container", nil)
	}
	switch string(signature[8:12]) {
	case "isom", "iso2", "iso3", "iso4", "iso5", "iso6", "mp41", "mp42", "avc1", "M4V ":
	default:
		return profile, roundError("invalid_container", nil)
	}
	data, err := runRoundTool(ctx, "ffprobe", "-v", "error", "-protocol_whitelist", "file,pipe", "-f", "mov", "-show_streams", "-show_format", "-of", "json", path)
	if err != nil {
		return profile, roundError("probe_failed", err)
	}
	var probe roundProbe
	if err = json.Unmarshal(data, &probe); err != nil {
		return profile, roundError("probe_failed", err)
	}
	if !strings.Contains(probe.Format.Name, "mp4") {
		return profile, roundError("invalid_container", nil)
	}
	videoCount, audioCount := 0, 0
	for _, stream := range probe.Streams {
		switch stream.CodecType {
		case "video":
			videoCount++
			if stream.CodecName != "h264" || stream.Width != stream.Height || stream.Width < 4 || stream.Width > roundMaxDimension || stream.Width%2 != 0 {
				return profile, roundError("invalid_video_profile", nil)
			}
			profile.Width, profile.Height = stream.Width, stream.Height
			if final && stream.PixelFormat != "yuv420p" {
				return profile, roundError("invalid_pixel_format", nil)
			}
			parts := strings.Split(stream.AverageFrameRate, "/")
			if len(parts) != 2 {
				return profile, roundError("invalid_frame_rate", nil)
			}
			n, ne := strconv.ParseFloat(parts[0], 64)
			d, de := strconv.ParseFloat(parts[1], 64)
			if ne != nil || de != nil || d <= 0 || n <= 0 || n/d > 60 {
				return profile, roundError("invalid_frame_rate", nil)
			}
			if final {
				for _, side := range stream.SideData {
					if side.Rotation != 0 {
						return profile, roundError("invalid_orientation", nil)
					}
				}
			}
		case "audio":
			audioCount++
			if stream.CodecName != "aac" {
				return profile, roundError("invalid_audio_profile", nil)
			}
			profile.AudioPassthrough = stream.Profile == "LC" && (stream.Channels == 1 || stream.Channels == 2) && (stream.SampleRate == "48000" || stream.SampleRate == "44100")
		default:
			return profile, roundError("unsupported_stream", nil)
		}
	}
	if videoCount != 1 || audioCount != 1 {
		return profile, roundError("invalid_stream_count", nil)
	}
	duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
	if err != nil || math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 || duration > 60 {
		return profile, roundError("duration_limit", nil)
	}
	profile.DurationMS = int(math.Ceil(duration * 1000))
	profile.Size = stat.Size()
	return profile, nil
}

// roundMaskFilter sanitizes encoded pixels outside the circle. Applying this
// to all YUV planes ensures the rectangle cannot reveal the original corners.
// geq evaluates its expression per pixel, so it is used only for the single
// thumbnail frame; video frames use the precomputed overlay mask below.
func roundMaskFilter() string {
	return "geq=lum='if(lte((X-W/2)*(X-W/2)+(Y-H/2)*(Y-H/2),(W/2-2)*(W/2-2)),lum(X,Y),16)':cb='if(lte((X-W/2)*(X-W/2)+(Y-H/2)*(Y-H/2),(W/2-1)*(W/2-1)),cb(X,Y),128)':cr='if(lte((X-W/2)*(X-W/2)+(Y-H/2)*(Y-H/2),(W/2-1)*(W/2-1)),cr(X,Y),128)'"
}

// roundMaskOutside is the luma predicate of roundMaskFilter: a pixel is
// sanitized when it lies outside the circle of radius W/2-2 around the centre.
func roundMaskOutside(x, y, width, height int) bool {
	dx := float64(x) - float64(width)/2
	dy := float64(y) - float64(height)/2
	radius := float64(width)/2 - 2
	return dx*dx+dy*dy > radius*radius
}

// roundMaskImage is computed once per send: opaque black outside the circle,
// fully transparent inside. ffmpeg composites it over every frame with the
// SIMD overlay filter instead of evaluating an expression per pixel.
func roundMaskImage(width, height int) *image.NRGBA {
	mask := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if roundMaskOutside(x, y, width, height) {
				mask.SetNRGBA(x, y, color.NRGBA{A: 255})
			}
		}
	}
	return mask
}

func writeRoundMask(path string, width, height int) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = png.Encode(f, roundMaskImage(width, height)); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Two encode slots on a 4 vCPU host: two threads each keep both busy without
// oversubscription.
const roundEncodeThreads = "2"

// roundPrepareTimeout bounds media preparation after the encode slot has been
// acquired. A 60 s 640x640 clip prepares in a few seconds with the overlay
// mask, so this is a safety net rather than an expected limit.
const roundPrepareTimeout = 2 * time.Minute

func roundEncodeArgs(source, mask, output string, maxBytes int64, audioPassthrough bool) []string {
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-threads", roundEncodeThreads, "-protocol_whitelist", "file,pipe", "-f", "mov", "-i", source,
		"-protocol_whitelist", "file", "-f", "png_pipe", "-i", mask,
		"-filter_complex", "[0:v:0]setsar=1,format=yuv420p[v];[v][1:v]overlay=format=yuv420:eof_action=repeat,format=yuv420p[o]",
		"-map", "[o]", "-map", "0:a:0", "-map_metadata", "-1", "-map_chapters", "-1",
		"-c:v", "libx264", "-threads", roundEncodeThreads, "-preset", "veryfast", "-crf", "21", "-maxrate", "1600k", "-bufsize", "3200k",
		// One-second GOP like Telegram's own video notes, independent of fps.
		"-force_key_frames", "expr:gte(t,n_forced)", "-pix_fmt", "yuv420p",
	}
	if audioPassthrough {
		args = append(args, "-c:a", "copy")
	} else {
		args = append(args, "-c:a", "aac", "-ac", "1", "-ar", "48000", "-b:a", "64k")
	}
	return append(args, "-movflags", "+faststart", "-fs", strconv.FormatInt(maxBytes, 10), output)
}

func prepareRoundMedia(ctx context.Context, path string, maxBytes int64) (_ *preparedRoundMedia, err error) {
	if maxBytes <= 0 {
		return nil, roundError("upload_limit_unknown", nil)
	}
	maxBytes = min(maxBytes, roundMaxFileBytes)
	select {
	case roundEncodeSlots <- struct{}{}:
		defer func() { <-roundEncodeSlots }()
	case <-ctx.Done():
		return nil, roundError("encode_timeout", ctx.Err())
	}
	// Queueing for a slot does not consume the preparation budget.
	ctx, cancel := context.WithTimeout(ctx, roundPrepareTimeout)
	defer cancel()
	var timings roundSendTimings
	phase := time.Now()
	source, err := probeRoundMedia(ctx, path, maxBytes, false)
	timings.Probe = time.Since(phase)
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp(roundTempDir(), "telegram-round-*")
	if err != nil {
		return nil, roundError("temporary_storage", err)
	}
	prepared := &preparedRoundMedia{Path: filepath.Join(directory, "round.mp4"), ThumbnailPath: filepath.Join(directory, "thumbnail.jpg"), directory: directory}
	defer func() {
		if err != nil {
			prepared.Close()
		}
	}()
	maskPath := filepath.Join(directory, "mask.png")
	if err = writeRoundMask(maskPath, source.Width, source.Height); err != nil {
		return nil, roundError("temporary_storage", err)
	}
	phase = time.Now()
	_, err = runRoundTool(ctx, "ffmpeg", roundEncodeArgs(path, maskPath, prepared.Path, maxBytes, source.AudioPassthrough)...)
	timings.Encode = time.Since(phase)
	if err != nil {
		if ctx.Err() != nil {
			return nil, roundError("encode_timeout", err)
		}
		return nil, roundError("encode_failed", err)
	}
	phase = time.Now()
	prepared.Profile, err = probeRoundMedia(ctx, prepared.Path, maxBytes, true)
	timings.FinalProbe = time.Since(phase)
	if err != nil {
		return nil, err
	}
	// -fs can stop encoding with a successful exit code. Never send a truncated
	// draft, and never trust the client's claimed duration/dimensions.
	if math.Abs(float64(prepared.Profile.DurationMS-source.DurationMS)) > 100 || prepared.Profile.Width != source.Width {
		return nil, roundError("truncated_encode", nil)
	}
	phase = time.Now()
	_, err = runRoundTool(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-threads", "1", "-filter_threads", "1", "-f", "mov", "-i", prepared.Path, "-map", "0:v:0", "-frames:v", "1", "-vf", "scale=320:320,format=yuv420p,"+roundMaskFilter(), "-c:v", "mjpeg", "-q:v", "3", "-fs", strconv.FormatInt(roundMaxThumbnailBytes, 10), prepared.ThumbnailPath)
	timings.Thumbnail = time.Since(phase)
	if err != nil {
		return nil, roundError("thumbnail_failed", err)
	}
	if err = validateRoundThumbnail(prepared.ThumbnailPath); err != nil {
		return nil, err
	}
	prepared.Timings = timings
	return prepared, nil
}

func validateRoundThumbnail(path string) error {
	stat, err := os.Stat(path)
	if err != nil || stat.Size() <= 0 || stat.Size() > roundMaxThumbnailBytes {
		return roundError("thumbnail_size_limit", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return roundError("thumbnail_failed", err)
	}
	defer f.Close()
	img, err := jpeg.Decode(f)
	if err != nil || img.Bounds().Dx() != 320 || img.Bounds().Dy() != 320 {
		return roundError("invalid_thumbnail", err)
	}
	for _, point := range []image.Point{{0, 0}, {319, 0}, {0, 319}, {319, 319}} {
		r, g, b, _ := img.At(point.X, point.Y).RGBA()
		if max(r, g, b) > 16*257 {
			return roundError("unsanitized_thumbnail", nil)
		}
	}
	return nil
}

func makeRoundUploadedDocument(file, thumbnail tg.InputFileClass, profile roundMediaProfile) *tg.InputMediaUploadedDocument {
	return &tg.InputMediaUploadedDocument{
		File: file, Thumb: thumbnail, MimeType: "video/mp4",
		// Same attribute set as Telegram's own video notes: no filename.
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeVideo{RoundMessage: true, SupportsStreaming: true, Duration: float64(profile.DurationMS) / 1000, W: profile.Width, H: profile.Height},
		},
	}
}

func roundUploadLimitFromConfig(config map[string]any, premium bool) (int64, error) {
	key := "upload_max_fileparts_default"
	if premium {
		key = "upload_max_fileparts_premium"
	}
	parts, ok := config[key].(float64)
	if !ok || math.IsNaN(parts) || math.IsInf(parts, 0) || parts <= 0 || parts != math.Trunc(parts) {
		return 0, roundError("upload_limit_unknown", nil)
	}
	return min(roundMaxFileBytes, int64(min(parts, float64(constant.UploadMaxParts)))*int64(uploader.MaximumPartSize)), nil
}

func (tc *TelegramClient) roundUploadLimit(ctx context.Context) (int64, error) {
	if tc.main.maxFileSize <= 0 {
		return 0, roundError("upload_limit_unknown", nil)
	}
	config, err := tc.getAppConfigCached(ctx)
	if err != nil {
		return 0, roundError("upload_limit_unknown", err)
	}
	limit, err := roundUploadLimitFromConfig(config, tc.isPremiumCache.Load())
	return min(limit, tc.main.maxFileSize), err
}

func (tc *TelegramClient) transferRoundMediaToTelegram(ctx context.Context, content *event.MessageEventContent, timings *roundSendTimings) (tg.InputMediaClass, roundMediaProfile, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	select {
	case roundTransferSlots <- struct{}{}:
		defer func() { <-roundTransferSlots }()
	case <-ctx.Done():
		return nil, roundMediaProfile{}, roundError("transfer_timeout", ctx.Err())
	}
	limit, err := tc.roundUploadLimit(ctx)
	if err != nil {
		return nil, roundMediaProfile{}, err
	}
	if int64(content.Info.Size) > limit {
		return nil, roundMediaProfile{}, roundError("file_size_limit", nil)
	}
	var result tg.InputMediaClass
	var profile roundMediaProfile
	err = tc.downloadRoundMediaToFile(ctx, content, limit, timings, func(f *os.File) error {
		var uploadErr error
		result, profile, uploadErr = prepareAndUploadRoundMediaTimed(ctx, f.Name(), limit, tc.client.API(), timings)
		return uploadErr
	})
	return result, profile, err
}

func prepareAndUploadRoundMedia(ctx context.Context, path string, limit int64, api *tg.Client) (tg.InputMediaClass, error) {
	media, _, err := prepareAndUploadRoundMediaTimed(ctx, path, limit, api, nil)
	return media, err
}

func prepareAndUploadRoundMediaTimed(ctx context.Context, path string, limit int64, api *tg.Client, timings *roundSendTimings) (tg.InputMediaClass, roundMediaProfile, error) {
	if timings == nil {
		timings = &roundSendTimings{}
	}
	prepared, err := prepareRoundMedia(ctx, path, limit)
	if err != nil {
		return nil, roundMediaProfile{}, err
	}
	defer prepared.Close()
	timings.merge(prepared.Timings)
	start := time.Now()
	defer func() { timings.Upload += time.Since(start) }()
	upload := uploader.NewUploader(api).WithThreads(telegramMediaUploadThreads).WithPartSize(uploader.MaximumPartSize)
	file, err := upload.FromPath(ctx, prepared.Path, "round.mp4")
	if err != nil {
		return nil, roundMediaProfile{}, roundUploadError(err)
	}
	thumbnail, err := upload.FromPath(ctx, prepared.ThumbnailPath, "thumbnail.jpg")
	if err != nil {
		return nil, roundMediaProfile{}, roundUploadError(err)
	}
	return makeRoundUploadedDocument(file, thumbnail, prepared.Profile), prepared.Profile, nil
}

// The generic SDK helper downloads an unbounded response before its callback.
// Round media needs a byte ceiling during transfer, including encrypted media.
func (tc *TelegramClient) downloadRoundMediaToFile(ctx context.Context, content *event.MessageEventContent, limit int64, timings *roundSendTimings, callback func(*os.File) error) error {
	bot, ok := tc.main.Bridge.Bot.(*matrix.ASIntent)
	if !ok || bot.Matrix == nil {
		return roundError("media_download_unavailable", nil)
	}
	uri := content.URL
	var encrypted *event.EncryptedFileInfo
	if content.File != nil {
		copy := *content.File
		encrypted = &copy
		uri = encrypted.URL
		if err := encrypted.PrepareForDecryption(); err != nil {
			return roundError("media_decrypt_failed", err)
		}
	}
	parsed, err := uri.Parse()
	if err != nil {
		return roundError("invalid_media_url", err)
	}
	start := time.Now()
	response, err := bot.Matrix.Download(ctx, parsed)
	if err != nil {
		return roundError("media_download_failed", err)
	}
	defer response.Body.Close()
	if response.ContentLength > limit {
		return roundError("file_size_limit", nil)
	}
	reader := response.Body
	if encrypted != nil {
		reader = encrypted.DecryptStream(reader)
	}
	defer reader.Close()
	return copyRoundDownloadToFileTimed(reader, limit, start, timings, callback)
}

func copyRoundDownloadToFile(reader io.ReadCloser, limit int64, callback func(*os.File) error) error {
	return copyRoundDownloadToFileTimed(reader, limit, time.Now(), nil, callback)
}

func copyRoundDownloadToFileTimed(reader io.ReadCloser, limit int64, start time.Time, timings *roundSendTimings, callback func(*os.File) error) error {
	if timings == nil {
		timings = &roundSendTimings{}
	}
	if limit <= 0 {
		return roundError("upload_limit_unknown", nil)
	}
	f, err := os.CreateTemp(roundTempDir(), "telegram-round-download-*")
	if err != nil {
		return roundError("temporary_storage", err)
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	// Decryption is streamed, so this phase covers transfer and decryption.
	count, err := io.Copy(f, io.LimitReader(reader, limit+1))
	timings.DownloadDecrypt = time.Since(start)
	if err != nil {
		return roundError("media_download_failed", err)
	}
	if count <= 0 || count > limit {
		return roundError("file_size_limit", nil)
	}
	// Encrypted attachment hashes are checked at Close. Verify before exposing
	// plaintext to ffmpeg or executing either Telegram upload RPC.
	verifyStart := time.Now()
	err = reader.Close()
	timings.HashVerify = time.Since(verifyStart)
	if err != nil {
		return roundError("media_decrypt_failed", err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return roundError("media_download_failed", err)
	}
	return callback(f)
}

// roundUploadError keeps a Telegram RPC error (FLOOD_WAIT, FILE_PARTS_INVALID)
// as such, so it is classified and humanised as a Telegram answer; any other
// upload failure is a retriable "upload_failed" (wave 5 F4).
func roundUploadError(err error) error {
	if _, ok := tgerr.As(err); ok {
		return fmt.Errorf("round upload: %w", err)
	}
	return roundError("upload_failed", err)
}

// roundTempDirEnv points round preparation at a directory on the bridge's data
// volume instead of the system temporary directory (wave 5 F10).
const roundTempDirEnv = "TELEGRAM_ROUND_TMPDIR"

// roundTempDirectory is set once at connector Init; "" means os.TempDir().
var roundTempDirectory string

func roundTempDirFromEnv() string {
	dir := strings.TrimSpace(os.Getenv(roundTempDirEnv))
	if dir == "" {
		return ""
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return ""
	}
	roundTempDirectory = dir
	return dir
}

func roundTempDir() string {
	return roundTempDirectory
}

// sweepRoundTempFiles removes telegram-round-* leftovers older than maxAge, as
// left by a crash or kill during preparation. Live sends finish within the
// 5 minute transfer bound, so an hour is safely past any of them. Only our own
// prefix is touched; symlinks are removed, never followed.
func sweepRoundTempFiles(dir string, maxAge time.Duration, now time.Time) (removed int) {
	if dir == "" {
		dir = os.TempDir()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "telegram-round-") {
			continue
		}
		info, err := entry.Info()
		if err != nil || now.Sub(info.ModTime()) < maxAge {
			continue
		}
		if os.RemoveAll(filepath.Join(dir, entry.Name())) == nil {
			removed++
		}
	}
	return removed
}
