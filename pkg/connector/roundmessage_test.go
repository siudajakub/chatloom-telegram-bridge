// SPDX-License-Identifier: AGPL-3.0-or-later
package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

func syntheticRoundContent(marker any) *event.MessageEventContent {
	return &event.MessageEventContent{MsgType: event.MsgVideo, Body: "round.mp4", FileName: "round.mp4", Info: &event.FileInfo{MimeType: "video/mp4", Duration: 1000, Width: 640, Height: 640, Extra: map[string]any{roundMessageMarker: marker}}}
}

func TestRoundStrictCanonicalMarker(t *testing.T) {
	for _, test := range []struct {
		name     string
		marker   any
		expected bool
		code     string
	}{
		{"true", true, true, ""}, {"false", false, false, ""}, {"string", "true", false, "invalid_marker"}, {"number", float64(1), false, "invalid_marker"}, {"null", nil, false, "invalid_marker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := isRoundMessage(syntheticRoundContent(test.marker))
			if got != test.expected {
				t.Fatalf("round=%v", got)
			}
			assertRoundError(t, err, test.code)
		})
	}
	content := syntheticRoundContent(true)
	delete(content.Info.Extra, roundMessageMarker)
	if got, err := isRoundMessage(content); got || err != nil {
		t.Fatal("ordinary square video inferred as round")
	}
	// A nested info.extra must not be accepted as the flattened marker.
	content.Info.Extra["extra"] = map[string]any{roundMessageMarker: true}
	if got, err := isRoundMessage(content); got || err != nil {
		t.Fatal("nested marker accepted")
	}
}

func assertRoundError(t *testing.T, err error, code string) {
	t.Helper()
	if code == "" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var typed *RoundMediaError
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestRoundRejectMessageAmbiguity(t *testing.T) {
	for _, test := range []struct {
		name, code string
		change     func(*event.MessageEventContent)
	}{
		{"image", "invalid_message_type", func(c *event.MessageEventContent) { c.MsgType = event.MsgImage }},
		{"mime", "invalid_profile", func(c *event.MessageEventContent) { c.Info.MimeType = "video/quicktime" }},
		{"animated", "invalid_profile", func(c *event.MessageEventContent) { c.Info.MauGIF = true }},
		{"caption", "caption_not_supported", func(c *event.MessageEventContent) { c.Body = "caption" }},
		{"html", "caption_not_supported", func(c *event.MessageEventContent) { c.FormattedBody = "caption" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := syntheticRoundContent(true)
			test.change(c)
			_, err := isRoundMessage(c)
			assertRoundError(t, err, test.code)
		})
	}
	// Force-document must reject before touching Matrix/client dependencies.
	_, err := (&TelegramClient{}).transferMediaToTelegram(context.Background(), syntheticRoundContent(true), false, false, true)
	assertRoundError(t, err, "document_fallback_forbidden")
}

type roundRPCStub struct {
	testing *testing.T
	calls   int
	request *tg.MessagesSendMediaRequest
	uploads map[int64][]byte
}

func (stub *roundRPCStub) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	stub.testing.Helper()
	stub.calls++
	if part, ok := input.(*tg.UploadSaveFilePartRequest); ok {
		if part.FilePart != 0 {
			return errors.New("synthetic round fixture exceeded one part")
		}
		if stub.uploads == nil {
			stub.uploads = make(map[int64][]byte)
		}
		stub.uploads[part.FileID] = append([]byte(nil), part.Bytes...)
		response := &bin.Buffer{}
		if err := (&tg.BoolTrue{}).Encode(response); err != nil {
			return err
		}
		return output.Decode(response)
	}
	wire := &bin.Buffer{}
	if err := input.Encode(wire); err != nil {
		return err
	}
	decoded := &tg.MessagesSendMediaRequest{}
	if err := decoded.Decode(wire); err != nil {
		return err
	}
	stub.request = decoded
	response := &bin.Buffer{}
	if err := (&tg.Updates{Updates: []tg.UpdateClass{}, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}}).Encode(response); err != nil {
		return err
	}
	return output.Decode(response)
}

func TestRoundRealTLRequest(t *testing.T) {
	stub := &roundRPCStub{testing: t}
	client := tg.NewClient(stub)
	media := makeRoundUploadedDocument(&tg.InputFile{ID: 1, Parts: 1, Name: "round.mp4", MD5Checksum: "synthetic"}, &tg.InputFile{ID: 2, Parts: 1, Name: "thumbnail.jpg", MD5Checksum: "synthetic"}, roundMediaProfile{DurationMS: 1234, Width: 320, Height: 320, Size: 5000})
	_, err := client.MessagesSendMedia(context.Background(), &tg.MessagesSendMediaRequest{Peer: &tg.InputPeerSelf{}, Media: media, RandomID: 12345, ReplyTo: &tg.InputReplyToMessage{ReplyToMsgID: 9, TopMsgID: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if stub.calls != 1 || stub.request.RandomID != 12345 || stub.request.Message != "" || len(stub.request.Entities) != 0 || stub.request.AllowPaidStars != 0 || stub.request.AllowPaidFloodskip {
		t.Fatal("send altered identity/caption/paid policy")
	}
	doc, ok := stub.request.Media.(*tg.InputMediaUploadedDocument)
	if !ok || doc.MimeType != "video/mp4" || doc.Thumb == nil || doc.ForceFile || doc.TTLSeconds != 0 {
		t.Fatal("round doc downgraded or lost thumbnail")
	}
	// Same attribute set as Telegram's own video notes: only the video
	// attribute, no filename/animated/audio attributes.
	if len(doc.Attributes) != 1 {
		t.Fatal("unexpected filename/animated/audio/caption attrs")
	}
	video, ok := doc.Attributes[0].(*tg.DocumentAttributeVideo)
	if !ok || !video.RoundMessage || video.Duration != 1.234 || video.W != 320 || video.H != 320 || !video.SupportsStreaming {
		t.Fatalf("invalid TL video attribute: %#v", video)
	}
	reply := stub.request.ReplyTo.(*tg.InputReplyToMessage)
	if reply.ReplyToMsgID != 9 || reply.TopMsgID != 4 {
		t.Fatal("reply/topic dropped")
	}
}

func TestRoundUploadLimits(t *testing.T) {
	for _, config := range []map[string]any{nil, {}, {"upload_max_fileparts_default": "4000"}, {"upload_max_fileparts_default": float64(-1)}, {"upload_max_fileparts_default": float64(1.5)}} {
		_, err := roundUploadLimitFromConfig(config, false)
		assertRoundError(t, err, "upload_limit_unknown")
	}
	limit, err := roundUploadLimitFromConfig(map[string]any{"upload_max_fileparts_default": float64(1)}, false)
	if err != nil || limit != 524288 {
		t.Fatalf("runtime limit: %d %v", limit, err)
	}
	limit, err = roundUploadLimitFromConfig(map[string]any{"upload_max_fileparts_default": float64(4000), "upload_max_fileparts_premium": float64(8000)}, true)
	if err != nil || limit != roundMaxFileBytes {
		t.Fatalf("local ceiling: %d %v", limit, err)
	}
}

func TestRoundTypedSendCertainty(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		certain bool
		status  event.MessageStatus
		reason  event.MessageStatusReason
	}{
		{"uncertain", &RoundOperationError{Code: "transport_unavailable", Certainty: "uncertain", Retriable: true}, false, event.MessageStatusRetriable, event.MessageStatusNetworkError},
		{"known retry", &RoundOperationError{Code: "FLOOD_WAIT", Certainty: "certain", Retriable: true}, true, event.MessageStatusRetriable, event.MessageStatusNetworkError},
		{"known permission", &RoundOperationError{Code: "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN", Certainty: "certain"}, true, event.MessageStatusFail, event.MessageStatusNoPermission},
		{"known invalid media", roundError("invalid_video_profile", nil), true, event.MessageStatusFail, event.MessageStatusUnsupported},
		{"unknown limit", roundError("upload_limit_unknown", nil), true, event.MessageStatusRetriable, event.MessageStatusUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := bridgev2.WrapErrorInStatus((&TelegramClient{}).humaniseSendError(test.err))
			if status.IsCertain != test.certain || status.Status != test.status || status.ErrorReason != test.reason {
				t.Fatalf("wrong certainty/status/reason: %+v", status)
			}
		})
	}
}

func syntheticRoundFile(t *testing.T, dimension int, duration, videoCodec, audioCodec string) string {
	t.Helper()
	if !roundToolsAvailable() {
		t.Skip("ffmpeg/ffprobe unavailable")
	}
	path := filepath.Join(t.TempDir(), "synthetic.mp4")
	_, err := runRoundTool(context.Background(), "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=red:s="+fmtDimension(dimension)+":r=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", duration, "-c:v", videoCodec, "-threads", "1", "-pix_fmt", "yuv420p", "-c:a", audioCodec, "-ac", "1", "-movflags", "+faststart", path)
	if err != nil {
		t.Fatal(err)
	}
	return path
}
func fmtDimension(dimension int) string { return fmt.Sprintf("%dx%d", dimension, dimension) }

func TestRoundActualBytesNormalization(t *testing.T) {
	source := syntheticRoundFile(t, 128, "1", "libx264", "aac")
	prepared, err := prepareRoundMedia(context.Background(), source, roundMaxFileBytes)
	if err != nil {
		t.Fatal(err)
	}
	directory := prepared.directory
	defer prepared.Close()
	if prepared.Profile.Width != 128 || prepared.Profile.Height != 128 || prepared.Profile.DurationMS < 950 || prepared.Profile.DurationMS > 1100 || prepared.Profile.Size <= 0 {
		t.Fatalf("bad real profile: %+v", prepared.Profile)
	}
	if err = validateRoundThumbnail(prepared.ThumbnailPath); err != nil {
		t.Fatal(err)
	}
	// Decode both the first and later encoded frame. A display-only circle or
	// thumbnail-only mask would leave red corners on the later frame.
	for _, timestamp := range []string{"0", "0.7"} {
		frame := filepath.Join(t.TempDir(), "frame.png")
		_, err = runRoundTool(context.Background(), "ffmpeg", "-v", "error", "-nostdin", "-y", "-ss", timestamp, "-i", prepared.Path, "-frames:v", "1", frame)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(frame)
		if err != nil {
			t.Fatal(err)
		}
		img, err := png.Decode(f)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		assertMaskedFrame(t, img)
	}
	// Drive the real upload and TL client through an MTProto stub. Check the
	// uploaded bytes are exactly the verified output and generated thumbnail.
	stub := &roundRPCStub{testing: t}
	api := tg.NewClient(stub)
	media, err := prepareAndUploadRoundMedia(context.Background(), source, roundMaxFileBytes, api)
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.MessagesSendMedia(context.Background(), &tg.MessagesSendMediaRequest{Peer: &tg.InputPeerSelf{}, RandomID: 12345, Media: media})
	if err != nil {
		t.Fatal(err)
	}
	if stub.calls != 3 || len(stub.uploads) != 2 {
		t.Fatal("expected exact two media uploads and one send")
	}
	doc := stub.request.Media.(*tg.InputMediaUploadedDocument)
	for _, upload := range []struct {
		file tg.InputFileClass
		path string
	}{{doc.File, prepared.Path}, {doc.Thumb, prepared.ThumbnailPath}} {
		input, ok := upload.file.(*tg.InputFile)
		if !ok {
			t.Fatal("unexpected fixture upload type")
		}
		actual, err := os.ReadFile(upload.path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(actual, stub.uploads[input.ID]) {
			t.Fatal("MTProto upload differs from verified media bytes")
		}
	}
	video := doc.Attributes[0].(*tg.DocumentAttributeVideo)
	if !video.RoundMessage || video.Duration != float64(prepared.Profile.DurationMS)/1000 || video.W != 128 || video.H != 128 || doc.Thumb == nil || doc.ForceFile {
		t.Fatal("verified bytes lost round profile at real TL dispatch")
	}
	prepared.Close()
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatal("normalized private temp files were not removed")
	}
}

func assertMaskedFrame(t *testing.T, img image.Image) {
	t.Helper()
	bounds := img.Bounds()
	for _, point := range []image.Point{{0, 0}, {bounds.Max.X - 1, 0}, {0, bounds.Max.Y - 1}, {bounds.Max.X - 1, bounds.Max.Y - 1}} {
		pixel := color.RGBAModel.Convert(img.At(point.X, point.Y)).(color.RGBA)
		if max(pixel.R, pixel.G, pixel.B) > 16 {
			t.Fatalf("unsanitized corner: %+v", pixel)
		}
	}
	center := color.RGBAModel.Convert(img.At(bounds.Dx()/2, bounds.Dy()/2)).(color.RGBA)
	if center.R < 150 || center.G > 40 || center.B > 40 {
		t.Fatalf("center content corrupted: %+v", center)
	}
}

func TestRoundBadBytesNeverDispatch(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "invalid.mp4")
	if err := os.WriteFile(bad, []byte("not an MP4 despite Matrix metadata"), 0600); err != nil {
		t.Fatal(err)
	}
	stub := &roundRPCStub{testing: t}
	prepared, err := prepareAndUploadRoundMedia(context.Background(), bad, roundMaxFileBytes, tg.NewClient(stub))
	assertRoundError(t, err, "invalid_container")
	if prepared != nil || stub.calls != 0 {
		t.Fatal("invalid bytes reached RPC")
	}
	_, err = prepareRoundMedia(context.Background(), bad, 4)
	assertRoundError(t, err, "file_size_limit")
	_, err = prepareRoundMedia(context.Background(), bad, 0)
	assertRoundError(t, err, "upload_limit_unknown")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = prepareRoundMedia(ctx, bad, roundMaxFileBytes)
	if err == nil {
		t.Fatal("canceled context accepted")
	}
}

type roundCloseFaultReader struct {
	io.Reader
	closeError error
}

func (r roundCloseFaultReader) Close() error { return r.closeError }

func TestRoundBoundedDownloadBeforeUpload(t *testing.T) {
	for _, test := range []struct {
		name   string
		reader io.ReadCloser
		limit  int64
		code   string
	}{
		{"overflow", io.NopCloser(bytes.NewReader(make([]byte, 17))), 16, "file_size_limit"},
		{"empty", io.NopCloser(bytes.NewReader(nil)), 16, "file_size_limit"},
		{"unknown", io.NopCloser(bytes.NewReader([]byte("synthetic"))), 0, "upload_limit_unknown"},
		{"encrypted-hash-failed", roundCloseFaultReader{bytes.NewReader([]byte("synthetic")), errors.New("synthetic hash mismatch")}, 16, "media_decrypt_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			err := copyRoundDownloadToFile(test.reader, test.limit, func(*os.File) error { calls++; return nil })
			assertRoundError(t, err, test.code)
			if calls != 0 {
				t.Fatal("rejected download reached validator/upload")
			}
		})
	}
	var temporary string
	err := copyRoundDownloadToFile(io.NopCloser(bytes.NewReader([]byte("synthetic"))), 16, func(file *os.File) error {
		temporary = file.Name()
		stat, err := file.Stat()
		if err != nil {
			return err
		}
		if stat.Mode().Perm() != 0600 {
			t.Fatal("private plaintext temp permissions")
		}
		data, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		if string(data) != "synthetic" {
			t.Fatal("download position/hash gate")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temporary); !os.IsNotExist(err) {
		t.Fatal("private download temp not removed")
	}
}

func TestRoundProbeRejectsUnsupportedProfiles(t *testing.T) {
	for _, test := range []struct {
		name                         string
		dimension                    int
		duration, video, audio, code string
	}{
		{"oversize", 642, "1", "libx264", "aac", "invalid_video_profile"},
		{"duration", 128, "60.1", "libx264", "aac", "duration_limit"},
		{"video-codec", 128, "1", "mpeg4", "aac", "invalid_video_profile"},
		{"audio-codec", 128, "1", "libx264", "mp3", "invalid_audio_profile"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := syntheticRoundFile(t, test.dimension, test.duration, test.video, test.audio)
			_, err := prepareRoundMedia(context.Background(), source, roundMaxFileBytes)
			assertRoundError(t, err, test.code)
		})
	}
}

// syntheticAndroidRoundFile matches the Android recorder's output profile:
// 384x384 H.264 ~1 Mbps 30 fps 1 s GOP, AAC-LC mono 48 kHz 64 kbps, faststart.
func syntheticAndroidRoundFile(t *testing.T, seconds string) string {
	t.Helper()
	if !roundToolsAvailable() {
		t.Skip("ffmpeg/ffprobe unavailable")
	}
	path := filepath.Join(t.TempDir(), "android-round.mp4")
	_, err := runRoundTool(context.Background(), "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-f", "lavfi", "-i", "testsrc2=s=384x384:r=30", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
		"-t", seconds, "-c:v", "libx264", "-preset", "veryfast", "-b:v", "1000k", "-g", "30", "-pix_fmt", "yuv420p",
		"-c:a", "aac", "-ac", "1", "-ar", "48000", "-b:a", "64k", "-movflags", "+faststart", path)
	require.NoError(t, err)
	return path
}

func roundStreamMD5(t *testing.T, path, stream string) string {
	t.Helper()
	out, err := runRoundTool(context.Background(), "ffmpeg", "-v", "error", "-nostdin", "-i", path, "-map", stream, "-c", "copy", "-f", "md5", "-")
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// TestRoundPrepareTimeBudget catches a return to per-pixel masking: the old
// geq filter needed ~3 s of CPU per second of 640x640 video, so a 60 s clip
// exceeded the 2 minute preparation timeout.
func TestRoundPrepareTimeBudget(t *testing.T) {
	for _, test := range []struct {
		seconds    string
		durationMS int
		budget     time.Duration
		long       bool
	}{
		{"10", 10_000, 6 * time.Second, false},
		{"60", 60_000, 30 * time.Second, true},
	} {
		t.Run(test.seconds+"s", func(t *testing.T) {
			if test.long && testing.Short() {
				t.Skip("long fixture skipped in -short mode")
			}
			source := syntheticAndroidRoundFile(t, test.seconds)
			start := time.Now()
			prepared, err := prepareRoundMedia(context.Background(), source, roundMaxFileBytes)
			elapsed := time.Since(start)
			require.NoError(t, err)
			defer prepared.Close()
			t.Logf("prepare %ss 384x384@30: total=%s probe=%s encode=%s final_probe=%s thumbnail=%s size=%d", test.seconds, elapsed.Round(time.Millisecond), prepared.Timings.Probe.Round(time.Millisecond), prepared.Timings.Encode.Round(time.Millisecond), prepared.Timings.FinalProbe.Round(time.Millisecond), prepared.Timings.Thumbnail.Round(time.Millisecond), prepared.Profile.Size)
			require.Less(t, elapsed, test.budget)
			require.Less(t, elapsed, roundPrepareTimeout/4)
			// Input dimensions are kept: 384 is not upscaled to 640.
			require.Equal(t, 384, prepared.Profile.Width)
			require.Equal(t, 384, prepared.Profile.Height)
			require.InDelta(t, test.durationMS, prepared.Profile.DurationMS, 100)
			// Already compliant AAC-LC mono audio is passed through unchanged.
			require.True(t, prepared.Profile.AudioPassthrough)
			require.Equal(t, roundStreamMD5(t, source, "0:a:0"), roundStreamMD5(t, prepared.Path, "0:a:0"))
			require.NoError(t, validateRoundThumbnail(prepared.ThumbnailPath))
		})
	}
}

func TestRoundEncodeArgsReencodeNonCompliantAudio(t *testing.T) {
	args := strings.Join(roundEncodeArgs("in.mp4", "mask.png", "out.mp4", 1000, false), " ")
	require.Contains(t, args, "-c:a aac -ac 1 -ar 48000 -b:a 64k")
	require.NotContains(t, args, "geq")
	require.Contains(t, args, "overlay=format=yuv420")
	require.Contains(t, args, "-movflags +faststart")
	require.Contains(t, args, "-pix_fmt yuv420p")
	require.NotContains(t, args, "scale")
	require.Contains(t, strings.Join(roundEncodeArgs("in.mp4", "mask.png", "out.mp4", 1000, true), " "), "-c:a copy")
}

func decodeRoundFrame(t *testing.T, args ...string) *image.Gray {
	t.Helper()
	frame := filepath.Join(t.TempDir(), "frame.png")
	_, err := runRoundTool(context.Background(), "ffmpeg", append(append([]string{"-v", "error", "-nostdin", "-y"}, args...), "-frames:v", "1", frame)...)
	require.NoError(t, err)
	f, err := os.Open(frame)
	require.NoError(t, err)
	defer f.Close()
	img, err := png.Decode(f)
	require.NoError(t, err)
	gray := image.NewGray(img.Bounds())
	for y := img.Bounds().Min.Y; y < img.Bounds().Max.Y; y++ {
		for x := img.Bounds().Min.X; x < img.Bounds().Max.X; x++ {
			gray.Set(x, y, img.At(x, y))
		}
	}
	return gray
}

// TestRoundOverlayMaskMatchesGeqMask checks the precomputed overlay mask is
// equivalent to the previous per-pixel geq mask: corners sanitised, centre
// preserved, same circle.
func TestRoundOverlayMaskMatchesGeqMask(t *testing.T) {
	for _, size := range []int{128, 384, 640} {
		mask := roundMaskImage(size, size)
		for y := 0; y < size; y++ {
			for x := 0; x < size; x++ {
				dx, dy, r := float64(x)-float64(size)/2, float64(y)-float64(size)/2, float64(size)/2-2
				outside := dx*dx+dy*dy > r*r
				if (mask.NRGBAAt(x, y).A == 255) != outside || (mask.NRGBAAt(x, y).A != 0 && mask.NRGBAAt(x, y).A != 255) {
					t.Fatalf("mask %d differs from geq predicate at %d,%d", size, x, y)
				}
			}
		}
	}

	source := syntheticAndroidRoundFile(t, "2")
	prepared, err := prepareRoundMedia(context.Background(), source, roundMaxFileBytes)
	require.NoError(t, err)
	defer prepared.Close()
	for _, timestamp := range []string{"0", "1.5"} {
		original := decodeRoundFrame(t, "-ss", timestamp, "-i", source)
		output := decodeRoundFrame(t, "-ss", timestamp, "-i", prepared.Path)
		reference := decodeRoundFrame(t, "-ss", timestamp, "-i", source, "-vf", "setsar=1,format=yuv420p,"+roundMaskFilter())
		size := output.Bounds().Dx()
		require.Equal(t, 384, size)
		r := float64(size)/2 - 2
		var insideDiff, referenceDiff float64
		var inside, mismatched int
		for y := 0; y < size; y++ {
			for x := 0; x < size; x++ {
				dx, dy := float64(x)-float64(size)/2, float64(y)-float64(size)/2
				distance := math.Sqrt(dx*dx + dy*dy)
				got, orig, ref := float64(output.GrayAt(x, y).Y), float64(original.GrayAt(x, y).Y), float64(reference.GrayAt(x, y).Y)
				switch {
				case distance > r+3:
					// Sanitised in both masks; encoding noise stays below 16.
					if got > 16 || ref > 16 {
						mismatched++
					}
				case distance < r-3:
					inside++
					insideDiff += math.Abs(got - orig)
					referenceDiff += math.Abs(got - ref)
				}
			}
		}
		require.Zero(t, mismatched, "pixels outside the circle are not sanitised")
		require.Less(t, insideDiff/float64(inside), 4.0, "centre content not preserved")
		require.Less(t, referenceDiff/float64(inside), 4.0, "overlay differs from geq reference")
		for _, corner := range []image.Point{{0, 0}, {size - 1, 0}, {0, size - 1}, {size - 1, size - 1}} {
			require.LessOrEqual(t, output.GrayAt(corner.X, corner.Y).Y, uint8(16))
		}
	}
}
