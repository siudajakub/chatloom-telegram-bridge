// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// gifRPC is a synthetic Telegram for @gif: contacts.resolveUsername and
// messages.getInlineBotResults. It records what was asked (never a real
// account or query).
type gifRPC struct {
	mu        sync.Mutex
	resolves  atomic.Int64
	searches  atomic.Int64
	queries   []string
	offsets   []string
	peers     []tg.InputPeerClass
	results   int
	cacheTime int
	fail      error
	resolveTo bool // resolve to a non-bot user
}

func gifDocument(docID int64, size int64) *tg.Document {
	return &tg.Document{
		ID:            docID,
		AccessHash:    docID * 7,
		FileReference: []byte{1, 2, 3},
		MimeType:      "video/mp4",
		Size:          size,
		Thumbs: []tg.PhotoSizeClass{
			// A minimal stripped thumbnail (version 1, 8×8) and a real size.
			&tg.PhotoStrippedSize{Type: "i", Bytes: []byte{1, 8, 8, 0xAA, 0xBB}},
			&tg.PhotoSize{Type: "m", W: 320, H: 180, Size: 5000},
		},
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeAnimated{},
			&tg.DocumentAttributeVideo{W: 320, H: 180, Duration: 2.5},
		},
	}
}

func (g *gifRPC) answer() *tg.MessagesBotResults {
	results := []tg.BotInlineResultClass{
		// A web result (third-party URL) and a photo are skipped.
		&tg.BotInlineResult{ID: "web", Type: "gif", SendMessage: &tg.BotInlineMessageMediaAuto{}},
		&tg.BotInlineMediaResult{ID: "photo", Type: "photo", Photo: &tg.Photo{ID: 9}, SendMessage: &tg.BotInlineMessageMediaAuto{}},
		// A document that is too large is skipped.
		&tg.BotInlineMediaResult{ID: "big", Type: "gif", Document: gifDocument(999, gifMaxBytes+1), SendMessage: &tg.BotInlineMessageMediaAuto{}},
	}
	for i := 0; i < g.results; i++ {
		results = append(results, &tg.BotInlineMediaResult{
			ID:          "r",
			Type:        "gif",
			Document:    gifDocument(int64(100+i), 200_000),
			SendMessage: &tg.BotInlineMessageMediaAuto{},
		})
	}
	return &tg.MessagesBotResults{QueryID: 42, NextOffset: "next", Results: results, CacheTime: g.cacheTime}
}

func (g *gifRPC) client() *telegram.Client {
	return telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{
		telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc {
			return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				var result bin.Encoder
				switch req := input.(type) {
				case *tg.ContactsResolveUsernameRequest:
					g.resolves.Add(1)
					if req.Username != gifBotUsername {
						return tgerr.New(400, "UNEXPECTED_USERNAME")
					}
					result = &tg.ContactsResolvedPeer{
						Peer:  &tg.PeerUser{UserID: 777},
						Chats: []tg.ChatClass{},
						Users: []tg.UserClass{&tg.User{ID: 777, AccessHash: 99, Bot: !g.resolveTo, Username: "gif"}},
					}
				case *tg.MessagesGetInlineBotResultsRequest:
					g.searches.Add(1)
					g.mu.Lock()
					g.queries = append(g.queries, req.Query)
					g.offsets = append(g.offsets, req.Offset)
					g.peers = append(g.peers, req.Peer)
					g.mu.Unlock()
					if bot, ok := req.Bot.(*tg.InputUser); !ok || bot.UserID != 777 || bot.AccessHash != 99 {
						return tgerr.New(400, "BOT_INVALID")
					}
					if g.fail != nil {
						return g.fail
					}
					result = g.answer()
				default:
					return tgerr.New(400, "UNEXPECTED_SYNTHETIC_RPC")
				}
				encoded := &bin.Buffer{}
				if err := result.Encode(encoded); err != nil {
					return err
				}
				return output.Decode(encoded)
			}
		}),
	}})
}

type gifClock struct{ now time.Time }

func (c *gifClock) get() time.Time { return c.now }

func gifTestClient(rpc *gifRPC) (*TelegramClient, *gifClock) {
	tc := &TelegramClient{main: &TelegramConnector{maxFileSize: 50 * 1024 * 1024}, client: rpc.client()}
	clock := &gifClock{now: time.Unix(1_800_000_000, 0)}
	tc.gifState().now = clock.get
	return tc, clock
}

func TestGIFSearchResolvesBotOnceAndCachesPerQuery(t *testing.T) {
	rpc := &gifRPC{results: 3}
	tc, clock := gifTestClient(rpc)
	ctx := context.Background()

	resp, err := tc.searchGIFs(ctx, "Synthetic Cats", "")
	require.NoError(t, err)
	require.Len(t, resp.Results, 3, "web, photo and oversized results are skipped")
	require.Equal(t, "next", resp.NextOffset)
	first := resp.Results[0]
	require.Equal(t, "100", first.ID)
	require.Equal(t, "video/mp4", first.MimeType)
	require.Equal(t, 320, first.Width)
	require.Equal(t, 180, first.Height)
	require.Equal(t, 2500, first.DurationMS)
	require.EqualValues(t, 200_000, first.Size)
	require.True(t, bytes.HasPrefix(first.Thumb, []byte{0xFF, 0xD8}), "the stripped thumbnail is expanded to a JPEG")

	// The chat stays private: the bot sees inputPeerSelf.
	require.IsType(t, &tg.InputPeerSelf{}, rpc.peers[0])
	require.Equal(t, "Synthetic Cats", rpc.queries[0], "the query keeps its case")

	// Same query (case and spacing differ): served from the cache.
	_, err = tc.searchGIFs(ctx, "  synthetic   cats ", "")
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.searches.Load())

	// Another page and another query ask Telegram; @gif is resolved once.
	_, err = tc.searchGIFs(ctx, "synthetic cats", "next")
	require.NoError(t, err)
	_, err = tc.searchGIFs(ctx, "synthetic dogs", "")
	require.NoError(t, err)
	require.EqualValues(t, 3, rpc.searches.Load())
	require.EqualValues(t, 1, rpc.resolves.Load())
	require.Equal(t, "next", rpc.offsets[1])

	// The cache entry expires.
	clock.now = clock.now.Add(gifSearchCacheTTL + time.Second)
	_, err = tc.searchGIFs(ctx, "synthetic cats", "")
	require.NoError(t, err)
	require.EqualValues(t, 4, rpc.searches.Load())
}

func TestGIFSearchHonoursTelegramCacheTime(t *testing.T) {
	rpc := &gifRPC{results: 1, cacheTime: 30}
	tc, clock := gifTestClient(rpc)
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	clock.now = clock.now.Add(31 * time.Second)
	_, err = tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, rpc.searches.Load())
}

func TestGIFSearchResultsAndCachesAreBounded(t *testing.T) {
	rpc := &gifRPC{results: gifMaxResults + 20}
	tc, _ := gifTestClient(rpc)
	resp, err := tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	require.Len(t, resp.Results, gifMaxResults)

	s := tc.gifState()
	for i := 0; i < gifSearchCacheSize+10; i++ {
		_, err = tc.searchGIFs(context.Background(), "synthetic "+strings.Repeat("x", i+1), "")
		require.NoError(t, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, gifSearchCacheSize, s.searches.len())
	require.LessOrEqual(t, s.docs.len(), gifDocCacheSize)
}

func TestBoundedCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := newBoundedCache[int, string](2)
	c.put(1, "a")
	c.put(2, "b")
	_, ok := c.get(1)
	require.True(t, ok)
	c.put(3, "c")
	_, ok = c.get(2)
	require.False(t, ok, "2 was the least recently used")
	_, ok = c.get(1)
	require.True(t, ok)
	require.Equal(t, 2, c.len())
	c.remove(1)
	require.Equal(t, 1, c.len())
}

func TestGIFSearchFloodWaitBacksOffWithoutRPC(t *testing.T) {
	rpc := &gifRPC{results: 1, fail: tgerr.New(420, "FLOOD_WAIT_7")}
	tc, clock := gifTestClient(rpc)
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	require.EqualValues(t, 7000, respErrorOf(t, err).ExtraData["retry_after_ms"])
	require.EqualValues(t, 1, rpc.searches.Load())

	clock.now = clock.now.Add(3 * time.Second)
	_, err = tc.searchGIFs(context.Background(), "other", "")
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	require.EqualValues(t, 4000, respErrorOf(t, err).ExtraData["retry_after_ms"])
	require.EqualValues(t, 1, rpc.searches.Load(), "no RPC while Telegram asked to wait")

	clock.now = clock.now.Add(5 * time.Second)
	rpc.fail = nil
	_, err = tc.searchGIFs(context.Background(), "other", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, rpc.searches.Load())
}

func TestGIFSearchForgetsAnInvalidBot(t *testing.T) {
	rpc := &gifRPC{results: 1, fail: tgerr.New(400, "BOT_INVALID")}
	tc, _ := gifTestClient(rpc)
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	requireRespError(t, err, "M_UNKNOWN", http.StatusBadGateway)
	rpc.fail = nil
	_, err = tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	require.EqualValues(t, 2, rpc.resolves.Load(), "the bot is resolved again")
}

func TestGIFSearchRefusesANonBotUsername(t *testing.T) {
	rpc := &gifRPC{results: 1, resolveTo: true}
	tc, _ := gifTestClient(rpc)
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	requireRespError(t, err, "M_UNKNOWN", http.StatusBadGateway)
	require.EqualValues(t, 0, rpc.searches.Load())
}

type gifTransferCall struct {
	docID int64
	thumb bool
}

func fakeGIFTransfer(calls *[]gifTransferCall, mu *sync.Mutex, fail func(thumb bool) error) gifTransferFunc {
	return func(_ context.Context, doc *tg.Document, thumb tg.PhotoSizeClass) (id.ContentURIString, *event.FileInfo, error) {
		mu.Lock()
		*calls = append(*calls, gifTransferCall{docID: doc.ID, thumb: thumb != nil})
		mu.Unlock()
		if fail != nil {
			if err := fail(thumb != nil); err != nil {
				return "", nil, err
			}
		}
		if thumb != nil {
			if thumb.GetType() != "m" {
				// The largest downloadable thumbnail, never the stripped one.
				return "", nil, tgerr.New(400, "WRONG_SYNTHETIC_THUMB")
			}
			return "mxc://example.invalid/thumb", &event.FileInfo{MimeType: "image/jpeg", Size: 5000}, nil
		}
		return "mxc://example.invalid/video", &event.FileInfo{MimeType: "video/mp4", Size: int(doc.Size)}, nil
	}
}

func TestGIFMediaServesOnlySearchedDocuments(t *testing.T) {
	rpc := &gifRPC{results: 2}
	tc, clock := gifTestClient(rpc)
	var calls []gifTransferCall
	var mu sync.Mutex
	tc.gifState().transfer = fakeGIFTransfer(&calls, &mu, nil)
	ctx := context.Background()

	_, err := tc.gifMedia(ctx, "100", true)
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
	_, err = tc.gifMedia(ctx, "not-a-number", true)
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
	require.Empty(t, calls)

	_, err = tc.searchGIFs(ctx, "synthetic", "")
	require.NoError(t, err)

	// Thumbnail only (grid).
	resp, err := tc.gifMedia(ctx, "100", false)
	require.NoError(t, err)
	require.Equal(t, id.ContentURIString("mxc://example.invalid/thumb"), resp.ThumbnailURL)
	require.Empty(t, resp.URL)
	require.Equal(t, 320, resp.ThumbnailInfo["w"])

	// The GIF itself, ready to send as m.video + fi.mau.gif.
	resp, err = tc.gifMedia(ctx, "100", true)
	require.NoError(t, err)
	require.Equal(t, id.ContentURIString("mxc://example.invalid/video"), resp.URL)
	info := resp.Info
	for _, flag := range []string{"fi.mau.gif", "fi.mau.loop", "fi.mau.autoplay", "fi.mau.hide_controls", "fi.mau.no_audio"} {
		require.Equal(t, true, info[flag], flag)
	}
	require.Equal(t, "video/mp4", info["mimetype"])
	require.Equal(t, 320, info["w"])
	require.Equal(t, 180, info["h"])
	require.Equal(t, 2500, info["duration"])
	require.Equal(t, id.ContentURIString("mxc://example.invalid/thumb"), info["thumbnail_url"])
	require.Equal(t, []gifTransferCall{{100, true}, {100, true}, {100, false}}, calls)

	// Documents expire.
	clock.now = clock.now.Add(gifDocTTL + time.Minute)
	_, err = tc.gifMedia(ctx, "101", true)
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
}

func TestGIFMediaErrors(t *testing.T) {
	rpc := &gifRPC{results: 2}
	tc, _ := gifTestClient(rpc)
	var calls []gifTransferCall
	var mu sync.Mutex
	var videoErr error
	var thumbErr error = tgerr.New(400, "SYNTHETIC_THUMB_FAILURE")
	tc.gifState().transfer = fakeGIFTransfer(&calls, &mu, func(thumb bool) error {
		if thumb {
			return thumbErr
		}
		return videoErr
	})
	ctx := context.Background()
	_, err := tc.searchGIFs(ctx, "synthetic", "")
	require.NoError(t, err)

	// A failed thumbnail does not stop the GIF itself.
	resp, err := tc.gifMedia(ctx, "100", true)
	require.NoError(t, err)
	require.Empty(t, resp.ThumbnailURL)
	require.NotContains(t, resp.Info, "thumbnail_url")
	// But a thumbnail-only request reports it (as a sentence).
	_, err = tc.gifMedia(ctx, "100", false)
	require.Error(t, err)

	// An expired file reference forgets the document: search again.
	thumbErr = nil
	videoErr = tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	_, err = tc.gifMedia(ctx, "100", true)
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
	_, err = tc.gifMedia(ctx, "100", false)
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)

	// FLOOD_WAIT on getFile blocks media (not search) for its duration.
	videoErr = tgerr.New(420, "FLOOD_WAIT_30")
	before := len(calls)
	_, err = tc.gifMedia(ctx, "101", true)
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	_, err = tc.gifMedia(ctx, "101", true)
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	require.Len(t, calls, before+2, "thumbnail and video once, then nothing while blocked")
	_, err = tc.searchGIFs(ctx, "another", "")
	require.NoError(t, err)
}

func TestGIFMediaRefusesLargeFilesForThisBridge(t *testing.T) {
	rpc := &gifRPC{results: 1}
	tc, _ := gifTestClient(rpc)
	tc.main.maxFileSize = 100_000
	var calls []gifTransferCall
	var mu sync.Mutex
	tc.gifState().transfer = fakeGIFTransfer(&calls, &mu, nil)
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	_, err = tc.gifMedia(context.Background(), "100", true)
	requireRespError(t, err, "M_TOO_LARGE", http.StatusRequestEntityTooLarge)
	require.Empty(t, calls)
}

func TestGIFMediaTransfersAreBounded(t *testing.T) {
	rpc := &gifRPC{results: 8}
	tc, _ := gifTestClient(rpc)
	release := make(chan struct{})
	var active, peak atomic.Int64
	tc.gifState().transfer = func(ctx context.Context, doc *tg.Document, thumb tg.PhotoSizeClass) (id.ContentURIString, *event.FileInfo, error) {
		now := active.Add(1)
		for {
			old := peak.Load()
			if now <= old || peak.CompareAndSwap(old, now) {
				break
			}
		}
		<-release
		active.Add(-1)
		return "mxc://example.invalid/x", &event.FileInfo{}, nil
	}
	_, err := tc.searchGIFs(context.Background(), "synthetic", "")
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = tc.gifMedia(context.Background(), itoa(100+i), false)
		}(i)
	}
	require.Eventually(t, func() bool { return active.Load() == gifMediaSlots }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	require.EqualValues(t, gifMediaSlots, peak.Load())
}

func itoa(i int) string { return strconv.Itoa(i) }

// --- routes ---------------------------------------------------------------

func gifRouter(t *testing.T, login *bridgev2.UserLogin) *http.ServeMux {
	t.Helper()
	original := gifLoggedIn
	gifLoggedIn = func(tc *TelegramClient) bool { return tc != nil && tc.client != nil }
	t.Cleanup(func() { gifLoggedIn = original })
	mux := http.NewServeMux()
	addGIFRoutes(mux, func(w http.ResponseWriter, r *http.Request) *bridgev2.UserLogin {
		if login == nil {
			errGIFNotLoggedIn.Write(w)
		}
		return login
	})
	return mux
}

func gifRequest(t *testing.T, mux *http.ServeMux, method, path, body string) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	var out map[string]any
	if rec.Body.Len() > 0 && strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}
	return rec.Code, out
}

func TestGIFRoutes(t *testing.T) {
	rpc := &gifRPC{results: 2}
	tc, _ := gifTestClient(rpc)
	var calls []gifTransferCall
	var mu sync.Mutex
	tc.gifState().transfer = fakeGIFTransfer(&calls, &mu, nil)
	mux := gifRouter(t, &bridgev2.UserLogin{Client: tc})

	code, body := gifRequest(t, mux, http.MethodPost, "/v3/gif/search", `{"query":"synthetic","offset":""}`)
	require.Equal(t, http.StatusOK, code)
	results := body["results"].([]any)
	require.Len(t, results, 2)
	first := results[0].(map[string]any)
	require.Equal(t, "100", first["id"])
	require.IsType(t, "", first["thumb"], "the inline preview is base64 JPEG")
	require.Equal(t, "next", body["next_offset"])

	code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/media", `{"id":"100","video":true}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "mxc://example.invalid/video", body["url"])
	require.Equal(t, true, body["info"].(map[string]any)["fi.mau.gif"])

	// The query is never in the URL: GET is not a route.
	code, _ = gifRequest(t, mux, http.MethodGet, "/v3/gif/search?q=synthetic", "")
	require.Equal(t, http.StatusMethodNotAllowed, code)

	for _, bad := range []string{`{"query":"   "}`, `{"query":"` + strings.Repeat("a", gifMaxQueryRunes+1) + `"}`, `{"query":"x","offset":"` + strings.Repeat("o", gifMaxOffsetBytes+1) + `"}`} {
		code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/search", bad)
		require.Equal(t, http.StatusBadRequest, code)
		require.Equal(t, "M_INVALID_PARAM", body["errcode"])
	}
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/search", `not json`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "M_NOT_JSON", body["errcode"])
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/media", `{"id":""}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/media", `{"id":"555"}`)
	require.Equal(t, http.StatusNotFound, code)
	require.Equal(t, "M_NOT_FOUND", body["errcode"])
	require.NotContains(t, body, "fi.mau.internal_error")
	require.EqualValues(t, 1, rpc.searches.Load())
}

func TestGIFRoutesNeedATelegramLogin(t *testing.T) {
	mux := gifRouter(t, nil)
	code, body := gifRequest(t, mux, http.MethodPost, "/v3/gif/search", `{"query":"synthetic"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "FI.MAU.NOT_LOGGED_IN", body["errcode"])

	// A login whose Telegram client is gone.
	mux = gifRouter(t, &bridgev2.UserLogin{Client: &TelegramClient{}})
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/gif/media", `{"id":"1"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "FI.MAU.NOT_LOGGED_IN", body["errcode"])
}

func TestGIFRouteErrorsAreSentences(t *testing.T) {
	rpc := &gifRPC{results: 1, fail: tgerr.New(500, "SYNTHETIC_INTERNAL")}
	tc, _ := gifTestClient(rpc)
	mux := gifRouter(t, &bridgev2.UserLogin{Client: tc})
	code, body := gifRequest(t, mux, http.MethodPost, "/v3/gif/search", `{"query":"synthetic"}`)
	require.Equal(t, http.StatusBadGateway, code)
	require.Equal(t, "Telegram didn't answer. Try again.", body["error"])
	require.NotContains(t, body, "fi.mau.internal_error")
}

func requireRespError(t *testing.T, err error, code string, status int) {
	t.Helper()
	respErr := respErrorOf(t, err)
	require.Equal(t, code, respErr.ErrCode)
	require.Equal(t, status, respErr.StatusCode)
	require.NotEmpty(t, respErr.Err)
	require.NotContains(t, respErr.Err, "_", "a sentence, not a Telegram code")
}

func respErrorOf(t *testing.T, err error) mautrix.RespError {
	t.Helper()
	var respErr mautrix.RespError
	require.ErrorAs(t, err, &respErr)
	return respErr
}
