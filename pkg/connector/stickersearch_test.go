// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

// stickerSetRPC is a synthetic Telegram for the sticker catalogue:
// messages.searchStickerSets, messages.getFeaturedStickers,
// messages.getOldFeaturedStickers and messages.installStickerSet. Fictional
// sets only.
type stickerSetRPC struct {
	mu        sync.Mutex
	searches  atomic.Int64
	featured  atomic.Int64
	old       atomic.Int64
	installs  atomic.Int64
	queries   []string
	offsets   []int
	installed []string
	sets      int
	total     int
	fail      error
	archive   bool
}

func stickerSet(i int, emoji bool) tg.StickerSet {
	return tg.StickerSet{
		ID:         int64(1000 + i),
		AccessHash: int64(7 * i),
		Title:      "Fictional set " + itoa(i),
		ShortName:  "FictionalSet" + itoa(i),
		Count:      10 + i,
		Emojis:     emoji,
	}
}

func coveredSets(n, from int) []tg.StickerSetCoveredClass {
	out := []tg.StickerSetCoveredClass{
		// No usable short name: skipped.
		&tg.StickerSetNoCovered{Set: tg.StickerSet{ID: 1, ShortName: "bad name!", Title: "Bad"}},
	}
	for i := from; i < from+n; i++ {
		switch i % 3 {
		case 0:
			out = append(out, &tg.StickerSetCovered{Set: stickerSet(i, false), Cover: gifDocument(int64(5000+i), 9000)})
		case 1:
			out = append(out, &tg.StickerSetMultiCovered{Set: stickerSet(i, true), Covers: []tg.DocumentClass{gifDocument(int64(5000+i), 9000)}})
		default:
			out = append(out, &tg.StickerSetNoCovered{Set: stickerSet(i, false)})
		}
	}
	return out
}

func (g *stickerSetRPC) client() *telegram.Client {
	return telegram.NewClient(1, "synthetic-not-an-app-secret", telegram.Options{Middlewares: []telegram.Middleware{
		telegram.MiddlewareFunc(func(tg.Invoker) telegram.InvokeFunc {
			return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				var result bin.Encoder
				switch req := input.(type) {
				case *tg.MessagesSearchStickerSetsRequest:
					g.searches.Add(1)
					g.mu.Lock()
					g.queries = append(g.queries, req.Q)
					g.mu.Unlock()
					if g.fail != nil {
						return g.fail
					}
					result = &tg.MessagesFoundStickerSets{Sets: coveredSets(g.sets, 0)}
				case *tg.MessagesGetFeaturedStickersRequest:
					g.featured.Add(1)
					if g.fail != nil {
						return g.fail
					}
					result = &tg.MessagesFeaturedStickers{Count: g.sets, Sets: coveredSets(g.sets, 0), Unread: []int64{}}
				case *tg.MessagesGetOldFeaturedStickersRequest:
					// The older featured sets are their own list (offset 0 is
					// the first old set), numbered after the current ones.
					g.old.Add(1)
					g.mu.Lock()
					g.offsets = append(g.offsets, req.Offset)
					g.mu.Unlock()
					oldCount := max(g.total-g.sets, 0)
					n := min(req.Limit, oldCount-req.Offset)
					result = &tg.MessagesFeaturedStickers{Count: oldCount, Sets: coveredSets(max(n, 0), g.sets+req.Offset)[1:], Unread: []int64{}}
				case *tg.MessagesInstallStickerSetRequest:
					g.installs.Add(1)
					if g.fail != nil {
						return g.fail
					}
					set, ok := req.Stickerset.(*tg.InputStickerSetShortName)
					if !ok {
						return tgerr.New(400, "STICKERSET_INVALID")
					}
					g.mu.Lock()
					g.installed = append(g.installed, set.ShortName)
					g.mu.Unlock()
					if g.archive {
						result = &tg.MessagesStickerSetInstallResultArchive{Sets: []tg.StickerSetCoveredClass{}}
					} else {
						result = &tg.MessagesStickerSetInstallResultSuccess{}
					}
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

type coverCall struct{ docID int64 }

func stickerSetTestClient(rpc *stickerSetRPC) (*TelegramClient, *gifClock, *[]coverCall) {
	tc := &TelegramClient{main: &TelegramConnector{}, client: rpc.client()}
	clock := &gifClock{now: time.Unix(1_800_000_000, 0)}
	s := tc.stickerSetState()
	s.now = clock.get
	var calls []coverCall
	var mu sync.Mutex
	s.transferCover = func(ctx context.Context, doc *tg.Document) (id.ContentURIString, *event.FileInfo, error) {
		mu.Lock()
		calls = append(calls, coverCall{doc.ID})
		mu.Unlock()
		return id.ContentURIString("mxc://example.invalid/cover" + itoa(int(doc.ID))), &event.FileInfo{MimeType: "image/webp", Size: 900, Width: 128, Height: 128}, nil
	}
	return tc, clock, &calls
}

func TestStickerSetSearchConvertsAndCaches(t *testing.T) {
	rpc := &stickerSetRPC{sets: 4}
	tc, clock, _ := stickerSetTestClient(rpc)
	ctx := context.Background()

	resp, err := tc.searchStickerSets(ctx, "Fictional  Cats")
	require.NoError(t, err)
	require.Len(t, resp.Sets, 4, "the set without a valid short name is skipped")
	first := resp.Sets[0]
	require.Equal(t, "FictionalSet0", first.ShortName)
	require.Equal(t, "https://t.me/addstickers/FictionalSet0", first.URL)
	require.Equal(t, 10, first.Count)
	require.True(t, first.HasCover)
	require.Equal(t, "https://t.me/addemoji/FictionalSet1", resp.Sets[1].URL, "an emoji set links to addemoji")
	require.True(t, resp.Sets[1].Emoji)
	require.False(t, resp.Sets[2].HasCover, "a set without a cover says so")

	// Same query (case and spacing differ): cached.
	_, err = tc.searchStickerSets(ctx, "fictional cats")
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.searches.Load())
	require.Equal(t, []string{"Fictional  Cats"}, rpc.queries, "the query keeps its case")

	clock.now = clock.now.Add(stickerSetSearchTTL + time.Second)
	_, err = tc.searchStickerSets(ctx, "fictional cats")
	require.NoError(t, err)
	require.EqualValues(t, 2, rpc.searches.Load())
}

func TestStickerSetSearchIsBounded(t *testing.T) {
	rpc := &stickerSetRPC{sets: stickerSetMaxResults + 10}
	tc, _, _ := stickerSetTestClient(rpc)
	resp, err := tc.searchStickerSets(context.Background(), "many")
	require.NoError(t, err)
	require.Len(t, resp.Sets, stickerSetMaxResults)
}

func TestStickerSetFeaturedPages(t *testing.T) {
	rpc := &stickerSetRPC{sets: 5, total: 30}
	tc, _, _ := stickerSetTestClient(rpc)
	ctx := context.Background()

	first, err := tc.featuredStickerSets(ctx, 0)
	require.NoError(t, err)
	require.Len(t, first.Sets, 5)
	require.Equal(t, 6, first.NextOffset, "the next offset counts every set Telegram sent")

	second, err := tc.featuredStickerSets(ctx, first.NextOffset)
	require.NoError(t, err)
	require.Len(t, second.Sets, stickerSetFeaturedPage)
	require.Equal(t, "FictionalSet5", second.Sets[0].ShortName, "old featured sets follow the current ones without a gap")
	require.Equal(t, 26, second.NextOffset)

	last, err := tc.featuredStickerSets(ctx, second.NextOffset)
	require.NoError(t, err)
	require.Len(t, last.Sets, 5)
	require.Equal(t, "FictionalSet25", last.Sets[0].ShortName)
	require.Zero(t, last.NextOffset, "no next page after the last one")

	_, err = tc.featuredStickerSets(ctx, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.featured.Load(), "pages are cached")
	require.EqualValues(t, 2, rpc.old.Load())
	require.Equal(t, []int{0, 20}, rpc.offsets, "old featured offsets start at 0")
}

func TestStickerSetFeaturedLaterPageAfterRestart(t *testing.T) {
	rpc := &stickerSetRPC{sets: 3, total: 10}
	tc, _, _ := stickerSetTestClient(rpc)
	// A client keeps paging after the bridge restarted (no first page).
	page, err := tc.featuredStickerSets(context.Background(), 4)
	require.NoError(t, err)
	require.EqualValues(t, 1, rpc.featured.Load(), "the current list is loaded to know its length")
	require.Equal(t, []int{0}, rpc.offsets)
	require.Equal(t, "FictionalSet3", page.Sets[0].ShortName)
	require.Zero(t, page.NextOffset)
}

func TestStickerSetFeaturedWithoutOldSets(t *testing.T) {
	rpc := &stickerSetRPC{sets: 2, total: 2}
	tc, _, _ := stickerSetTestClient(rpc)
	ctx := context.Background()
	first, err := tc.featuredStickerSets(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 3, first.NextOffset)
	empty, err := tc.featuredStickerSets(ctx, first.NextOffset)
	require.NoError(t, err)
	require.Empty(t, empty.Sets)
	require.Zero(t, empty.NextOffset)
}

func TestStickerSetCoverServesOnlyAnsweredSets(t *testing.T) {
	rpc := &stickerSetRPC{sets: 3}
	tc, clock, calls := stickerSetTestClient(rpc)
	ctx := context.Background()

	_, err := tc.stickerSetCover(ctx, "FictionalSet0")
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)

	_, err = tc.searchStickerSets(ctx, "fictional")
	require.NoError(t, err)
	cover, err := tc.stickerSetCover(ctx, "fictionalset0")
	require.NoError(t, err, "short names match without case")
	require.Equal(t, id.ContentURIString("mxc://example.invalid/cover5000"), cover.URL)
	require.Equal(t, "image/webp", cover.Info["mimetype"])
	require.Equal(t, 128, cover.Info["w"])

	cover, err = tc.stickerSetCover(ctx, "FictionalSet1")
	require.NoError(t, err, "a multi-covered set uses its first cover")
	require.Equal(t, id.ContentURIString("mxc://example.invalid/cover5001"), cover.URL)

	_, err = tc.stickerSetCover(ctx, "FictionalSet2")
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
	require.Len(t, *calls, 2)

	clock.now = clock.now.Add(stickerSetCoverTTL + time.Second)
	_, err = tc.stickerSetCover(ctx, "FictionalSet0")
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
}

func TestStickerSetCoverForgetsExpiredReferences(t *testing.T) {
	rpc := &stickerSetRPC{sets: 1}
	tc, _, _ := stickerSetTestClient(rpc)
	ctx := context.Background()
	_, err := tc.searchStickerSets(ctx, "fictional")
	require.NoError(t, err)
	tc.stickerSetState().transferCover = func(context.Context, *tg.Document) (id.ContentURIString, *event.FileInfo, error) {
		return "", nil, tgerr.New(400, "FILE_REFERENCE_EXPIRED")
	}
	_, err = tc.stickerSetCover(ctx, "FictionalSet0")
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
	_, ok := tc.stickerSetState().covers.get("fictionalset0")
	require.False(t, ok)
}

func TestStickerSetFloodWaitBacksOffWithoutRPC(t *testing.T) {
	rpc := &stickerSetRPC{sets: 1, fail: tgerr.New(420, "FLOOD_WAIT_30")}
	tc, clock, _ := stickerSetTestClient(rpc)
	ctx := context.Background()
	_, err := tc.searchStickerSets(ctx, "one")
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	_, err = tc.featuredStickerSets(ctx, 0)
	requireRespError(t, err, "M_LIMIT_EXCEEDED", http.StatusTooManyRequests)
	require.EqualValues(t, 1, rpc.searches.Load())
	require.Zero(t, rpc.featured.Load(), "no RPC while Telegram asked to wait")

	rpc.fail = nil
	clock.now = clock.now.Add(31 * time.Second)
	_, err = tc.featuredStickerSets(ctx, 0)
	require.NoError(t, err)
}

func TestStickerSetInstall(t *testing.T) {
	rpc := &stickerSetRPC{sets: 2}
	tc, _, _ := stickerSetTestClient(rpc)
	ctx := context.Background()
	_, err := tc.searchStickerSets(ctx, "fictional")
	require.NoError(t, err)

	archived, err := tc.installStickerSet(ctx, "FictionalSet0")
	require.NoError(t, err)
	require.False(t, archived)
	require.Equal(t, []string{"FictionalSet0"}, rpc.installed)

	// Installing drops cached answers (their installed flags are stale).
	_, err = tc.searchStickerSets(ctx, "fictional")
	require.NoError(t, err)
	require.EqualValues(t, 2, rpc.searches.Load())

	rpc.archive = true
	archived, err = tc.installStickerSet(ctx, "FictionalSet1")
	require.NoError(t, err)
	require.True(t, archived)

	rpc.fail = tgerr.New(400, "STICKERSET_INVALID")
	_, err = tc.installStickerSet(ctx, "Missing")
	requireRespError(t, err, "M_NOT_FOUND", http.StatusNotFound)
}

// --- routes ---------------------------------------------------------------

func stickerSetRouter(t *testing.T, login *bridgev2.UserLogin) *http.ServeMux {
	t.Helper()
	original := gifLoggedIn
	gifLoggedIn = func(tc *TelegramClient) bool { return tc != nil && tc.client != nil }
	t.Cleanup(func() { gifLoggedIn = original })
	mux := http.NewServeMux()
	addStickerSetRoutes(mux, func(w http.ResponseWriter, r *http.Request) *bridgev2.UserLogin {
		if login == nil {
			errGIFNotLoggedIn.Write(w)
		}
		return login
	})
	return mux
}

func TestStickerSetRoutes(t *testing.T) {
	rpc := &stickerSetRPC{sets: 2, total: 2}
	tc, _, _ := stickerSetTestClient(rpc)
	mux := stickerSetRouter(t, &bridgev2.UserLogin{Client: tc})

	code, body := gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/search", `{"query":"synthetic"}`)
	require.Equal(t, http.StatusOK, code)
	sets := body["sets"].([]any)
	require.Len(t, sets, 2)
	first := sets[0].(map[string]any)
	require.Equal(t, "FictionalSet0", first["short_name"])
	require.Equal(t, "Fictional set 0", first["title"])
	require.EqualValues(t, 10, first["count"])
	require.Equal(t, true, first["has_cover"])

	code, body = gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/featured", `{"offset":0}`)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["sets"].([]any), 2)
	require.EqualValues(t, 3, body["next_offset"], "older featured sets may follow")

	code, body = gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/cover", `{"short_name":"FictionalSet0"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "mxc://example.invalid/cover5000", body["url"])

	code, body = gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/install", `{"short_name":"FictionalSet0"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, body["installed"])

	// The query is never in the URL: GET is not a route.
	code, _ = gifRequest(t, mux, http.MethodGet, "/v3/sticker_sets/search?q=synthetic", "")
	require.Equal(t, http.StatusMethodNotAllowed, code)

	for path, bad := range map[string]string{
		"/v3/sticker_sets/search":   `{"query":"` + strings.Repeat("a", stickerSetMaxQueryRunes+1) + `"}`,
		"/v3/sticker_sets/featured": `{"offset":-1}`,
		"/v3/sticker_sets/cover":    `{"short_name":"../etc"}`,
		"/v3/sticker_sets/install":  `{"short_name":""}`,
	} {
		code, body = gifRequest(t, mux, http.MethodPost, path, bad)
		require.Equal(t, http.StatusBadRequest, code, path)
		require.Equal(t, "M_INVALID_PARAM", body["errcode"], path)
	}
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/search", `{"query":"   "}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, body = gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/featured", `nope`)
	require.Equal(t, "M_NOT_JSON", body["errcode"])
}

func TestStickerSetRoutesNeedATelegramLogin(t *testing.T) {
	mux := stickerSetRouter(t, nil)
	code, body := gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/search", `{"query":"synthetic"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Equal(t, "FI.MAU.NOT_LOGGED_IN", body["errcode"])
}

func TestStickerSetRouteErrorsAreSentences(t *testing.T) {
	rpc := &stickerSetRPC{sets: 1, fail: tgerr.New(500, "SYNTHETIC_INTERNAL")}
	tc, _, _ := stickerSetTestClient(rpc)
	mux := stickerSetRouter(t, &bridgev2.UserLogin{Client: tc})
	code, body := gifRequest(t, mux, http.MethodPost, "/v3/sticker_sets/search", `{"query":"synthetic"}`)
	require.Equal(t, http.StatusBadGateway, code)
	require.Equal(t, "Telegram didn't answer. Try again.", body["error"])
	require.NotContains(t, body, "fi.mau.internal_error")
}
