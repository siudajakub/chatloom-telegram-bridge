// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

// Telegram's sticker catalogue for ChatLoom's "Add stickers" (wave 11).
//
// Four provisioning routes next to the GIF search ones (same router, same
// authentication as `/v3/image_pack/*`, `login_id` optional):
//
//	POST /_matrix/provision/v3/sticker_sets/search   {"query": "cats"}
//	POST /_matrix/provision/v3/sticker_sets/featured {"offset": 0}
//	POST /_matrix/provision/v3/sticker_sets/cover    {"short_name": "Cats"}
//	POST /_matrix/provision/v3/sticker_sets/install  {"short_name": "Cats"}
//
// search and featured answer set metadata only (title, short name, link,
// sticker count, flags) through `messages.searchStickerSets` and
// `messages.getFeaturedStickers` / `messages.getOldFeaturedStickers` with
// the user's own login; nothing is uploaded. cover uploads the small static
// thumbnail of a set's cover sticker (a set that a recent search or featured
// page of the same login returned) as public media, cached by Telegram
// location ID like every bridged file, so the app can show a grid of covers.
// The stickers themselves are previewed and imported through the existing
// `/v3/image_pack/import?pack_url=` route. install adds the set to the user's
// Telegram account (`messages.installStickerSet`), only when the app asks.
//
// Queries are POST bodies (never in the access-logged URI) and are never
// logged; Telegram errors are answered as sentences.

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/media"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

const (
	stickerSetMaxQueryRunes = 64
	// stickerSetMaxResults bounds one answer (Telegram answers ~50 sets).
	stickerSetMaxResults = 50
	// stickerSetFeaturedPage is the page size of the older featured sets.
	stickerSetFeaturedPage = 20
	stickerSetMaxOffset    = 1000
	stickerSetCacheSize    = 64
	stickerSetSearchTTL    = 10 * time.Minute
	stickerSetFeaturedTTL  = 30 * time.Minute
	// stickerSetCoverCacheSize/TTL: covers of sets an answer returned, the
	// only ones the cover route serves.
	stickerSetCoverCacheSize = 1000
	stickerSetCoverTTL       = time.Hour
	// stickerSetCoverSlots: concurrent cover transfers per login.
	stickerSetCoverSlots   = 4
	stickerSetCoverTimeout = 30 * time.Second
)

var (
	errStickerSetUnknown = mautrix.MNotFound.WithMessage("This sticker pack isn't available. Search again.")
	errStickerSetNoCover = mautrix.MNotFound.WithMessage("This sticker pack has no cover.")
	errStickerSetInvalid = mautrix.MNotFound.WithMessage("This sticker pack doesn't exist.")
	stickerShortNameRe   = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)
)

// stickerSetInfo is one set of a search or featured answer.
type stickerSetInfo struct {
	ShortName string `json:"short_name"`
	Title     string `json:"title"`
	URL       string `json:"url"`
	Count     int    `json:"count"`
	Emoji     bool   `json:"emoji,omitempty"`
	Official  bool   `json:"official,omitempty"`
	Masks     bool   `json:"masks,omitempty"`
	// Installed: in the user's Telegram account (not archived).
	Installed bool `json:"installed,omitempty"`
	// HasCover: the cover route can serve a cover for it.
	HasCover bool `json:"has_cover,omitempty"`
}

type stickerSetsResponse struct {
	Sets       []stickerSetInfo `json:"sets"`
	NextOffset int              `json:"next_offset,omitempty"`
}

type stickerSetCoverResponse struct {
	URL  id.ContentURIString `json:"url"`
	Info map[string]any      `json:"info,omitempty"`
}

// stickerCoverTransferFunc uploads a cover (its thumbnail when it has one)
// and returns the mxc URI with the file info.
type stickerCoverTransferFunc func(ctx context.Context, doc *tg.Document) (id.ContentURIString, *event.FileInfo, error)

type stickerSetsEntry struct {
	resp    *stickerSetsResponse
	expires time.Time
}

type stickerCoverEntry struct {
	doc *tg.Document
	at  time.Time
}

// stickerSetState is the per-login state of the sticker catalogue.
type stickerSetState struct {
	mu           sync.Mutex
	answers      *boundedCache[string, stickerSetsEntry]
	covers       *boundedCache[string, stickerCoverEntry]
	blockedUntil time.Time
	coverBlocked time.Time
	// featuredLen is how many sets Telegram's current featured list had
	// (-1 unknown): pages after it are the older featured sets, whose own
	// offsets start at 0 (as in TDLib's getTrendingStickerSets).
	featuredLen   int
	slots         chan struct{}
	now           func() time.Time
	transferCover stickerCoverTransferFunc
}

func (tc *TelegramClient) stickerSetState() *stickerSetState {
	tc.stickerSetsOnce.Do(func() {
		tc.stickerSets = &stickerSetState{
			answers:       newBoundedCache[string, stickerSetsEntry](stickerSetCacheSize),
			covers:        newBoundedCache[string, stickerCoverEntry](stickerSetCoverCacheSize),
			slots:         make(chan struct{}, stickerSetCoverSlots),
			featuredLen:   -1,
			now:           time.Now,
			transferCover: tc.transferStickerCover,
		}
	})
	return tc.stickerSets
}

func (s *stickerSetState) blockedFor(cover bool, now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	until := s.blockedUntil
	if cover {
		until = s.coverBlocked
	}
	if now.Before(until) {
		return until.Sub(now)
	}
	return 0
}

// rpcError turns a Telegram error into a route error and records FLOOD_WAIT.
func (s *stickerSetState) rpcError(err error, cover bool, now time.Time) error {
	if wait, ok := tgerr.AsFloodWait(err); ok {
		s.mu.Lock()
		if cover {
			s.coverBlocked = now.Add(wait)
		} else {
			s.blockedUntil = now.Add(wait)
		}
		s.mu.Unlock()
		return gifFloodError(wait)
	}
	if tgerr.Is(err, tg.ErrStickersetInvalid) {
		return errStickerSetInvalid
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return errGIFTelegram.WithMessage("Telegram didn't answer in time. Try again.").WithStatus(http.StatusGatewayTimeout)
	}
	return gifUpstreamError{err}
}

func stickerSetKey(shortName string) string { return strings.ToLower(shortName) }

// stickerCoverOf picks the cover document of a covered set, or nil.
func stickerCoverOf(covered tg.StickerSetCoveredClass) *tg.Document {
	var raw tg.DocumentClass
	switch set := covered.(type) {
	case *tg.StickerSetCovered:
		raw = set.Cover
	case *tg.StickerSetMultiCovered:
		if len(set.Covers) > 0 {
			raw = set.Covers[0]
		}
	case *tg.StickerSetFullCovered:
		if len(set.Documents) > 0 {
			raw = set.Documents[0]
		}
	}
	doc, _ := raw.(*tg.Document)
	return doc
}

// convertStickerSets keeps sets with a short name, bounded, deduplicated,
// and remembers their covers for the cover route.
func (s *stickerSetState) convertStickerSets(sets []tg.StickerSetCoveredClass, now time.Time) []stickerSetInfo {
	out := make([]stickerSetInfo, 0, min(len(sets), stickerSetMaxResults))
	seen := make(map[string]struct{}, cap(out))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, covered := range sets {
		if len(out) >= stickerSetMaxResults {
			break
		}
		set := covered.GetSet()
		key := stickerSetKey(set.ShortName)
		if !stickerShortNameRe.MatchString(set.ShortName) {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		linkType := "addstickers"
		if set.Emojis {
			linkType = "addemoji"
		}
		info := stickerSetInfo{
			ShortName: set.ShortName,
			Title:     set.Title,
			URL:       "https://t.me/" + linkType + "/" + set.ShortName,
			Count:     set.Count,
			Emoji:     set.Emojis,
			Official:  set.Official,
			Masks:     set.Masks,
			Installed: set.InstalledDate != 0 && !set.Archived,
		}
		if doc := stickerCoverOf(covered); doc != nil {
			s.covers.put(key, stickerCoverEntry{doc: doc, at: now})
			info.HasCover = true
		} else if entry, ok := s.covers.get(key); ok {
			entry.at = now
			s.covers.put(key, entry)
			info.HasCover = true
		}
		out = append(out, info)
	}
	return out
}

// touchCovers keeps the covers of a cached answer servable.
func (s *stickerSetState) touchCovers(resp *stickerSetsResponse, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, set := range resp.Sets {
		key := stickerSetKey(set.ShortName)
		if entry, ok := s.covers.get(key); ok {
			entry.at = now
			s.covers.put(key, entry)
		}
	}
}

func (s *stickerSetState) cached(key string, now time.Time) (*stickerSetsResponse, bool) {
	s.mu.Lock()
	entry, ok := s.answers.get(key)
	if ok && now.After(entry.expires) {
		s.answers.remove(key)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	s.touchCovers(entry.resp, now)
	return entry.resp, true
}

func (s *stickerSetState) store(key string, resp *stickerSetsResponse, ttl time.Duration, now time.Time) {
	s.mu.Lock()
	s.answers.put(key, stickerSetsEntry{resp: resp, expires: now.Add(ttl)})
	s.mu.Unlock()
}

// searchStickerSets answers the sets matching query (Telegram's own set
// search).
func (tc *TelegramClient) searchStickerSets(ctx context.Context, query string) (*stickerSetsResponse, error) {
	s := tc.stickerSetState()
	now := s.now()
	key := "search\x00" + strings.ToLower(strings.Join(strings.Fields(query), " "))
	if resp, ok := s.cached(key, now); ok {
		return resp, nil
	}
	if wait := s.blockedFor(false, now); wait > 0 {
		return nil, gifFloodError(wait)
	}
	res, err := tc.client.API().MessagesSearchStickerSets(ctx, &tg.MessagesSearchStickerSetsRequest{Q: query})
	if err != nil {
		return nil, s.rpcError(err, false, s.now())
	}
	resp := &stickerSetsResponse{Sets: []stickerSetInfo{}}
	if found, ok := res.(*tg.MessagesFoundStickerSets); ok {
		resp.Sets = s.convertStickerSets(found.Sets, now)
	}
	s.store(key, resp, stickerSetSearchTTL, now)
	return resp, nil
}

// featuredStickerSets answers one page of Telegram's trending sets. Offsets
// count one list: the current featured sets first (offset 0, one page),
// then the older featured sets (`messages.getOldFeaturedStickers`, whose
// own offset starts at 0 after the current ones).
func (tc *TelegramClient) featuredStickerSets(ctx context.Context, offset int) (*stickerSetsResponse, error) {
	s := tc.stickerSetState()
	now := s.now()
	key := "featured\x00" + strconv.Itoa(offset)
	if resp, ok := s.cached(key, now); ok {
		return resp, nil
	}
	if wait := s.blockedFor(false, now); wait > 0 {
		return nil, gifFloodError(wait)
	}
	if offset == 0 {
		res, err := tc.client.API().MessagesGetFeaturedStickers(ctx, 0)
		if err != nil {
			return nil, s.rpcError(err, false, s.now())
		}
		resp := &stickerSetsResponse{Sets: []stickerSetInfo{}}
		if featured, ok := res.(*tg.MessagesFeaturedStickers); ok {
			resp.Sets = s.convertStickerSets(featured.Sets, now)
			s.mu.Lock()
			s.featuredLen = len(featured.Sets)
			s.mu.Unlock()
			// Older featured sets may follow; an empty next page ends it.
			if len(featured.Sets) > 0 {
				resp.NextOffset = len(featured.Sets)
			}
		}
		s.store(key, resp, stickerSetFeaturedTTL, now)
		return resp, nil
	}
	s.mu.Lock()
	current := s.featuredLen
	s.mu.Unlock()
	if current < 0 {
		// The bridge restarted since the first page: learn its length.
		if _, err := tc.featuredStickerSets(ctx, 0); err != nil {
			return nil, err
		}
		s.mu.Lock()
		current = s.featuredLen
		s.mu.Unlock()
	}
	oldOffset := offset - current
	if oldOffset < 0 {
		// The current list shrank under a paging client: start the old one.
		oldOffset = 0
	}
	res, err := tc.client.API().MessagesGetOldFeaturedStickers(ctx, &tg.MessagesGetOldFeaturedStickersRequest{
		Offset: oldOffset,
		Limit:  stickerSetFeaturedPage,
	})
	if err != nil {
		return nil, s.rpcError(err, false, s.now())
	}
	resp := &stickerSetsResponse{Sets: []stickerSetInfo{}}
	if old, ok := res.(*tg.MessagesFeaturedStickers); ok {
		resp.Sets = s.convertStickerSets(old.Sets, now)
		next := offset + len(old.Sets)
		more := oldOffset+len(old.Sets) < old.Count || (old.Count == 0 && len(old.Sets) >= stickerSetFeaturedPage)
		if len(old.Sets) > 0 && more && next <= stickerSetMaxOffset {
			resp.NextOffset = next
		}
	}
	s.store(key, resp, stickerSetFeaturedTTL, now)
	return resp, nil
}

// stickerCoverThumb picks the largest downloadable thumbnail, or nil.
func stickerCoverThumb(doc *tg.Document) tg.PhotoSizeClass {
	thumb, _, _ := gifThumbnail(doc)
	return thumb
}

// transferStickerCover is the production stickerCoverTransferFunc: the
// cover's static thumbnail (a small WebP or JPEG), else the sticker itself
// with the bridge's sticker conversion; uploaded once, cached by location.
func (tc *TelegramClient) transferStickerCover(ctx context.Context, doc *tg.Document) (id.ContentURIString, *event.FileInfo, error) {
	xfer := media.NewTransferer(tc.client.API()).WithFilename("cover")
	var ready *media.ReadyTransferer
	if thumb := stickerCoverThumb(doc); thumb != nil {
		only := *doc
		only.Thumbs = []tg.PhotoSizeClass{thumb}
		ready = xfer.WithDocument(&only, true)
	} else {
		ready = xfer.WithStickerConfig(tc.main.Config.AnimatedSticker).WithDocument(doc, false)
	}
	mxc, _, info, err := ready.Transfer(ctx, tc.main.Store, tc.main.Bridge.Bot)
	return mxc, info, err
}

// stickerSetCover uploads (or finds) the cover of a set a recent answer of
// this login returned.
func (tc *TelegramClient) stickerSetCover(ctx context.Context, shortName string) (*stickerSetCoverResponse, error) {
	s := tc.stickerSetState()
	now := s.now()
	key := stickerSetKey(shortName)
	s.mu.Lock()
	entry, ok := s.covers.get(key)
	if ok && now.Sub(entry.at) > stickerSetCoverTTL {
		s.covers.remove(key)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return nil, errStickerSetUnknown
	}
	if entry.doc == nil {
		return nil, errStickerSetNoCover
	}
	if wait := s.blockedFor(true, now); wait > 0 {
		return nil, gifFloodError(wait)
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return nil, s.rpcError(ctx.Err(), true, now)
	}
	mxc, info, err := s.transferCover(ctx, entry.doc)
	if err != nil {
		if tgerr.Is(err, tg.ErrFileReferenceExpired, tg.ErrFileReferenceInvalid, tg.ErrFileIDInvalid) {
			s.mu.Lock()
			s.covers.remove(key)
			s.mu.Unlock()
			return nil, errStickerSetUnknown
		}
		return nil, s.rpcError(err, true, s.now())
	}
	if mxc == "" {
		return nil, errGIFTelegram
	}
	resp := &stickerSetCoverResponse{URL: mxc, Info: map[string]any{}}
	if info != nil {
		if info.MimeType != "" {
			resp.Info["mimetype"] = info.MimeType
		}
		if info.Size > 0 {
			resp.Info["size"] = info.Size
		}
		if info.Width > 0 && info.Height > 0 {
			resp.Info["w"], resp.Info["h"] = info.Width, info.Height
		}
	}
	return resp, nil
}

// installStickerSet adds a set to the user's Telegram account (unarchived).
// Answers whether Telegram archived other sets to make room.
func (tc *TelegramClient) installStickerSet(ctx context.Context, shortName string) (bool, error) {
	s := tc.stickerSetState()
	now := s.now()
	if wait := s.blockedFor(false, now); wait > 0 {
		return false, gifFloodError(wait)
	}
	res, err := tc.client.API().MessagesInstallStickerSet(ctx, &tg.MessagesInstallStickerSetRequest{
		Stickerset: &tg.InputStickerSetShortName{ShortName: shortName},
	})
	if err != nil {
		return false, s.rpcError(err, false, s.now())
	}
	// The installed flags of cached answers are stale now.
	s.mu.Lock()
	s.answers = newBoundedCache[string, stickerSetsEntry](stickerSetCacheSize)
	s.mu.Unlock()
	_, archived := res.(*tg.MessagesStickerSetInstallResultArchive)
	return archived, nil
}

// --- HTTP routes ----------------------------------------------------------

func addStickerSetRoutes(router *http.ServeMux, getLogin gifLoginResolver) {
	router.HandleFunc("POST /v3/sticker_sets/search", stickerSetSearchHandler(getLogin))
	router.HandleFunc("POST /v3/sticker_sets/featured", stickerSetFeaturedHandler(getLogin))
	router.HandleFunc("POST /v3/sticker_sets/cover", stickerSetCoverHandler(getLogin))
	router.HandleFunc("POST /v3/sticker_sets/install", stickerSetInstallHandler(getLogin))
}

func writeStickerSetError(w http.ResponseWriter, r *http.Request, err error) {
	var respErr mautrix.RespError
	if errors.As(err, &respErr) {
		respErr.Write(w)
		return
	}
	zerolog.Ctx(r.Context()).Warn().Err(err).Msg("Sticker set request failed")
	errGIFTelegram.Write(w)
}

func decodeShortName(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req struct {
		ShortName string `json:"short_name"`
	}
	if !decodeGIFBody(w, r, &req) {
		return "", false
	}
	if !stickerShortNameRe.MatchString(req.ShortName) {
		mautrix.MInvalidParam.WithMessage("Invalid sticker pack name").Write(w)
		return "", false
	}
	return req.ShortName, true
}

func stickerSetSearchHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		var req struct {
			Query string `json:"query"`
		}
		if !decodeGIFBody(w, r, &req) {
			return
		}
		query := strings.TrimSpace(req.Query)
		if query == "" || !utf8.ValidString(query) || utf8.RuneCountInString(query) > stickerSetMaxQueryRunes {
			mautrix.MInvalidParam.WithMessage("Type up to 64 characters to search.").Write(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), gifRPCTimeout)
		defer cancel()
		resp, err := tc.searchStickerSets(ctx, query)
		if err != nil {
			writeStickerSetError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	}
}

func stickerSetFeaturedHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		var req struct {
			Offset int `json:"offset"`
		}
		if !decodeGIFBody(w, r, &req) {
			return
		}
		if req.Offset < 0 || req.Offset > stickerSetMaxOffset {
			mautrix.MInvalidParam.WithMessage("Invalid offset").Write(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), gifRPCTimeout)
		defer cancel()
		resp, err := tc.featuredStickerSets(ctx, req.Offset)
		if err != nil {
			writeStickerSetError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	}
}

func stickerSetCoverHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		shortName, ok := decodeShortName(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), stickerSetCoverTimeout)
		defer cancel()
		resp, err := tc.stickerSetCover(ctx, shortName)
		if err != nil {
			writeStickerSetError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	}
}

func stickerSetInstallHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		shortName, ok := decodeShortName(w, r)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), gifRPCTimeout)
		defer cancel()
		archived, err := tc.installStickerSet(ctx, shortName)
		if err != nil {
			writeStickerSetError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, map[string]any{"installed": true, "archived_others": archived})
	}
}
