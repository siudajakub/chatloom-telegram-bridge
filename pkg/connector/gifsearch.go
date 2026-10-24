// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2026 The Personal Chat contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

// GIF search through Telegram's @gif inline bot (ChatLoom wave 5b, ST-10 D3).
//
// Two provisioning routes, registered on the bridgev2 provisioning router, so
// they have the same authentication as `/v3/image_pack/*` (shared secret or
// the user's Matrix token, `login_id` optional):
//
//	POST /_matrix/provision/v3/gif/search  {"query": "cats", "offset": ""}
//	POST /_matrix/provision/v3/gif/media   {"id": "<result id>", "video": true}
//
// Search is a POST so the query never appears in the request URI that the
// provisioning access log writes; nothing here logs a query or a result.
// `search` resolves @gif once per login (cached), calls
// `messages.getInlineBotResults` with the user's own login and `inputPeerSelf`
// as the chat (the bot does not learn which chat is open), and answers a
// bounded, compact list with a ~1 KB inline preview (the expanded stripped
// thumbnail). `media` only serves documents that a recent search of the same
// login returned: it uploads the static thumbnail and, on request, the mp4 to
// the Matrix media repository as the bridge bot (unencrypted public GIF media,
// cached by Telegram location ID like every bridged file) and answers mxc URIs
// plus a ready `m.video` + `fi.mau.gif` info block. The app sends the GIF as a
// normal Matrix event, so it works for every bridged network; a Telegram chat
// receives it as a native GIF (DocumentAttributeAnimated).

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-telegram/pkg/connector/media"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram/thumbnail"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tgerr"
)

const (
	// gifBotUsername is Telegram's GIF inline bot (`gif_search_username`).
	gifBotUsername = "gif"
	// gifMaxQueryRunes bounds the query (Telegram's inline query limit is
	// 256 characters).
	gifMaxQueryRunes  = 256
	gifMaxOffsetBytes = 128
	// gifMaxResults bounds one answer (the bot pages by about 50).
	gifMaxResults = 40
	// gifSearchCacheSize/TTL: short in-memory cache per normalised query and
	// offset (Telegram's own `cache_time` shortens it).
	gifSearchCacheSize = 64
	gifSearchCacheTTL  = 10 * time.Minute
	// gifDocCacheSize/TTL: documents a search returned, the only ones the
	// media route serves (file references expire after a while anyway).
	gifDocCacheSize = 1000
	gifDocTTL       = time.Hour
	// gifMaxBytes: larger "GIFs" are refused (Telegram's are far smaller).
	gifMaxBytes = 20 * 1024 * 1024
	// gifMediaSlots: concurrent media transfers per login.
	gifMediaSlots = 3
	gifRPCTimeout = 15 * time.Second
	// gifMediaTimeout covers getFile and the upload of one mp4.
	gifMediaTimeout = 60 * time.Second
	gifMaxBodyBytes = 4 * 1024
)

var (
	errGIFNotLoggedIn = mautrix.RespError{ErrCode: "FI.MAU.NOT_LOGGED_IN", Err: "Not logged in", StatusCode: http.StatusBadRequest}
	errGIFUnknown     = mautrix.MNotFound.WithMessage("This GIF is no longer available. Search again.")
	errGIFTooLarge    = mautrix.MTooLarge.WithMessage("This GIF is too large to send.")
	errGIFBot         = mautrix.MUnknown.WithMessage("Telegram's GIF search isn't available right now.").WithStatus(http.StatusBadGateway)
	errGIFTelegram    = mautrix.MUnknown.WithMessage("Telegram didn't answer. Try again.").WithStatus(http.StatusBadGateway)
)

// gifLoggedIn is the login check of the routes (tests replace it).
var gifLoggedIn = (*TelegramClient).IsLoggedIn

// gifSearchResult is one GIF of a search answer.
type gifSearchResult struct {
	ID         string `json:"id"`
	MimeType   string `json:"mimetype"`
	Width      int    `json:"w,omitempty"`
	Height     int    `json:"h,omitempty"`
	DurationMS int    `json:"duration,omitempty"`
	Size       int64  `json:"size,omitempty"`
	// Thumb is the expanded stripped thumbnail (a tiny JPEG, base64 in JSON)
	// for an instant blurred preview; absent when Telegram sent none.
	Thumb []byte `json:"thumb,omitempty"`
}

type gifSearchResponse struct {
	Results    []gifSearchResult `json:"results"`
	NextOffset string            `json:"next_offset,omitempty"`
}

type gifMediaResponse struct {
	ThumbnailURL  id.ContentURIString `json:"thumbnail_url,omitempty"`
	ThumbnailInfo map[string]any      `json:"thumbnail_info,omitempty"`
	URL           id.ContentURIString `json:"url,omitempty"`
	// Info is the complete `info` of the `m.video` event to send.
	Info map[string]any `json:"info,omitempty"`
}

// gifTransferFunc uploads a document (thumb nil) or one of its thumbnails
// to Matrix media and returns the mxc URI with the file info.
type gifTransferFunc func(ctx context.Context, doc *tg.Document, thumb tg.PhotoSizeClass) (id.ContentURIString, *event.FileInfo, error)

type gifDocEntry struct {
	doc *tg.Document
	at  time.Time
}

type gifSearchEntry struct {
	resp    *gifSearchResponse
	expires time.Time
}

// gifSearchState is the per-login state of GIF search.
type gifSearchState struct {
	mu       sync.Mutex
	searches *boundedCache[string, gifSearchEntry]
	docs     *boundedCache[int64, gifDocEntry]
	// Deadlines after a FLOOD_WAIT: no RPC of that kind until they pass.
	searchBlockedUntil time.Time
	mediaBlockedUntil  time.Time

	botMu sync.Mutex
	bot   *tg.InputUser

	slots    chan struct{}
	now      func() time.Time
	transfer gifTransferFunc
}

func (tc *TelegramClient) gifState() *gifSearchState {
	tc.gifsOnce.Do(func() {
		tc.gifs = &gifSearchState{
			searches: newBoundedCache[string, gifSearchEntry](gifSearchCacheSize),
			docs:     newBoundedCache[int64, gifDocEntry](gifDocCacheSize),
			slots:    make(chan struct{}, gifMediaSlots),
			now:      time.Now,
			transfer: tc.transferGIFMedia,
		}
	})
	return tc.gifs
}

// gifCacheKey trims and lower-cases the query for the cache key; the
// query sent to Telegram keeps its case.
func gifCacheKey(query, offset string) string {
	return strings.ToLower(strings.Join(strings.Fields(query), " ")) + "\x00" + offset
}

func gifFloodError(wait time.Duration) mautrix.RespError {
	ms := wait.Milliseconds()
	if ms < 1000 {
		ms = 1000
	}
	return mautrix.MLimitExceeded.
		WithMessage("Telegram asked to slow down. Try again in a moment.").
		WithExtraField("retry_after_ms", ms)
}

// blockedFor returns the remaining FLOOD_WAIT of one kind, or zero.
func (s *gifSearchState) blockedFor(media bool, now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	until := s.searchBlockedUntil
	if media {
		until = s.mediaBlockedUntil
	}
	if now.Before(until) {
		return until.Sub(now)
	}
	return 0
}

// rpcError turns a Telegram error into a route error and records FLOOD_WAIT.
func (s *gifSearchState) rpcError(err error, media bool, now time.Time) error {
	if wait, ok := tgerr.AsFloodWait(err); ok {
		s.mu.Lock()
		if media {
			s.mediaBlockedUntil = now.Add(wait)
		} else {
			s.searchBlockedUntil = now.Add(wait)
		}
		s.mu.Unlock()
		return gifFloodError(wait)
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return errGIFTelegram.WithMessage("Telegram didn't answer in time. Try again.").WithStatus(http.StatusGatewayTimeout)
	}
	return gifUpstreamError{err}
}

// gifUpstreamError is a Telegram failure: the route answers a sentence and
// logs the error (never the query), the client never sees Telegram's text.
type gifUpstreamError struct{ err error }

func (e gifUpstreamError) Error() string { return e.err.Error() }
func (e gifUpstreamError) Unwrap() error { return e.err }

// gifBot resolves @gif once per login.
func (tc *TelegramClient) gifBot(ctx context.Context, s *gifSearchState) (*tg.InputUser, error) {
	s.botMu.Lock()
	defer s.botMu.Unlock()
	if s.bot != nil {
		return s.bot, nil
	}
	resolved, err := tc.client.API().ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: gifBotUsername})
	if err != nil {
		if tgerr.Is(err, tg.ErrUsernameNotOccupied, tg.ErrUsernameInvalid) {
			return nil, errGIFBot
		}
		return nil, s.rpcError(err, false, s.now())
	}
	peer, ok := resolved.Peer.(*tg.PeerUser)
	if !ok {
		return nil, errGIFBot
	}
	for _, raw := range resolved.Users {
		if user, ok := raw.(*tg.User); ok && user.ID == peer.UserID && user.Bot {
			s.bot = &tg.InputUser{UserID: user.ID, AccessHash: user.AccessHash}
			return s.bot, nil
		}
	}
	return nil, errGIFBot
}

func (s *gifSearchState) forgetBot() {
	s.botMu.Lock()
	s.bot = nil
	s.botMu.Unlock()
}

// searchGIFs answers one page of @gif results for query.
func (tc *TelegramClient) searchGIFs(ctx context.Context, query, offset string) (*gifSearchResponse, error) {
	s := tc.gifState()
	now := s.now()
	key := gifCacheKey(query, offset)
	s.mu.Lock()
	cached, ok := s.searches.get(key)
	if ok && now.After(cached.expires) {
		s.searches.remove(key)
		ok = false
	}
	s.mu.Unlock()
	if ok {
		// Refresh the documents: the media route must know them.
		s.rememberDocs(cached.resp, nil, now)
		return cached.resp, nil
	}
	if wait := s.blockedFor(false, now); wait > 0 {
		return nil, gifFloodError(wait)
	}
	bot, err := tc.gifBot(ctx, s)
	if err != nil {
		return nil, err
	}
	res, err := tc.client.API().MessagesGetInlineBotResults(ctx, &tg.MessagesGetInlineBotResultsRequest{
		Bot:    bot,
		Peer:   &tg.InputPeerSelf{},
		Query:  query,
		Offset: offset,
	})
	if err != nil {
		if tgerr.Is(err, tg.ErrBotInvalid, tg.ErrUserBotInvalid, tg.ErrBotInlineDisabled) {
			s.forgetBot()
			return nil, errGIFBot
		}
		return nil, s.rpcError(err, false, s.now())
	}
	resp, docs := convertGIFResults(res)
	s.rememberDocs(resp, docs, now)
	ttl := gifSearchCacheTTL
	if cacheTime := time.Duration(res.CacheTime) * time.Second; cacheTime > 0 && cacheTime < ttl {
		ttl = cacheTime
	}
	s.mu.Lock()
	s.searches.put(key, gifSearchEntry{resp: resp, expires: now.Add(ttl)})
	s.mu.Unlock()
	return resp, nil
}

// rememberDocs stores (docs non-nil) or refreshes the documents of resp.
func (s *gifSearchState) rememberDocs(resp *gifSearchResponse, docs []*tg.Document, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, doc := range docs {
		s.docs.put(doc.ID, gifDocEntry{doc: doc, at: now})
	}
	if docs != nil {
		return
	}
	for _, result := range resp.Results {
		docID, err := strconv.ParseInt(result.ID, 10, 64)
		if err != nil {
			continue
		}
		if entry, ok := s.docs.get(docID); ok {
			entry.at = now
			s.docs.put(docID, entry)
		}
	}
}

func gifVideoAttribute(doc *tg.Document) *tg.DocumentAttributeVideo {
	for _, attr := range doc.Attributes {
		if video, ok := attr.(*tg.DocumentAttributeVideo); ok {
			return video
		}
	}
	return nil
}

func gifImageSize(doc *tg.Document) (int, int) {
	if video := gifVideoAttribute(doc); video != nil {
		return video.W, video.H
	}
	for _, attr := range doc.Attributes {
		if size, ok := attr.(*tg.DocumentAttributeImageSize); ok {
			return size.W, size.H
		}
	}
	return 0, 0
}

// isGIFDocument accepts mp4 animations (what @gif returns) and real GIFs.
func isGIFDocument(doc *tg.Document) bool {
	switch doc.MimeType {
	case "video/mp4", "image/gif":
		return doc.Size > 0 && doc.Size <= gifMaxBytes
	default:
		return false
	}
}

// convertGIFResults keeps media results with a GIF document (web results
// would need a third-party download and are skipped), bounded to
// gifMaxResults.
func convertGIFResults(res *tg.MessagesBotResults) (*gifSearchResponse, []*tg.Document) {
	resp := &gifSearchResponse{Results: make([]gifSearchResult, 0, min(len(res.Results), gifMaxResults))}
	docs := make([]*tg.Document, 0, cap(resp.Results))
	seen := make(map[int64]struct{}, cap(resp.Results))
	for _, raw := range res.Results {
		if len(resp.Results) >= gifMaxResults {
			break
		}
		result, ok := raw.(*tg.BotInlineMediaResult)
		if !ok {
			continue
		}
		doc, ok := result.Document.(*tg.Document)
		if !ok || !isGIFDocument(doc) {
			continue
		}
		if _, dup := seen[doc.ID]; dup {
			continue
		}
		seen[doc.ID] = struct{}{}
		w, h := gifImageSize(doc)
		item := gifSearchResult{
			ID:       strconv.FormatInt(doc.ID, 10),
			MimeType: doc.MimeType,
			Width:    w,
			Height:   h,
			Size:     doc.Size,
		}
		if video := gifVideoAttribute(doc); video != nil {
			item.DurationMS = int(video.Duration * 1000)
		}
		for _, thumb := range doc.Thumbs {
			if stripped, ok := thumb.(*tg.PhotoStrippedSize); ok {
				if jpeg, err := thumbnail.Expand(stripped.Bytes); err == nil {
					item.Thumb = jpeg
				}
				break
			}
		}
		resp.Results = append(resp.Results, item)
		docs = append(docs, doc)
	}
	resp.NextOffset = res.NextOffset
	return resp, docs
}

// gifThumbnail picks the largest downloadable thumbnail (never the inline
// stripped or path sizes), or nil.
func gifThumbnail(doc *tg.Document) (tg.PhotoSizeClass, int, int) {
	var best tg.PhotoSizeClass
	var bestW, bestH, bestArea int
	for _, raw := range doc.Thumbs {
		var w, h int
		switch size := raw.(type) {
		case *tg.PhotoSize:
			w, h = size.W, size.H
		case *tg.PhotoSizeProgressive:
			w, h = size.W, size.H
		default:
			continue
		}
		if area := w * h; best == nil || area > bestArea {
			best, bestW, bestH, bestArea = raw, w, h, area
		}
	}
	return best, bestW, bestH
}

// transferGIFMedia is the production gifTransferFunc: the bridge's normal
// Telegram → Matrix transfer (unencrypted upload as the bridge bot, cached
// by location ID, so a GIF is uploaded once).
func (tc *TelegramClient) transferGIFMedia(ctx context.Context, doc *tg.Document, thumb tg.PhotoSizeClass) (id.ContentURIString, *event.FileInfo, error) {
	xfer := media.NewTransferer(tc.client.API())
	var ready *media.ReadyTransferer
	if thumb == nil {
		if video := gifVideoAttribute(doc); video != nil {
			xfer = xfer.WithVideo(video)
		}
		ready = xfer.WithFilename("gif.mp4").WithDocument(doc, false)
	} else {
		// WithDocument picks the largest thumbnail: offer only the chosen one.
		only := *doc
		only.Thumbs = []tg.PhotoSizeClass{thumb}
		ready = xfer.WithFilename("gif.jpg").WithMIMEType("image/jpeg").WithDocument(&only, true)
	}
	mxc, _, info, err := ready.Transfer(ctx, tc.main.Store, tc.main.Bridge.Bot)
	return mxc, info, err
}

// gifMedia uploads (or finds) the thumbnail and, when video is set, the mp4
// of a GIF a recent search returned.
func (tc *TelegramClient) gifMedia(ctx context.Context, rawID string, video bool) (*gifMediaResponse, error) {
	s := tc.gifState()
	docID, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		return nil, errGIFUnknown
	}
	now := s.now()
	s.mu.Lock()
	entry, ok := s.docs.get(docID)
	if ok && now.Sub(entry.at) > gifDocTTL {
		s.docs.remove(docID)
		ok = false
	}
	s.mu.Unlock()
	if !ok {
		return nil, errGIFUnknown
	}
	doc := entry.doc
	if doc.Size > gifMaxBytes || (tc.main != nil && tc.main.maxFileSize > 0 && doc.Size > tc.main.maxFileSize) {
		return nil, errGIFTooLarge
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
	resp := &gifMediaResponse{}
	if thumb, w, h := gifThumbnail(doc); thumb != nil {
		mxc, info, err := s.transfer(ctx, doc, thumb)
		if err == nil && mxc != "" {
			resp.ThumbnailURL = mxc
			resp.ThumbnailInfo = map[string]any{"mimetype": "image/jpeg", "w": w, "h": h}
			if info != nil {
				if info.MimeType != "" {
					resp.ThumbnailInfo["mimetype"] = info.MimeType
				}
				if info.Size > 0 {
					resp.ThumbnailInfo["size"] = info.Size
				}
			}
		} else if err != nil {
			if mapped, fatal := s.mediaError(docID, err); fatal || !video {
				return nil, mapped
			}
			// The thumbnail is optional when the GIF itself is wanted.
			zerolog.Ctx(ctx).Debug().Err(err).Msg("GIF thumbnail transfer failed")
		}
	}
	if !video {
		return resp, nil
	}
	mxc, info, err := s.transfer(ctx, doc, nil)
	if err != nil {
		mapped, _ := s.mediaError(docID, err)
		return nil, mapped
	}
	if mxc == "" {
		return nil, errGIFTelegram
	}
	resp.URL = mxc
	resp.Info = gifVideoInfo(doc, info, resp)
	return resp, nil
}

// mediaError maps a transfer error; fatal errors also stop a thumbnail-only
// fallback (flood waits, unknown documents).
func (s *gifSearchState) mediaError(docID int64, err error) (error, bool) {
	if tgerr.Is(err, tg.ErrFileReferenceExpired, tg.ErrFileReferenceInvalid, tg.ErrFileIDInvalid) {
		s.mu.Lock()
		s.docs.remove(docID)
		s.mu.Unlock()
		return errGIFUnknown, true
	}
	if errors.Is(err, bridgev2.ErrMediaTooLarge) {
		return errGIFTooLarge, true
	}
	if _, flood := tgerr.AsFloodWait(err); flood {
		return s.rpcError(err, true, s.now()), true
	}
	return s.rpcError(err, true, s.now()), false
}

// gifVideoInfo is the `info` of the m.video event the app sends, with the
// same flags the bridge gives incoming Telegram GIFs (tomatrix.go).
func gifVideoInfo(doc *tg.Document, info *event.FileInfo, resp *gifMediaResponse) map[string]any {
	out := map[string]any{
		"mimetype":             "video/mp4",
		"size":                 doc.Size,
		"fi.mau.gif":           true,
		"fi.mau.loop":          true,
		"fi.mau.autoplay":      true,
		"fi.mau.hide_controls": true,
		"fi.mau.no_audio":      true,
	}
	if w, h := gifImageSize(doc); w > 0 && h > 0 {
		out["w"], out["h"] = w, h
	}
	if video := gifVideoAttribute(doc); video != nil && video.Duration > 0 {
		out["duration"] = int(video.Duration * 1000)
	}
	if info != nil {
		if info.MimeType != "" {
			out["mimetype"] = info.MimeType
		}
		if info.Size > 0 {
			out["size"] = info.Size
		}
	}
	if resp.ThumbnailURL != "" {
		out["thumbnail_url"] = resp.ThumbnailURL
		out["thumbnail_info"] = resp.ThumbnailInfo
	}
	return out
}

// --- HTTP routes ----------------------------------------------------------

type gifLoginResolver func(w http.ResponseWriter, r *http.Request) *bridgev2.UserLogin

// gifProvisioning is the part of the bridgev2 provisioning API the routes
// need (implemented by `bridgev2/matrix.ProvisioningAPI`).
type gifProvisioning interface {
	GetRouter() *http.ServeMux
	GetLoginForRequest(w http.ResponseWriter, r *http.Request) *bridgev2.UserLogin
}

// registerGIFRoutes adds the GIF routes to the provisioning router (after
// the Matrix connector started, so the router exists).
func (tc *TelegramConnector) registerGIFRoutes(ctx context.Context) {
	if tc.Bridge == nil {
		return
	}
	withProv, ok := tc.Bridge.Matrix.(bridgev2.MatrixConnectorWithProvisioning)
	if !ok {
		return
	}
	prov, ok := withProv.GetProvisioning().(gifProvisioning)
	if !ok {
		zerolog.Ctx(ctx).Warn().Msg("Provisioning API can't resolve logins, GIF search routes not registered")
		return
	}
	router := prov.GetRouter()
	if router == nil {
		return
	}
	tc.gifRoutesOnce.Do(func() {
		addGIFRoutes(router, prov.GetLoginForRequest)
	})
}

func addGIFRoutes(router *http.ServeMux, getLogin gifLoginResolver) {
	router.HandleFunc("POST /v3/gif/search", gifSearchHandler(getLogin))
	router.HandleFunc("POST /v3/gif/media", gifMediaHandler(getLogin))
}

func gifClientForRequest(w http.ResponseWriter, r *http.Request, getLogin gifLoginResolver) *TelegramClient {
	login := getLogin(w, r)
	if login == nil {
		return nil
	}
	tc, ok := login.Client.(*TelegramClient)
	if !ok || !gifLoggedIn(tc) {
		errGIFNotLoggedIn.Write(w)
		return nil
	}
	return tc
}

func decodeGIFBody(w http.ResponseWriter, r *http.Request, into any) bool {
	err := json.NewDecoder(io.LimitReader(r.Body, gifMaxBodyBytes)).Decode(into)
	if err != nil {
		mautrix.MNotJSON.WithMessage("Failed to decode request body").Write(w)
		return false
	}
	return true
}

func writeGIFError(w http.ResponseWriter, r *http.Request, err error) {
	var respErr mautrix.RespError
	if errors.As(err, &respErr) {
		respErr.Write(w)
		return
	}
	zerolog.Ctx(r.Context()).Warn().Err(err).Msg("GIF search request failed")
	errGIFTelegram.Write(w)
}

func gifSearchHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		var req struct {
			Query  string `json:"query"`
			Offset string `json:"offset"`
		}
		if !decodeGIFBody(w, r, &req) {
			return
		}
		query := strings.TrimSpace(req.Query)
		if query == "" || !utf8.ValidString(query) || utf8.RuneCountInString(query) > gifMaxQueryRunes {
			mautrix.MInvalidParam.WithMessage("Type up to 256 characters to search.").Write(w)
			return
		}
		if len(req.Offset) > gifMaxOffsetBytes {
			mautrix.MInvalidParam.WithMessage("Invalid offset").Write(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), gifRPCTimeout)
		defer cancel()
		resp, err := tc.searchGIFs(ctx, query, req.Offset)
		if err != nil {
			writeGIFError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	}
}

func gifMediaHandler(getLogin gifLoginResolver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tc := gifClientForRequest(w, r, getLogin)
		if tc == nil {
			return
		}
		var req struct {
			ID    string `json:"id"`
			Video bool   `json:"video"`
		}
		if !decodeGIFBody(w, r, &req) {
			return
		}
		if req.ID == "" || len(req.ID) > 32 {
			mautrix.MInvalidParam.WithMessage("Missing GIF").Write(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), gifMediaTimeout)
		defer cancel()
		resp, err := tc.gifMedia(ctx, req.ID, req.Video)
		if err != nil {
			writeGIFError(w, r, err)
			return
		}
		exhttp.WriteJSONResponse(w, http.StatusOK, resp)
	}
}

// --- bounded LRU ----------------------------------------------------------

// boundedCache is a small LRU (not safe for concurrent use; callers lock).
type boundedCache[K comparable, V any] struct {
	limit int
	order *list.List
	items map[K]*list.Element
}

type boundedCacheItem[K comparable, V any] struct {
	key   K
	value V
}

func newBoundedCache[K comparable, V any](limit int) *boundedCache[K, V] {
	return &boundedCache[K, V]{limit: limit, order: list.New(), items: make(map[K]*list.Element, limit)}
}

func (c *boundedCache[K, V]) get(key K) (V, bool) {
	if el, ok := c.items[key]; ok {
		c.order.MoveToFront(el)
		return el.Value.(*boundedCacheItem[K, V]).value, true
	}
	var zero V
	return zero, false
}

func (c *boundedCache[K, V]) put(key K, value V) {
	if el, ok := c.items[key]; ok {
		el.Value.(*boundedCacheItem[K, V]).value = value
		c.order.MoveToFront(el)
		return
	}
	c.items[key] = c.order.PushFront(&boundedCacheItem[K, V]{key: key, value: value})
	for c.order.Len() > c.limit {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*boundedCacheItem[K, V]).key)
	}
}

func (c *boundedCache[K, V]) remove(key K) {
	if el, ok := c.items[key]; ok {
		c.order.Remove(el)
		delete(c.items, key)
	}
}

func (c *boundedCache[K, V]) len() int { return c.order.Len() }
