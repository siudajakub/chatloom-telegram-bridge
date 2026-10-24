package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mau.fi/mautrix-telegram/pkg/gotd/bin"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"go.mau.fi/mautrix-telegram/pkg/gotd/tg"
)

// TF-01: an expired app config is refreshed, a failed refresh keeps serving
// the previous config, and only the very first fetch can fail.
func TestRoundRuntimeLimitServesStaleOnRefreshError(t *testing.T) {
	calls, fail := 0, false
	client := telegram.NewClient(1, "synthetic-not-a-secret", telegram.Options{Middlewares: []telegram.Middleware{
		telegram.MiddlewareFunc(func(_ tg.Invoker) telegram.InvokeFunc {
			return func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				if _, ok := input.(*tg.HelpGetAppConfigRequest); !ok {
					return errors.New("unexpected runtime-limit RPC")
				}
				calls++
				if fail {
					return context.DeadlineExceeded
				}
				b := &bin.Buffer{}
				if err := (&tg.HelpAppConfigNotModified{}).Encode(b); err != nil {
					return err
				}
				return output.Decode(b)
			}
		}),
	}})
	tc := &TelegramClient{main: &TelegramConnector{maxFileSize: 16 * 1024 * 1024}, metadata: &UserLoginMetadata{}, client: client, appConfig: map[string]any{"upload_max_fileparts_default": float64(4000)}, appConfigFetched: time.Now().Add(-6 * time.Minute)}
	limit, err := tc.roundUploadLimit(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 16*1024*1024, limit)
	require.Equal(t, 1, calls)
	_, err = tc.roundUploadLimit(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	tc.appConfigFetched = time.Now().Add(-6 * time.Minute)
	fail = true
	limit, err = tc.roundUploadLimit(context.Background())
	require.NoError(t, err, "a failed refresh keeps the cached config")
	require.EqualValues(t, 16*1024*1024, limit)
	require.Equal(t, 2, calls)
	// Retries are spaced: no RPC storm while Telegram is failing.
	_, err = tc.roundUploadLimit(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

func TestAppConfigFirstFetchFailureFailsClosed(t *testing.T) {
	tc := &TelegramClient{main: &TelegramConnector{maxFileSize: 16 * 1024 * 1024}, metadata: &UserLoginMetadata{}}
	tc.fetchAppConfig = func(context.Context, int) (tg.HelpAppConfigClass, error) {
		return nil, context.DeadlineExceeded
	}
	limit, err := tc.roundUploadLimit(context.Background())
	var typed *RoundMediaError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "upload_limit_unknown", typed.Code)
	require.Zero(t, limit)
}

func TestAppConfigRefreshOutsideLock(t *testing.T) {
	tc, portal, _ := capabilityFixture(t)
	tc.appConfigFetched = time.Now().Add(-6 * time.Minute)
	started, release := make(chan struct{}), make(chan struct{})
	tc.fetchAppConfig = func(ctx context.Context, _ int) (tg.HelpAppConfigClass, error) {
		close(started)
		<-release
		return &tg.HelpAppConfigNotModified{}, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = tc.getAppConfigCached(context.Background())
	}()
	<-started
	start := time.Now()
	tc.roundAvailability(context.Background(), portal)
	config, err := tc.getAppConfigCached(context.Background())
	require.NoError(t, err)
	require.NotNil(t, config, "other callers get the stale config while one refreshes")
	require.Less(t, time.Since(start), 100*time.Millisecond)
	close(release)
	<-done
}
