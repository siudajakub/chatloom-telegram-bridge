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

func TestRoundRuntimeLimitExpiresRevalidatesAndFailsClosed(t *testing.T) {
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
	var typed *RoundMediaError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, "upload_limit_unknown", typed.Code)
	require.Zero(t, limit)
}
