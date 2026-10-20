package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"
)

func openRoundLedgerTestDB(t *testing.T, path string) (*RoundLedger, *sql.DB) {
	t.Helper()
	raw, err := sql.Open("sqlite3", path+"?_busy_timeout=5000")
	require.NoError(t, err)
	raw.SetMaxOpenConns(1)
	db, err := dbutil.NewWithDB(raw, "sqlite3")
	require.NoError(t, err)
	// Upstream store v10 updates the bridgev2 message table, which exists in real deployments.
	_, err = raw.Exec("CREATE TABLE IF NOT EXISTS message (id TEXT NOT NULL, room_id TEXT NOT NULL)")
	require.NoError(t, err)
	c := NewStore(db, dbutil.NoopLogger)
	require.NoError(t, c.Upgrade(context.Background()))
	return NewRoundLedger(c.Database), raw
}

func syntheticRoundOperation() *RoundOperation {
	return &RoundOperation{RoundOperationKey: RoundOperationKey{BridgeID: "synthetic-bridge", LoginID: "synthetic-login", PortalID: "user:777", PortalReceiver: "synthetic-login", Action: "forward", RequestID: "synthetic-request"}, EventID: "$synthetic-control", MediaIdentity: "$synthetic-source", PayloadHash: "synthetic-sha256"}
}

func TestRoundLedgerRestartRetryAndChangedPayload(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "ledger.db")
	ledger, raw := openRoundLedgerTestDB(t, path)
	first, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	require.NotZero(t, first.RandomID)
	claimed, err := ledger.Claim(ctx, first, time.Now(), time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, raw.Close()) // crash after durable claim, before response
	ledger, raw = openRoundLedgerTestDB(t, path)
	defer raw.Close()
	retry, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	require.Equal(t, first.RandomID, retry.RandomID)
	require.Equal(t, "dispatching", retry.Status)
	claimed, err = ledger.Claim(ctx, retry, time.Now(), time.Minute)
	require.NoError(t, err)
	require.False(t, claimed)
	claimed, err = ledger.Claim(ctx, retry, time.Now().Add(2*time.Minute), time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.ErrorIs(t, ledger.Finish(ctx, first, "accepted", "stale result", ""), ErrRoundDispatchBusy)
	require.NoError(t, ledger.Finish(ctx, retry, "accepted", "synthetic result", ""))
	accepted, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	require.Equal(t, "accepted", accepted.Status)
	require.Equal(t, "synthetic result", accepted.Result)
	claimed, err = ledger.Claim(ctx, accepted, time.Now().Add(10*time.Minute), time.Minute)
	require.NoError(t, err)
	require.False(t, claimed)
	changed := syntheticRoundOperation()
	changed.PayloadHash = "changed-sha256"
	_, err = ledger.Reserve(ctx, changed)
	require.ErrorIs(t, err, ErrRoundIdentityChanged)
	changed = syntheticRoundOperation()
	changed.RequestID = "changed-request"
	_, err = ledger.Reserve(ctx, changed)
	require.ErrorIs(t, err, ErrRoundIdentityChanged)
}

func TestRoundLedgerConcurrentDispatchClaimAndUncertainRetry(t *testing.T) {
	ctx := context.Background()
	ledger, raw := openRoundLedgerTestDB(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	op, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	var claims atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copy := *op
			won, err := ledger.Claim(ctx, &copy, time.Now(), time.Minute)
			if err != nil {
				t.Error(err)
			}
			if won {
				claims.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, claims.Load())
	stored, err := ledger.Get(ctx, op.RoundOperationKey)
	require.NoError(t, err)
	require.NoError(t, ledger.Finish(ctx, stored, "uncertain", "", "ack_lost"))
	retry, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	require.Equal(t, op.RandomID, retry.RandomID)
	won, err := ledger.Claim(ctx, retry, time.Now(), time.Minute)
	require.NoError(t, err)
	require.True(t, won)
}

func TestRoundMediaStateRevisionSurvivesRestartAndSeparatesAccounts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	ledger, raw := openRoundLedgerTestDB(t, path)
	unread := false
	state := &RoundMediaState{BridgeID: "synthetic-bridge", LoginID: "synthetic-a", PortalID: "channel:111", EventID: "$synthetic-media", MediaUnread: &unread, PlayedAtMS: 1000}
	first, err := ledger.PutMediaState(ctx, state)
	require.NoError(t, err)
	require.EqualValues(t, 1, first.Revision)
	require.NoError(t, raw.Close())
	ledger, raw = openRoundLedgerTestDB(t, path)
	defer raw.Close()
	second, err := ledger.PutMediaState(ctx, state)
	require.NoError(t, err)
	require.EqualValues(t, 2, second.Revision)
	loaded, err := ledger.GetMediaState(ctx, state.BridgeID, state.LoginID, state.PortalID, state.PortalReceiver, state.EventID)
	require.NoError(t, err)
	require.NotNil(t, loaded.MediaUnread)
	require.False(t, *loaded.MediaUnread)
	require.EqualValues(t, 1000, loaded.PlayedAtMS)
	state.LoginID = "synthetic-b"
	other, err := ledger.PutMediaState(ctx, state)
	require.NoError(t, err)
	require.EqualValues(t, 1, other.Revision)
}

func TestRoundLedgerClaimTokenRejectsSameMillisecondStaleFinisher(t *testing.T) {
	ctx := context.Background()
	ledger, raw := openRoundLedgerTestDB(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	first, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	now := time.Now().Truncate(time.Millisecond)
	claimed, err := ledger.Claim(ctx, first, now, time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, ledger.Finish(ctx, first, "uncertain", "", "ack_lost"))
	retry, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	claimed, err = ledger.Claim(ctx, retry, now, time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, first.LeaseUntilMS, retry.LeaseUntilMS)
	require.NotEqual(t, first.ClaimToken, retry.ClaimToken)
	require.Equal(t, first.RandomID, retry.RandomID)
	require.ErrorIs(t, ledger.Finish(ctx, first, "accepted", "stale response", ""), ErrRoundDispatchBusy)
	require.NoError(t, ledger.Finish(ctx, retry, "accepted", "current response", ""))
}

func TestRoundMediaStateStaleAckCannotResurrectUnreadOrExpired(t *testing.T) {
	ctx := context.Background()
	ledger, raw := openRoundLedgerTestDB(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	unread := true
	initial := &RoundMediaState{BridgeID: "synthetic-bridge", LoginID: "synthetic-login", PortalID: "channel:555", EventID: "$synthetic-round", MediaUnread: &unread}
	_, err := ledger.PutMediaState(ctx, initial)
	require.NoError(t, err)
	staleAck, err := ledger.GetMediaState(ctx, initial.BridgeID, initial.LoginID, initial.PortalID, initial.PortalReceiver, initial.EventID)
	require.NoError(t, err)
	played := false
	current := *initial
	current.MediaUnread = &played
	current.Expired = true
	current.PlayedAtMS = 1000
	_, err = ledger.PutMediaState(ctx, &current)
	require.NoError(t, err)
	require.True(t, *staleAck.MediaUnread)
	merged, err := ledger.PutMediaState(ctx, staleAck)
	require.NoError(t, err)
	require.EqualValues(t, 3, merged.Revision)
	require.False(t, *merged.MediaUnread)
	require.True(t, merged.Expired)
	require.EqualValues(t, 1000, merged.PlayedAtMS)
}

func TestRoundLedgerDispatchRecordSurvivesUncertainFinish(t *testing.T) {
	ctx := context.Background()
	ledger, raw := openRoundLedgerTestDB(t, filepath.Join(t.TempDir(), "ledger.db"))
	defer raw.Close()
	op, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	// Only the current claim holder can record a dispatch.
	require.ErrorIs(t, ledger.RecordDispatch(ctx, op, `{"kind":"synthetic"}`), ErrRoundDispatchBusy)
	claimed, err := ledger.Claim(ctx, op, time.Now(), time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)
	stale := *op
	stale.ClaimToken = "stale-token"
	require.ErrorIs(t, ledger.RecordDispatch(ctx, &stale, `{"kind":"stale"}`), ErrRoundDispatchBusy)
	require.NoError(t, ledger.RecordDispatch(ctx, op, `{"kind":"synthetic"}`))
	require.Equal(t, `{"kind":"synthetic"}`, op.Result)
	stored, err := ledger.Get(ctx, op.RoundOperationKey)
	require.NoError(t, err)
	require.Equal(t, "dispatching", stored.Status)
	require.Equal(t, `{"kind":"synthetic"}`, stored.Result)
	require.NoError(t, ledger.Finish(ctx, op, "uncertain", op.Result, "transport_unavailable"))
	retry, err := ledger.Reserve(ctx, syntheticRoundOperation())
	require.NoError(t, err)
	require.Equal(t, "uncertain", retry.Status)
	require.Equal(t, `{"kind":"synthetic"}`, retry.Result)
	require.Equal(t, op.RandomID, retry.RandomID)
}
