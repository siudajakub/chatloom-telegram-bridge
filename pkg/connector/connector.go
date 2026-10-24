// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2024 Sumner Evans
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"

	"go.mau.fi/mautrix-telegram/pkg/connector/store"
)

type TelegramConnector struct {
	Bridge *bridgev2.Bridge
	Config TelegramConfig
	Store  *store.Container

	useDirectMedia       bool
	maxFileSize          int64
	mediaControlsEnabled bool

	// Wave 5 (ChatLoom fork) runtime switches, read from the environment
	// because bbctl regenerates config.yaml.
	protectionStaleOnError bool
	roundTempDir           string

	// Wave 5b: the @gif search provisioning routes are added once.
	gifRoutesOnce sync.Once
}

var _ bridgev2.NetworkConnector = (*TelegramConnector)(nil)
var _ bridgev2.MaxFileSizeingNetwork = (*TelegramConnector)(nil)

func (tc *TelegramConnector) Init(bridge *bridgev2.Bridge) {
	tc.Store = store.NewStore(bridge.DB.Database, dbutil.ZeroLogger(bridge.Log.With().Str("db_section", "telegram").Logger()))
	tc.Bridge = bridge
	tc.protectionStaleOnError = envFlag(protectionStaleOnErrorEnv, true)
	tc.roundTempDir = roundTempDirFromEnv()
	tc.RegisterMediaControlHandlers()
	tc.Bridge.Commands.(*commands.Processor).AddHandlers(cmdSyncChats, cmdEmojiPack, cmdUpgrade, cmdJoin)
}

func (tc *TelegramConnector) Start(ctx context.Context) error {
	if err := tc.Store.Upgrade(ctx); err != nil {
		return err
	}
	tc.startupMaintenance(ctx)
	tc.registerGIFRoutes(ctx)
	return nil
}

// startupMaintenance does the fork's one-off clean-up at start (wave 5 F10,
// F11): crash leftovers of round preparation and old ledger rows. Failures
// are logged and never block the start.
func (tc *TelegramConnector) startupMaintenance(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	if removed := sweepRoundTempFiles(tc.roundTempDir, time.Hour, time.Now()); removed > 0 {
		log.Info().Int("removed", removed).Msg("Removed stale round video temporary files")
	}
	contents, operations, err := store.NewRoundLedger(tc.Store.Database).PruneFinished(ctx, time.Now().Add(-store.RoundLedgerRetention))
	if err != nil {
		log.Warn().Err(err).Msg("Failed to prune old round ledger rows")
	} else if contents > 0 || operations > 0 {
		log.Info().Int64("contents_read_rows", contents).Int64("operation_rows", operations).Msg("Pruned old round ledger rows")
	}
}

func (tc *TelegramConnector) PreSplitPortalMigrate(ctx context.Context) error {
	return tc.Store.Upgrade(ctx)
}

func (tc *TelegramConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) (err error) {
	login.Client, err = NewTelegramClient(ctx, tc, login)
	return
}

func (tc *TelegramConnector) SetMaxFileSize(maxSize int64) {
	tc.maxFileSize = maxSize
}

func (tc *TelegramConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Telegram",
		NetworkURL:           "https://telegram.org/",
		NetworkIcon:          "mxc://maunium.net/tJCRmUyJDsgRNgqhOgoiHWbX",
		NetworkID:            "telegram",
		BeeperBridgeType:     "telegram",
		DefaultPort:          29317,
		DefaultCommandPrefix: "!tg",
	}
}
