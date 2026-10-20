// Copyright (C) 2026 mautrix-telegram contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package upgrades

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
)

// Before rebasing onto v26.10, the round fork used v10 and v11 for its own
// tables. Databases that reached those versions skipped upstream v10, so run
// its namespacing again. The query only touches plain numeric IDs in channel
// portals, which makes it a no-op where upstream v10 already ran.
var upgradeV13 = dbutil.WrapUpgrade(-1, 13, 2, "Reconcile round fork migration renumbering", dbutil.TxnModeOn, func(ctx context.Context, db *dbutil.Database) error {
	res, err := db.Exec(ctx, NamespaceMigratedMessagesQuery+` AND room_id LIKE 'channel:%'`, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		zerolog.Ctx(ctx).Info().Int64("affected", affected).Msg("Updated IDs of historical minigroup messages skipped by the round fork")
	}
	return nil
})
