package connector

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundSendRejectsDifferentCapabilityAccountBeforeAnyRPC(t *testing.T) {
	for _, receiver := range []bool{false, true} {
		t.Run(map[bool]string{false: "capability", true: "receiver"}[receiver], func(t *testing.T) {
			container, raw := integrationStore(t, filepath.Join(t.TempDir(), "binding.db"))
			defer raw.Close()
			stub := &integrationSendRPC{}
			tc, portal := integrationSendClient(t, container, stub, nil)
			if receiver {
				portal.Receiver = "foreign-login"
			} else {
				portal.CapState.Source = "foreign-login"
			}
			response, err := tc.HandleMatrixMessage(context.Background(), integrationMessage(portal))
			require.Error(t, err)
			require.Nil(t, response)
			require.Zero(t, stub.permissionCalls)
			require.Zero(t, stub.uploads)
			require.Zero(t, stub.sends)
		})
	}
}
