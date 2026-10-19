package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"
)

func TestRoundReverseSyncRejectsRestrictedUnknownAndOrdinaryMetadata(t *testing.T) {
	for _, meta := range []*MessageMetadata{{}, {IsRound: true}, {IsRound: true, ProtectionKnown: true, NoForwards: true}, {IsRound: true, ProtectionKnown: true, TTLSeconds: 10}, {IsRound: true, ProtectionKnown: true, MediaExpired: true}, {IsRound: true, ProtectionKnown: true, MediaPolicy: &TelegramMediaPolicy{TransferAllowed: false}}} {
		if roundStateEligible(&database.Message{Metadata: meta}) {
			t.Fatalf("restricted state eligible: %+v", meta)
		}
	}
	if !roundStateEligible(&database.Message{Metadata: &MessageMetadata{IsRound: true, ProtectionKnown: true}}) {
		t.Fatal("ordinary unprotected round should receive state")
	}
}
