package events_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/events"
	"moderation_service/internal/models"
)

// fixedClock is a deterministic clock for envelope assertions.
func fixedClock() time.Time { return time.Date(2026, 1, 14, 10, 30, 0, 0, time.UTC) }

// deterministicBuilder returns a builder with fixed clock and event ids.
func deterministicBuilder() *events.Builder {
	counter := 0
	return &events.Builder{
		Now: fixedClock,
		NewID: func() string {
			counter++
			return "00000000-0000-4000-8000-00000000000" + string(rune('0'+counter))
		},
	}
}

// sanctionPayload is a minimal valid EVT-004 payload.
func sanctionPayload() events.MemberSanctionChangedPayload {
	return events.MemberSanctionChangedPayload{
		SanctionID:   "11111111-1111-4111-8111-111111111111",
		CommunityID:  "22222222-2222-4222-8222-222222222222",
		TargetUserID: "33333333-3333-4333-8333-333333333333",
		Action:       "mute",
		Source:       models.SourceModerator,
		Appealable:   true,
	}
}

// TestEnvelopeCarriesAllNineFields pins the mandatory envelope: nine fields, a
// parseable timestamp, the registered schema reference, and a nullable
// causation id for a flow's first event.
func TestEnvelopeCarriesAllNineFields(t *testing.T) {
	t.Parallel()

	message, err := deterministicBuilder().MemberSanctionChanged(events.Meta{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
	}, sanctionPayload())
	require.NoError(t, err)

	encoded, err := json.Marshal(message.Envelope)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))

	expected := []string{
		"event_id", "event_type", "event_version", "occurred_at", "producer",
		"trace_id", "correlation_id", "causation_id", "schema_ref",
	}
	assert.Len(t, fields, len(expected))
	for _, name := range expected {
		assert.Contains(t, fields, name)
	}

	assert.Equal(t, events.EventTypeMemberSanctionChanged, message.Envelope.EventType)
	assert.Equal(t, 1, message.Envelope.EventVersion)
	assert.Equal(t, "moderation_service", message.Envelope.Producer)
	assert.Equal(t, "EVENT_CONTRACTS.md#member_sanction_changed-v1", message.Envelope.SchemaRef)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", message.Envelope.TraceID)
	assert.Nil(t, message.Envelope.CausationID)
	assert.Equal(t, message.Envelope.EventID, message.Envelope.CorrelationID,
		"the first event of a flow correlates with itself")

	occurredAt, err := time.Parse(time.RFC3339, message.Envelope.OccurredAt)
	require.NoError(t, err)
	assert.True(t, occurredAt.Equal(fixedClock()))
}

// TestEnvelopeMintsTraceIDWhenAbsent covers scheduled jobs: they have no
// originating request, so the builder mints a 32-hex trace id.
func TestEnvelopeMintsTraceIDWhenAbsent(t *testing.T) {
	t.Parallel()

	message, err := deterministicBuilder().AppealStatusChanged(events.Meta{}, events.AppealStatusChangedPayload{
		AppealID:        "44444444-4444-4444-8444-444444444444",
		SanctionID:      "11111111-1111-4111-8111-111111111111",
		CommunityID:     "22222222-2222-4222-8222-222222222222",
		AppellantUserID: "33333333-3333-4333-8333-333333333333",
		SanctionType:    models.SanctionTypeMute,
		Status:          models.AppealStatusOverturned,
	})
	require.NoError(t, err)

	assert.NotEmpty(t, message.Envelope.TraceID)
	assert.Len(t, message.Envelope.TraceID, 32)
	assert.True(t, models.IsHexID(message.Envelope.TraceID, 32))
}

// TestEnvelopePropagatesCausation covers the chained-event case.
func TestEnvelopePropagatesCausation(t *testing.T) {
	t.Parallel()

	cause := "55555555-5555-4555-8555-555555555555"
	message, err := deterministicBuilder().AutomodRulesChanged(events.Meta{
		TraceID:       "0af7651916cd43dd8448eb211c80319c",
		CorrelationID: "66666666-6666-4666-8666-666666666666",
		CausationID:   &cause,
	}, events.AutomodRulesChangedPayload{
		CommunityID:    "22222222-2222-4222-8222-222222222222",
		RuleSetVersion: 3,
		Spam:           models.AutomodSpamRules{Action: models.AutomodActionBlock, MaxMessagesPerWindow: 5, WindowSeconds: 10, MaxDuplicateMessages: 2, MaxMentionsPerMessage: 3},
		BannedWords:    models.AutomodBannedWords{Action: models.AutomodActionRemove, MatchMode: models.AutomodMatchWholeWord, Words: []string{"spam"}},
		ExemptRoleIDs:  []string{},
	})
	require.NoError(t, err)

	require.NotNil(t, message.Envelope.CausationID)
	assert.Equal(t, cause, *message.Envelope.CausationID)
	assert.Equal(t, "66666666-6666-4666-8666-666666666666", message.Envelope.CorrelationID)
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", message.Envelope.TraceID)
}

// envelopeLeakingPayload is an invalid payload: it redeclares envelope fields.
type envelopeLeakingPayload struct {
	TraceID string `json:"trace_id"`
	EventID string `json:"event_id"`
}

// TestValidateRejectsEnvelopeFieldsInPayload enforces the contract rule that a
// payload must not carry an envelope field name at its top level.
func TestValidateRejectsEnvelopeFieldsInPayload(t *testing.T) {
	t.Parallel()

	_, err := deterministicBuilder().NewMessage(events.Meta{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
	}, events.EventTypeMemberSanctionChanged, 1, envelopeLeakingPayload{TraceID: "x", EventID: "y"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "envelope field")
}

// TestValidateRejectsBrokenEnvelopes enumerates the invariants that must never
// reach the wire.
func TestValidateRejectsBrokenEnvelopes(t *testing.T) {
	t.Parallel()

	valid := func() events.Message {
		message, err := deterministicBuilder().MemberSanctionChanged(events.Meta{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		}, sanctionPayload())
		require.NoError(t, err)
		return message
	}

	tests := []struct {
		name   string
		mutate func(*events.Message)
	}{
		{name: "missing event id", mutate: func(m *events.Message) { m.Envelope.EventID = "" }},
		{name: "missing event type", mutate: func(m *events.Message) { m.Envelope.EventType = "" }},
		{name: "zero version", mutate: func(m *events.Message) { m.Envelope.EventVersion = 0 }},
		{name: "unparseable occurred_at", mutate: func(m *events.Message) { m.Envelope.OccurredAt = "yesterday" }},
		{name: "missing producer", mutate: func(m *events.Message) { m.Envelope.Producer = "" }},
		{name: "empty trace id", mutate: func(m *events.Message) { m.Envelope.TraceID = "" }},
		{name: "non hex trace id", mutate: func(m *events.Message) { m.Envelope.TraceID = "zzzz" }},
		{name: "missing correlation id", mutate: func(m *events.Message) { m.Envelope.CorrelationID = "" }},
		{name: "missing schema ref", mutate: func(m *events.Message) { m.Envelope.SchemaRef = "" }},
		{name: "missing payload", mutate: func(m *events.Message) { m.Payload = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			message := valid()
			test.mutate(&message)
			require.Error(t, message.Validate())
		})
	}
}

// TestOutboxRowKeepsTwoTopLevelKeys proves the stored row reproduces the wire
// shape: exactly "envelope" and "payload" at the top level, keyed by community.
func TestOutboxRowKeepsTwoTopLevelKeys(t *testing.T) {
	t.Parallel()

	payload := sanctionPayload()
	message, err := deterministicBuilder().MemberSanctionChanged(events.Meta{
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
	}, payload)
	require.NoError(t, err)

	row, err := message.OutboxRow("moderation_events", payload.CommunityID, fixedClock())
	require.NoError(t, err)

	assert.Equal(t, message.Envelope.EventID, row.ID, "outbox id equals envelope.event_id")
	assert.Equal(t, "moderation_events", row.Topic)
	assert.Equal(t, payload.CommunityID, row.MessageKey)
	require.NotNil(t, row.CreatedAt)

	value, err := message.KafkaValue()
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(value, &decoded))
	assert.Len(t, decoded, 2)
	assert.Contains(t, decoded, "envelope")
	assert.Contains(t, decoded, "payload")

	headers := message.Headers()
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", headers["trace_id"])
	assert.Equal(t, events.EventTypeMemberSanctionChanged, headers["event_type"])
}
