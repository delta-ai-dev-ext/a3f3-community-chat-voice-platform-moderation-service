package events

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/tests/mocks"
)

// TestNewBuilderUsesProductionDefaults proves the shared builder mints unique
// event ids and stamps the current time.
func TestNewBuilderUsesProductionDefaults(t *testing.T) {
	t.Parallel()

	builder := NewBuilder()
	require.NotNil(t, builder.Now)
	require.NotNil(t, builder.NewID)

	assert.True(t, models.IsUUID(builder.NewID()))
	before := time.Now().Add(-time.Second)
	assert.True(t, builder.Now().After(before))
}

// TestKafkaConfigIsIdempotent pins the producer settings the delivery contract
// depends on, without needing a live broker.
func TestKafkaConfigIsIdempotent(t *testing.T) {
	t.Parallel()

	config := kafkaConfig()

	assert.True(t, config.Producer.Idempotent, "the relay must publish idempotently")
	assert.Equal(t, int16(-1), int16(config.Producer.RequiredAcks), "acks must be WaitForAll")
	assert.Equal(t, 1, config.Net.MaxOpenRequests, "idempotence requires one in-flight request")
	assert.True(t, config.Producer.Return.Successes, "the sync producer needs successes")
	assert.Equal(t, "moderation_service", config.ClientID)
	require.NoError(t, config.Validate())
}

// TestKafkaValueRejectsInvalidMessages proves nothing unvalidated reaches the
// wire.
func TestKafkaValueRejectsInvalidMessages(t *testing.T) {
	t.Parallel()

	invalid := Message{
		Envelope: Envelope{EventType: EventTypeMemberSanctionChanged, EventVersion: 1},
		Payload:  map[string]any{"sanction_id": "x"},
	}

	_, err := invalid.KafkaValue()
	require.Error(t, err)

	_, err = invalid.OutboxRow("moderation_events", "key", time.Now())
	require.Error(t, err)
}

// TestHeadersMirrorsEnvelope proves the Kafka headers carry the trace id and the
// event type even when reconstructed from a stored row.
func TestHeadersMirrorsEnvelope(t *testing.T) {
	t.Parallel()

	message, err := NewBuilder().MemberSanctionChanged(Meta{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736"},
		MemberSanctionChangedPayload{Action: "ban", Source: models.SourceModerator})
	require.NoError(t, err)

	row, err := message.OutboxRow("moderation_events", "22222222-2222-4222-8222-222222222222", time.Now())
	require.NoError(t, err)

	headers, err := Headers(*row)
	require.NoError(t, err)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", headers["trace_id"])
	assert.Equal(t, EventTypeMemberSanctionChanged, headers["event_type"])

	broken := *row
	broken.Envelope = []byte(`not-json`)
	_, err = Headers(broken)
	require.Error(t, err)
}

// TestDispatchSkipsCorruptRows proves a row with an undecodable envelope stops
// the batch instead of publishing a partial message.
func TestDispatchSkipsCorruptRows(t *testing.T) {
	t.Parallel()

	store := mocks.NewStore()
	require.NoError(t, store.Outbox.Append(context.Background(), &models.OutboxEvent{
		ID:         "11111111-1111-4111-8111-111111111111",
		Topic:      "moderation_events",
		MessageKey: "22222222-2222-4222-8222-222222222222",
		Envelope:   []byte(`{"event_id":`),
		Payload:    []byte(`{}`),
		CreatedAt:  time.Now(),
	}))

	producer := &mocks.Producer{}
	dispatcher := NewDispatcher(DispatcherConfig{Store: store, Producer: producer})

	published, err := dispatcher.DispatchOnce(context.Background())

	require.Error(t, err)
	assert.Zero(t, published)
	assert.Zero(t, producer.PublishCount())
	assert.Len(t, store.Outbox.Unpublished(), 1, "a corrupt row is not acknowledged")
}

// TestRenderPreservesStoredEnvelope proves the relayed value keeps the stored
// envelope bytes unchanged and adds only the payload.
func TestRenderPreservesStoredEnvelope(t *testing.T) {
	t.Parallel()

	message, err := NewBuilder().AppealStatusChanged(Meta{TraceID: observability.MintTraceID()},
		AppealStatusChangedPayload{
			AppealID:        "33333333-3333-4333-8333-333333333333",
			SanctionID:      "44444444-4444-4444-8444-444444444444",
			CommunityID:     "55555555-5555-4555-8555-555555555555",
			AppellantUserID: "66666666-6666-4666-8666-666666666666",
			SanctionType:    models.SanctionTypeBan,
			Status:          models.AppealStatusSubmitted,
		})
	require.NoError(t, err)

	row, err := message.OutboxRow("moderation_events", "55555555-5555-4555-8555-555555555555", time.Now())
	require.NoError(t, err)

	value, headers, err := render(*row)
	require.NoError(t, err)
	assert.Equal(t, message.Envelope.TraceID, headers["trace_id"])

	var decoded struct {
		Envelope Envelope        `json:"envelope"`
		Payload  json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(value, &decoded))
	assert.Equal(t, message.Envelope.EventID, decoded.Envelope.EventID)
	assert.Equal(t, EventTypeAppealStatusChanged, decoded.Envelope.EventType)
	assert.Contains(t, string(decoded.Payload), "appeal_id")
}

// TestDefaultDispatcherConfig proves the zero-value config is usable, so the
// wiring in main can omit optional fields.
func TestDefaultDispatcherConfig(t *testing.T) {
	t.Parallel()

	dispatcher := NewDispatcher(DispatcherConfig{Store: mocks.NewStore(), Producer: &mocks.Producer{}})

	assert.Equal(t, DefaultDispatchInterval, dispatcher.interval)
	assert.Equal(t, DefaultDispatchBatch, dispatcher.batchSize)
	assert.Equal(t, DefaultOutboxPruneGrace, dispatcher.pruneGrace)
	assert.NotNil(t, dispatcher.clock)
	assert.NotNil(t, dispatcher.logger)
}
