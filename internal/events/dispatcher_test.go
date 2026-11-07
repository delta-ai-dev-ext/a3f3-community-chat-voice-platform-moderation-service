package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/tests/mocks"
)

// dispatcherFixture wires a dispatcher over the in-memory store and producer.
type dispatcherFixture struct {
	store      *mocks.Store
	producer   *mocks.Producer
	metrics    *observability.Metrics
	dispatcher *events.Dispatcher
}

// newDispatcherFixture seeds count unpublished events for one community.
func newDispatcherFixture(t *testing.T, count int) *dispatcherFixture {
	t.Helper()

	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	require.NoError(t, err)

	store := mocks.NewStore()
	producer := &mocks.Producer{}
	builder := deterministicBuilder()
	for index := 0; index < count; index++ {
		message, err := builder.MemberSanctionChanged(events.Meta{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
		}, sanctionPayload())
		require.NoError(t, err)
		row, err := message.OutboxRow("moderation_events", sanctionPayload().CommunityID,
			fixedClock().Add(time.Duration(index)*time.Second))
		require.NoError(t, err)
		require.NoError(t, store.Outbox.Append(context.Background(), row))
	}

	return &dispatcherFixture{
		store:    store,
		producer: producer,
		metrics:  metrics,
		dispatcher: events.NewDispatcher(events.DispatcherConfig{
			Store:    store,
			Producer: producer,
			Metrics:  metrics,
			Clock:    fixedClock,
		}),
	}
}

// TestDispatchOnceRelaysAndAcknowledges proves each unpublished row is
// published with the envelope headers and then marked published.
func TestDispatchOnceRelaysAndAcknowledges(t *testing.T) {
	t.Parallel()

	fixture := newDispatcherFixture(t, 2)

	published, err := fixture.dispatcher.DispatchOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, published)
	assert.Equal(t, 2, fixture.producer.PublishCount())
	assert.Empty(t, fixture.store.Outbox.Unpublished(), "every relayed row must be acknowledged")

	first := fixture.producer.Events[0]
	assert.Equal(t, "moderation_events", first.Topic)
	assert.Equal(t, sanctionPayload().CommunityID, first.Key)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", first.Headers["trace_id"])
	assert.Equal(t, events.EventTypeMemberSanctionChanged, first.Headers["event_type"])

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(first.Value, &decoded))
	assert.Len(t, decoded, 2)
	assert.Contains(t, decoded, "envelope")
	assert.Contains(t, decoded, "payload")
}

// TestDispatchOnceHoldsBatchOnProducerFailure proves a failed publish leaves the
// row unpublished (at-least-once), preserves ordering, and is counted.
func TestDispatchOnceHoldsBatchOnProducerFailure(t *testing.T) {
	t.Parallel()

	fixture := newDispatcherFixture(t, 2)
	fixture.producer.Err = errors.New("broker unavailable")

	published, err := fixture.dispatcher.DispatchOnce(context.Background())
	require.Error(t, err)
	assert.Zero(t, published)
	assert.Len(t, fixture.store.Outbox.Unpublished(), 2, "nothing may be acknowledged")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		fixture.metrics.KafkaProducerErrors.WithLabelValues("moderation_events"),
	))
}

// TestDispatchOnceWithNoWorkIsANoop keeps the poll loop cheap when idle.
func TestDispatchOnceWithNoWorkIsANoop(t *testing.T) {
	t.Parallel()

	fixture := newDispatcherFixture(t, 0)

	published, err := fixture.dispatcher.DispatchOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, published)
	assert.Zero(t, fixture.producer.PublishCount())
}

// TestRunStopsOnContextCancellation proves the loop honours the shutdown budget
// instead of blocking forever.
func TestRunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	fixture := newDispatcherFixture(t, 1)
	dispatcher := events.NewDispatcher(events.DispatcherConfig{
		Store:    fixture.store,
		Producer: fixture.producer,
		Interval: 5 * time.Millisecond,
		Clock:    fixedClock,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		dispatcher.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return fixture.producer.PublishCount() == 1 },
		2*time.Second, 5*time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not stop after cancellation")
	}
}

// TestDispatchPrunesPublishedRows keeps the outbox bounded.
func TestDispatchPrunesPublishedRows(t *testing.T) {
	t.Parallel()

	fixture := newDispatcherFixture(t, 1)
	old := fixedClock().Add(-48 * time.Hour)
	require.NoError(t, fixture.store.Outbox.Append(context.Background(), &models.OutboxEvent{
		ID:          "77777777-7777-4777-8777-777777777777",
		Topic:       "moderation_events",
		MessageKey:  "22222222-2222-4222-8222-222222222222",
		Envelope:    []byte(`{}`),
		Payload:     []byte(`{}`),
		CreatedAt:   old,
		PublishedAt: &old,
	}))

	_, err := fixture.dispatcher.DispatchOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, fixture.store.Outbox.Len(), "the published grace-window row is pruned")
}
