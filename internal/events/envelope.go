// Package events owns the mandatory event envelope, the typed payloads of the
// three contracts this service produces, and the transactional-outbox relay to
// Kafka (ADR-003).
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// Registered event types and their versions.
const (
	// EventTypeMemberSanctionChanged is EVT-004, consumed by community_service
	// and notification_service.
	EventTypeMemberSanctionChanged = "member_sanction_changed"
	// EventTypeAutomodRulesChanged is EVT-005, consumed by messaging_service.
	EventTypeAutomodRulesChanged = "automod_rules_changed"
	// EventTypeAppealStatusChanged is EVT-006, consumed by notification_service.
	EventTypeAppealStatusChanged = "appeal_status_changed"

	// eventVersionV1 is the only version of each contract currently produced.
	eventVersionV1 = 1
)

// envelopeFields is the closed set of envelope keys. A payload must never
// declare one of them at its top level.
var envelopeFields = map[string]struct{}{
	"event_id":       {},
	"event_type":     {},
	"event_version":  {},
	"occurred_at":    {},
	"producer":       {},
	"trace_id":       {},
	"correlation_id": {},
	"causation_id":   {},
	"schema_ref":     {},
}

// Envelope is the mandatory wrapper every message carries. All nine fields are
// required; trace_id is never empty.
type Envelope struct {
	EventID       string  `json:"event_id"`
	EventType     string  `json:"event_type"`
	EventVersion  int     `json:"event_version"`
	OccurredAt    string  `json:"occurred_at"`
	Producer      string  `json:"producer"`
	TraceID       string  `json:"trace_id"`
	CorrelationID string  `json:"correlation_id"`
	CausationID   *string `json:"causation_id"`
	SchemaRef     string  `json:"schema_ref"`
}

// Message is the complete Kafka value: exactly two top-level keys.
type Message struct {
	Envelope Envelope `json:"envelope"`
	Payload  any      `json:"payload"`
}

// Meta carries the propagated identifiers of the originating flow.
type Meta struct {
	// TraceID is the propagated trace id; an empty value is minted so the
	// envelope can never carry "".
	TraceID string
	// CorrelationID groups every event of one business flow; an empty value
	// defaults to the event id of the first event.
	CorrelationID string
	// CausationID is the event id of the causing event, nil for the first
	// event of a flow.
	CausationID *string
}

// Builder constructs envelopes and payloads with injectable clock and id
// generation so tests are deterministic.
type Builder struct {
	// Now is the clock used for occurred_at.
	Now func() time.Time
	// NewID produces uuid strings for event ids.
	NewID func() string
}

// NewBuilder returns a Builder wired to the real clock and crypto/rand uuids.
func NewBuilder() *Builder {
	return &Builder{Now: time.Now, NewID: models.NewUUID}
}

// NewMessage builds a complete message for a registered contract.
func (b *Builder) NewMessage(meta Meta, eventType string, version int, payload any) (Message, error) {
	now := time.Now
	if b != nil && b.Now != nil {
		now = b.Now
	}
	newID := models.NewUUID
	if b != nil && b.NewID != nil {
		newID = b.NewID
	}

	eventID := newID()
	correlationID := meta.CorrelationID
	if correlationID == "" {
		correlationID = eventID
	}
	traceID := meta.TraceID
	if traceID == "" {
		// Scheduled jobs have no originating request; mint at the edge of the
		// flow so trace_id is never empty.
		traceID = observability.MintTraceID()
	}

	message := Message{
		Envelope: Envelope{
			EventID:       eventID,
			EventType:     eventType,
			EventVersion:  version,
			OccurredAt:    now().UTC().Format(time.RFC3339),
			Producer:      observability.ServiceName,
			TraceID:       traceID,
			CorrelationID: correlationID,
			CausationID:   meta.CausationID,
			SchemaRef:     fmt.Sprintf("EVENT_CONTRACTS.md#%s-v%d", eventType, version),
		},
		Payload: payload,
	}
	if err := message.Validate(); err != nil {
		return Message{}, err
	}
	return message, nil
}

// MemberSanctionChanged builds an EVT-004 message.
func (b *Builder) MemberSanctionChanged(meta Meta, payload MemberSanctionChangedPayload) (Message, error) {
	return b.NewMessage(meta, EventTypeMemberSanctionChanged, eventVersionV1, payload)
}

// AutomodRulesChanged builds an EVT-005 message with a full rule-set snapshot.
func (b *Builder) AutomodRulesChanged(meta Meta, payload AutomodRulesChangedPayload) (Message, error) {
	return b.NewMessage(meta, EventTypeAutomodRulesChanged, eventVersionV1, payload)
}

// AppealStatusChanged builds an EVT-006 message.
func (b *Builder) AppealStatusChanged(meta Meta, payload AppealStatusChangedPayload) (Message, error) {
	return b.NewMessage(meta, EventTypeAppealStatusChanged, eventVersionV1, payload)
}

// Validate checks the invariants every published message must satisfy: nine
// envelope fields, a non-empty hex trace id, and a payload that carries no
// envelope field name at its top level.
func (m Message) Validate() error {
	if m.Envelope.EventID == "" {
		return errors.New("events: envelope.event_id is required")
	}
	if m.Envelope.EventType == "" {
		return errors.New("events: envelope.event_type is required")
	}
	if m.Envelope.EventVersion < 1 {
		return errors.New("events: envelope.event_version must be >= 1")
	}
	if _, err := time.Parse(time.RFC3339, m.Envelope.OccurredAt); err != nil {
		return fmt.Errorf("events: envelope.occurred_at: %w", err)
	}
	if m.Envelope.Producer == "" {
		return errors.New("events: envelope.producer is required")
	}
	if !models.IsHexID(m.Envelope.TraceID, 32) {
		return errors.New("events: envelope.trace_id must be a non-empty 32-character hex id")
	}
	if m.Envelope.CorrelationID == "" {
		return errors.New("events: envelope.correlation_id is required")
	}
	if m.Envelope.SchemaRef == "" {
		return errors.New("events: envelope.schema_ref is required")
	}
	if m.Payload == nil {
		return errors.New("events: payload is required")
	}
	encoded, err := json.Marshal(m.Payload)
	if err != nil {
		return fmt.Errorf("events: enumerate payload fields: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return fmt.Errorf("events: payload must be a JSON object: %w", err)
	}
	for name := range fields {
		if _, collides := envelopeFields[name]; collides {
			return fmt.Errorf("events: payload must not declare envelope field %q", name)
		}
	}
	return nil
}

// OutboxRow renders the message as an outbox_event row keyed by community id,
// so the relay partitions per community and preserves per-community ordering.
func (m Message) OutboxRow(topic, messageKey string, createdAt time.Time) (*models.OutboxEvent, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	envelope, err := json.Marshal(m.Envelope)
	if err != nil {
		return nil, fmt.Errorf("events: encode envelope: %w", err)
	}
	payload, err := json.Marshal(m.Payload)
	if err != nil {
		return nil, fmt.Errorf("events: encode payload: %w", err)
	}
	return &models.OutboxEvent{
		ID:         m.Envelope.EventID,
		Topic:      topic,
		MessageKey: messageKey,
		Envelope:   envelope,
		Payload:    payload,
		CreatedAt:  createdAt.UTC(),
	}, nil
}

// KafkaValue renders the wire value: {"envelope": {...}, "payload": {...}}.
func (m Message) KafkaValue() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("events: encode message: %w", err)
	}
	return encoded, nil
}

// Headers mirrors envelope.trace_id and envelope.event_type into Kafka message
// headers, as required by the platform delivery contract.
func (m Message) Headers() map[string]string {
	return map[string]string{
		"trace_id":   m.Envelope.TraceID,
		"event_type": m.Envelope.EventType,
	}
}
