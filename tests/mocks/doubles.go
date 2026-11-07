package mocks

import (
	"context"
	"sync"

	"github.com/stretchr/testify/mock"

	"moderation_service/internal/authz"
	"moderation_service/internal/models"
)

// Authorizer is a testify/mock of the API-001 client.
type Authorizer struct {
	mock.Mock
}

var _ authz.Checker = (*Authorizer)(nil)

// Check records the call and returns the configured decision.
func (a *Authorizer) Check(ctx context.Context, communityID string, request authz.CheckRequest) (authz.Decision, error) {
	arguments := a.Called(ctx, communityID, request)
	decision, _ := arguments.Get(0).(authz.Decision)
	return decision, arguments.Error(1)
}

// Allow configures the double to permit authorization checks.
func (a *Authorizer) Allow() *Authorizer {
	a.On("Check", mock.Anything, mock.Anything, mock.Anything).Return(authz.Decision{Allowed: true}, nil)
	return a
}

// Deny configures the double to deny authorization checks with a reason.
func (a *Authorizer) Deny(reason string) *Authorizer {
	a.On("Check", mock.Anything, mock.Anything, mock.Anything).Return(authz.Decision{Allowed: false, DenialReason: reason}, nil)
	return a
}

// Unavailable configures the double to fail closed for authorization checks.
func (a *Authorizer) Unavailable() *Authorizer {
	a.On("Check", mock.Anything, mock.Anything, mock.Anything).Return(authz.Decision{}, models.ErrAuthorizationUnavailable)
	return a
}

// Published records one relayed event.
type Published struct {
	// Topic is the destination topic.
	Topic string
	// Key is the message key (community id).
	Key string
	// Value is the encoded message.
	Value []byte
	// Headers are the mirrored envelope headers.
	Headers map[string]string
}

// Producer is an in-memory events.Producer double.
type Producer struct {
	mu sync.Mutex
	// Events holds every published message in order.
	Events []Published
	// Err, when set, fails every publish.
	Err error
	// HealthyResult is returned by Healthy.
	HealthyResult bool
	// Closed reports whether Close was called.
	Closed bool
}

// Publish records the message or returns the configured error.
func (p *Producer) Publish(_ context.Context, topic, key string, value []byte, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	copied := make(map[string]string, len(headers))
	for name, headerValue := range headers {
		copied[name] = headerValue
	}
	p.Events = append(p.Events, Published{Topic: topic, Key: key, Value: value, Headers: copied})
	return nil
}

// Healthy reports the configured availability.
func (p *Producer) Healthy(context.Context) bool { return p.HealthyResult }

// Close records shutdown.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Closed = true
	return nil
}

// PublishCount returns how many messages were published.
func (p *Producer) PublishCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.Events)
}

// Last returns the most recently published message.
func (p *Producer) Last() (Published, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.Events) == 0 {
		return Published{}, false
	}
	return p.Events[len(p.Events)-1], true
}
