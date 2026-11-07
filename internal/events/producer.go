package events

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/IBM/sarama"
)

// Producer publishes one encoded event to a topic. The outbox dispatcher
// depends on this narrow port so tests can substitute a mock producer.
type Producer interface {
	// Publish sends value to topic with the given message key and headers.
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	// Healthy reports producer availability for the readiness probe.
	Healthy(ctx context.Context) bool
	// Close releases the producer and its client.
	Close() error
}

// KafkaProducer is the sarama-backed Producer with idempotence enabled, so a
// relay retry can never duplicate a message within a producer session.
type KafkaProducer struct {
	client   sarama.Client
	producer sarama.SyncProducer
}

var _ Producer = (*KafkaProducer)(nil)

// NewKafkaProducer connects an idempotent sync producer to the brokers.
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := kafkaConfig()

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("connect kafka client: %w", err)
	}
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &KafkaProducer{client: client, producer: producer}, nil
}

// kafkaConfig builds the producer configuration. Idempotence is mandatory: it is
// what makes a relay retry safe, and it requires acks=all with a single
// in-flight request per connection.
func kafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.ClientID = "moderation_service"
	config.Version = sarama.V2_1_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	// Idempotence requires one in-flight request per connection.
	config.Net.MaxOpenRequests = 1
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	return config
}

// Publish sends one event and waits for the broker acknowledgement.
func (p *KafkaProducer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: encodeHeaders(headers),
	}
	if _, _, err := p.producer.SendMessage(message); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// Healthy refreshes metadata for the service's topics to prove the broker is
// reachable, which is what /readyz needs to know.
func (p *KafkaProducer) Healthy(ctx context.Context) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	return p.client.RefreshMetadata() == nil
}

// Close shuts the producer down before its client.
func (p *KafkaProducer) Close() error {
	var firstErr error
	if p.producer != nil {
		if err := p.producer.Close(); err != nil {
			firstErr = err
		}
	}
	if p.client != nil {
		if err := p.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// encodeHeaders renders the envelope headers deterministically so a message's
// binary form is reproducible.
func encodeHeaders(headers map[string]string) []sarama.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	encoded := make([]sarama.RecordHeader, 0, len(names))
	for _, name := range names {
		encoded = append(encoded, sarama.RecordHeader{Key: []byte(name), Value: []byte(headers[name])})
	}
	return encoded
}
