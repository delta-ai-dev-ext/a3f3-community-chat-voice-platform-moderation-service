package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Label values with a fixed domain, kept as constants so no call site can
// accidentally introduce a high-cardinality value.
const (
	// ProviderCommunityService is the API-001 provider label.
	ProviderCommunityService = "community_service"
	// OperationAuthorizationChecks is the API-001 operation label.
	OperationAuthorizationChecks = "authorization-checks"
)

// Metrics holds every business and technical metric this service exports.
// Labels are deliberately low-cardinality: never user, community, sanction, or
// appeal ids.
type Metrics struct {
	// HTTPRequests counts completed requests per route, method, status class.
	HTTPRequests *prometheus.CounterVec
	// HTTPRequestDuration observes request latency per route and method.
	HTTPRequestDuration *prometheus.HistogramVec
	// Sanctions counts issued or expired sanctions by type and source.
	Sanctions *prometheus.CounterVec
	// Appeals counts appeal status transitions.
	Appeals *prometheus.CounterVec
	// AppealDecisionSeconds measures submission-to-decision latency.
	AppealDecisionSeconds prometheus.Histogram
	// AutomodConfigUpdates counts stored automod rule-set versions.
	AutomodConfigUpdates prometheus.Counter
	// AuditEntriesPurged counts audit entries removed by the retention job.
	AuditEntriesPurged prometheus.Counter
	// RetentionLastSuccess is the unix timestamp of the last successful purge.
	RetentionLastSuccess prometheus.Gauge
	// ExternalCallDuration observes outbound API-001 latency.
	ExternalCallDuration *prometheus.HistogramVec
	// ExternalCallErrors counts outbound API-001 failures by error class.
	ExternalCallErrors *prometheus.CounterVec
	// KafkaProducerErrors counts failed event relays per topic.
	KafkaProducerErrors *prometheus.CounterVec
}

// NewMetrics builds and registers every metric on reg exactly once, together
// with the Go runtime collectors used by the default scrape endpoint.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of completed HTTP requests.",
		}, []string{"route", "method", "status_class"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		Sanctions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moderation_sanctions_total",
			Help: "Sanctions issued, lifted, or expired by type and source.",
		}, []string{"type", "source"}),
		Appeals: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moderation_appeals_total",
			Help: "Appeal status transitions.",
		}, []string{"status"}),
		AppealDecisionSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "moderation_appeal_decision_seconds",
			Help:    "Seconds between appeal submission and decision.",
			Buckets: []float64{60, 300, 900, 3600, 21600, 86400, 259200, 604800},
		}),
		AutomodConfigUpdates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "moderation_automod_config_updates_total",
			Help: "Automod rule-set versions stored.",
		}),
		AuditEntriesPurged: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "moderation_audit_entries_purged_total",
			Help: "Audit log entries deleted by the retention job.",
		}),
		RetentionLastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "moderation_retention_job_last_success_timestamp",
			Help: "Unix timestamp of the last successful audit retention purge.",
		}),
		ExternalCallDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "external_call_duration_seconds",
			Help:    "Outbound dependency call latency in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2},
		}, []string{"provider", "operation"}),
		ExternalCallErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "external_call_errors_total",
			Help: "Outbound dependency call failures by error class.",
		}, []string{"provider", "operation", "error_class"}),
		KafkaProducerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_producer_errors_total",
			Help: "Failed Kafka relay attempts per topic.",
		}, []string{"topic"}),
	}

	collectorsToRegister := []prometheus.Collector{
		m.HTTPRequests,
		m.HTTPRequestDuration,
		m.Sanctions,
		m.Appeals,
		m.AppealDecisionSeconds,
		m.AutomodConfigUpdates,
		m.AuditEntriesPurged,
		m.RetentionLastSuccess,
		m.ExternalCallDuration,
		m.ExternalCallErrors,
		m.KafkaProducerErrors,
	}
	for _, collector := range collectorsToRegister {
		if err := reg.Register(collector); err != nil {
			return nil, err
		}
	}
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, err
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, err
	}
	return m, nil
}

// RecordSanction increments the sanction counter. type and source are always
// drawn from a closed enum set by the caller.
func (m *Metrics) RecordSanction(sanctionType, source string) {
	if m == nil {
		return
	}
	m.Sanctions.WithLabelValues(sanctionType, source).Inc()
}

// RecordAppeal increments the appeal transition counter.
func (m *Metrics) RecordAppeal(status string) {
	if m == nil {
		return
	}
	m.Appeals.WithLabelValues(status).Inc()
}

// RecordAutomodUpdate counts one stored automod rule-set version.
func (m *Metrics) RecordAutomodUpdate() {
	if m == nil {
		return
	}
	m.AutomodConfigUpdates.Inc()
}

// RecordAuditPurged adds the number of entries deleted by one purge batch.
func (m *Metrics) RecordAuditPurged(count int64) {
	if m == nil || count <= 0 {
		return
	}
	m.AuditEntriesPurged.Add(float64(count))
}

// RecordRetentionSuccess stamps the last-successful-purge gauge.
func (m *Metrics) RecordRetentionSuccess(unixSeconds float64) {
	if m == nil {
		return
	}
	m.RetentionLastSuccess.Set(unixSeconds)
}

// RecordExternalCallError counts an outbound API-001 failure.
func (m *Metrics) RecordExternalCallError(errorClass string) {
	if m == nil {
		return
	}
	m.ExternalCallErrors.WithLabelValues(ProviderCommunityService, OperationAuthorizationChecks, errorClass).Inc()
}

// RecordKafkaError counts a failed event relay attempt for a topic.
func (m *Metrics) RecordKafkaError(topic string) {
	if m == nil {
		return
	}
	m.KafkaProducerErrors.WithLabelValues(topic).Inc()
}

// ObserveExternalCall records the latency of one outbound API-001 attempt.
func (m *Metrics) ObserveExternalCall(seconds float64) {
	if m == nil {
		return
	}
	m.ExternalCallDuration.WithLabelValues(ProviderCommunityService, OperationAuthorizationChecks).Observe(seconds)
}
