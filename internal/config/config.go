// Package config loads and validates the service configuration from the
// environment. Configuration is 12-factor: nothing is read from files at
// runtime except the optional local .env convenience file.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"go.uber.org/zap/zapcore"
)

// Default values applied when the corresponding variable is unset.
const (
	DefaultServicePort          = 8080
	DefaultKafkaTopic           = "moderation_events"
	DefaultShutdownTimeout      = 20 // seconds; platform-wide drain budget
	DefaultAuthorizationTimeout = 2  // seconds; API-001 budget per attempt
	DefaultLogLevel             = "info"
)

// Config is the fully validated runtime configuration.
type Config struct {
	// DatabaseURL is the PostgreSQL connection string used by pgx and by the
	// migration runner.
	DatabaseURL string
	// KafkaBrokers is the comma-separated broker list for the producer.
	KafkaBrokers []string
	// KafkaTopicModerationEvents is the topic every event is published to.
	KafkaTopicModerationEvents string
	// CommunityServiceURL is the base URL of community_service (API-001).
	CommunityServiceURL string
	// CommunityServiceToken is the per-edge service token sent to API-001.
	CommunityServiceToken string
	// IdentityJWKSKeys holds identity_service public verification keys.
	IdentityJWKSKeys string
	// IdentityTokenIssuer is the expected "iss" claim.
	IdentityTokenIssuer string
	// IdentityTokenAudience is the expected "aud" claim.
	IdentityTokenAudience string
	// ServicePort is the HTTP listen port.
	ServicePort int
	// LogLevel is the zap level for the JSON logger.
	LogLevel zapcore.Level
	// ShutdownTimeoutSeconds bounds the graceful drain on SIGTERM/SIGINT.
	ShutdownTimeoutSeconds int
}

// Load reads and validates configuration from the process environment. It
// returns an error naming every missing or invalid variable so startup fails
// fast and loudly rather than degrading silently.
func Load() (*Config, error) {
	// Local development convenience only; production supplies real env vars.
	_ = godotenv.Load()

	cfg := &Config{
		DatabaseURL:                strings.TrimSpace(os.Getenv("DATABASE_URL")),
		KafkaTopicModerationEvents: strings.TrimSpace(os.Getenv("KAFKA_TOPIC_MODERATION_EVENTS")),
		CommunityServiceURL:        strings.TrimSpace(os.Getenv("COMMUNITY_SERVICE_URL")),
		CommunityServiceToken:      strings.TrimSpace(os.Getenv("COMMUNITY_SERVICE_TOKEN")),
		IdentityJWKSKeys:           strings.TrimSpace(os.Getenv("IDENTITY_JWKS_KEYS")),
		IdentityTokenIssuer:        strings.TrimSpace(os.Getenv("IDENTITY_TOKEN_ISSUER")),
		IdentityTokenAudience:      strings.TrimSpace(os.Getenv("IDENTITY_TOKEN_AUDIENCE")),
	}

	if brokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS")); brokers != "" {
		for _, broker := range strings.Split(brokers, ",") {
			if trimmed := strings.TrimSpace(broker); trimmed != "" {
				cfg.KafkaBrokers = append(cfg.KafkaBrokers, trimmed)
			}
		}
	}

	if cfg.KafkaTopicModerationEvents == "" {
		cfg.KafkaTopicModerationEvents = DefaultKafkaTopic
	}

	var problems []string
	if cfg.DatabaseURL == "" {
		problems = append(problems, "DATABASE_URL is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		problems = append(problems, "KAFKA_BROKERS is required")
	}
	if cfg.CommunityServiceURL == "" {
		problems = append(problems, "COMMUNITY_SERVICE_URL is required")
	}
	if cfg.CommunityServiceToken == "" {
		problems = append(problems, "COMMUNITY_SERVICE_TOKEN is required")
	}
	if cfg.IdentityJWKSKeys == "" {
		problems = append(problems, "IDENTITY_JWKS_KEYS is required")
	}
	if cfg.IdentityTokenIssuer == "" {
		problems = append(problems, "IDENTITY_TOKEN_ISSUER is required")
	}
	if cfg.IdentityTokenAudience == "" {
		problems = append(problems, "IDENTITY_TOKEN_AUDIENCE is required")
	}

	port, err := intEnv("SERVICE_PORT", DefaultServicePort)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.ServicePort = port

	shutdown, err := intEnv("SHUTDOWN_TIMEOUT_SECONDS", DefaultShutdownTimeout)
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.ShutdownTimeoutSeconds = shutdown

	level, err := parseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		problems = append(problems, err.Error())
	}
	cfg.LogLevel = level

	if len(problems) > 0 {
		return nil, errors.New("invalid configuration: " + strings.Join(problems, "; "))
	}
	return cfg, nil
}

// intEnv parses an optional positive integer variable.
func intEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

// parseLevel maps LOG_LEVEL onto a zap level, defaulting to info.
func parseLevel(raw string) (zapcore.Level, error) {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		raw = DefaultLogLevel
	}
	var level zapcore.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("LOG_LEVEL must be one of debug, info, warn, error")
	}
	return level, nil
}
