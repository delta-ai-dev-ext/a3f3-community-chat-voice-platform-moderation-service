package config_test

import (
	"strings"
	"testing"

	"go.uber.org/zap/zapcore"

	"moderation_service/internal/config"
)

// setRequiredEnv sets every required variable to a valid value.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/moderation?sslmode=disable")
	t.Setenv("KAFKA_BROKERS", "broker-1:9092, broker-2:9092")
	t.Setenv("COMMUNITY_SERVICE_URL", "http://community.test")
	t.Setenv("COMMUNITY_SERVICE_TOKEN", "service-token")
	t.Setenv("IDENTITY_JWKS_KEYS", `{"keys":[]}`)
	t.Setenv("IDENTITY_TOKEN_ISSUER", "identity_service")
	t.Setenv("IDENTITY_TOKEN_AUDIENCE", "moderation_service")
}

// TestLoadAppliesDefaults proves the documented defaults and broker parsing.
func TestLoadAppliesDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ServicePort != config.DefaultServicePort {
		t.Errorf("ServicePort = %d, want %d", cfg.ServicePort, config.DefaultServicePort)
	}
	if cfg.KafkaTopicModerationEvents != config.DefaultKafkaTopic {
		t.Errorf("topic = %q, want %q", cfg.KafkaTopicModerationEvents, config.DefaultKafkaTopic)
	}
	if cfg.LogLevel != zapcore.InfoLevel {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if cfg.ShutdownTimeoutSeconds != config.DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeoutSeconds = %d, want %d", cfg.ShutdownTimeoutSeconds, config.DefaultShutdownTimeout)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "broker-1:9092" || cfg.KafkaBrokers[1] != "broker-2:9092" {
		t.Errorf("KafkaBrokers = %v, want trimmed list", cfg.KafkaBrokers)
	}
}

// TestLoadOverridesDefaults proves every documented override is honoured.
func TestLoadOverridesDefaults(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SERVICE_PORT", "9091")
	t.Setenv("KAFKA_TOPIC_MODERATION_EVENTS", "custom_events")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "5")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ServicePort != 9091 || cfg.KafkaTopicModerationEvents != "custom_events" ||
		cfg.LogLevel != zapcore.DebugLevel || cfg.ShutdownTimeoutSeconds != 5 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

// TestLoadFailsFastOnMissingRequiredVariables proves startup cannot proceed with
// an incomplete configuration, and that every problem is reported at once.
func TestLoadFailsFastOnMissingRequiredVariables(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("COMMUNITY_SERVICE_URL", "")
	t.Setenv("COMMUNITY_SERVICE_TOKEN", "")
	t.Setenv("IDENTITY_JWKS_KEYS", "")
	t.Setenv("IDENTITY_TOKEN_ISSUER", "")
	t.Setenv("IDENTITY_TOKEN_AUDIENCE", "")

	cfg, err := config.Load()
	if err == nil {
		t.Fatal("Load() must fail when required variables are missing")
	}
	if cfg != nil {
		t.Fatalf("Load() returned a config on error: %+v", cfg)
	}
	for _, name := range []string{
		"DATABASE_URL", "KAFKA_BROKERS", "COMMUNITY_SERVICE_URL", "COMMUNITY_SERVICE_TOKEN",
		"IDENTITY_JWKS_KEYS", "IDENTITY_TOKEN_ISSUER", "IDENTITY_TOKEN_AUDIENCE",
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name %s", err, name)
		}
	}
}

// TestLoadRejectsInvalidNumbersAndLevels proves malformed values fail startup.
func TestLoadRejectsInvalidNumbersAndLevels(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SERVICE_PORT", "eighty")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() must reject a non-numeric SERVICE_PORT")
	}

	setRequiredEnv(t)
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "-1")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() must reject a negative SHUTDOWN_TIMEOUT_SECONDS")
	}

	setRequiredEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() must reject an unknown LOG_LEVEL")
	}
}
