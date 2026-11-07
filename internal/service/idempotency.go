package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"moderation_service/internal/models"
	"moderation_service/internal/repository"
)

// idempotencyScope builds the replay scope of a keyed write. The scope includes
// the endpoint and every resource that makes the key meaningful, so the same
// key may be reused across communities and users (US-015).
func idempotencyScope(operation string, parts ...string) string {
	segments := make([]string, 0, len(parts)+1)
	segments = append(segments, operation)
	for _, part := range parts {
		if part != "" {
			segments = append(segments, part)
		}
	}
	return strings.Join(segments, ":")
}

// replayResponse decodes the first accepted response for a key into out. It
// returns false when the key is unknown, so the caller performs the write.
func replayResponse(ctx context.Context, repos repository.Repositories, scope, key string, out any) (bool, error) {
	if key == "" {
		return false, nil
	}
	record, err := repos.Idempotency.Get(ctx, scope, key)
	if err != nil {
		return false, fmt.Errorf("load idempotent response: %w", err)
	}
	if record == nil {
		return false, nil
	}
	if err := json.Unmarshal(record.ResponseBody, out); err != nil {
		return false, fmt.Errorf("decode idempotent response: %w", err)
	}
	return true, nil
}

// storeResponse persists the first accepted response for a key inside the
// caller's transaction. It reports whether the record was stored; a concurrent
// duplicate leaves the winner's record untouched and is not an error.
func storeResponse(ctx context.Context, repos repository.Repositories, scope, key string, request any, statusCode int, response any, now time.Time) error {
	if key == "" {
		return nil
	}
	encodedRequest, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode idempotent request: %w", err)
	}
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode idempotent response: %w", err)
	}
	sum := sha256.Sum256(encodedRequest)
	record := &models.IdempotencyRecord{
		Scope:        scope,
		Key:          key,
		RequestHash:  hex.EncodeToString(sum[:]),
		ResponseCode: statusCode,
		ResponseBody: encodedResponse,
		CreatedAt:    now.UTC(),
	}
	if err := repos.Idempotency.Put(ctx, record); err != nil {
		if err == repository.ErrDuplicate {
			return nil
		}
		return fmt.Errorf("store idempotent response: %w", err)
	}
	return nil
}

// requireUUID validates a path or body identifier so a malformed id becomes a
// client error instead of a database casting failure.
func requireUUID(value string, field string) error {
	if !models.IsUUID(value) {
		return models.ErrInvalidRequest.WithDetailf("%s must be a UUID.", field)
	}
	return nil
}

// normalizeText trims a free-text field and maps the empty string to nil so
// optional columns store SQL NULL rather than "".
func normalizeText(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
