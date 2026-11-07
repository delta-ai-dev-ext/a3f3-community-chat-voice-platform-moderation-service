package models_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/models"
)

// TestNewUUIDProducesCanonicalV4 checks the id format every row and envelope uses.
func TestNewUUIDProducesCanonicalV4(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id := models.NewUUID()
		require.True(t, models.IsUUID(id), "not a canonical uuid: %q", id)
		assert.Equal(t, '4', rune(id[14]), "version nibble must be 4")
		assert.Contains(t, "89ab", string(id[19]), "variant nibble must be 8, 9, a or b")
		_, duplicate := seen[id]
		assert.False(t, duplicate, "ids must be unique")
		seen[id] = struct{}{}
	}
}

// TestIsUUID rejects malformed identifiers so path parameters cannot reach the
// database as casting failures.
func TestIsUUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		value string
		valid bool
	}{
		{value: "11111111-1111-4111-8111-111111111111", valid: true},
		{value: "11111111-1111-4111-8111-11111111111G", valid: false},
		{value: "11111111111141118111111111111111", valid: false},
		{value: "11111111-1111-4111-8111-1111111111111", valid: false},
		{value: "", valid: false},
	}

	for _, test := range tests {
		assert.Equal(t, test.valid, models.IsUUID(test.value), test.value)
	}
}

// TestSanctionComputeStatus covers the computed active/expired/lifted view.
func TestSanctionComputeStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)
	liftedAt := now.Add(-time.Hour)
	liftSource := models.SourceModerator

	tests := []struct {
		name       string
		sanction   models.Sanction
		expect     models.SanctionStatus
		appealable bool
	}{
		{
			name:       "indefinite mute is active and appealable",
			sanction:   models.Sanction{Type: models.SanctionTypeMute},
			expect:     models.StatusActive,
			appealable: true,
		},
		{
			name:       "timed mute in the future is active",
			sanction:   models.Sanction{Type: models.SanctionTypeMute, ExpiresAt: &future},
			expect:     models.StatusActive,
			appealable: true,
		},
		{
			name:     "timed mute past its expiry is expired",
			sanction: models.Sanction{Type: models.SanctionTypeMute, ExpiresAt: &past},
			expect:   models.StatusExpired,
		},
		{
			name: "lifted sanction is lifted regardless of expiry",
			sanction: models.Sanction{
				Type:       models.SanctionTypeMute,
				ExpiresAt:  &future,
				LiftedAt:   &liftedAt,
				LiftSource: &liftSource,
			},
			expect: models.StatusLifted,
		},
		{
			name:     "kick is never appealable",
			sanction: models.Sanction{Type: models.SanctionTypeKick},
			expect:   models.StatusActive,
		},
		{
			name:       "ban is appealable",
			sanction:   models.Sanction{Type: models.SanctionTypeBan},
			expect:     models.StatusActive,
			appealable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.expect, test.sanction.ComputeStatus(now))
			assert.Equal(t, test.expect == models.StatusActive, test.sanction.Active(now))
			assert.Equal(t, test.appealable, test.sanction.Appealable(now))
		})
	}
}

// TestCursorRoundTrip proves the opaque cursor survives encoding unchanged and
// rejects tampered values.
func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()

	cursor := models.Cursor{
		Timestamp: time.Date(2026, 1, 14, 12, 0, 0, 123456789, time.UTC),
		ID:        "11111111-1111-4111-8111-111111111111",
	}
	decoded, err := models.DecodeCursor(cursor.Encode())
	require.NoError(t, err)
	require.NotNil(t, decoded)
	assert.True(t, cursor.Timestamp.Equal(decoded.Timestamp))
	assert.Equal(t, cursor.ID, decoded.ID)

	empty, err := models.DecodeCursor("")
	require.NoError(t, err)
	assert.Nil(t, empty)

	for _, token := range []string{"!!!", "dGVzdA", "bm90LWEtdGltZXwxMjM"} {
		_, err := models.DecodeCursor(token)
		assert.ErrorIs(t, err, models.ErrInvalidCursor, token)
	}
}

// TestNewPaginationBounds covers the default and maximum page size.
func TestNewPaginationBounds(t *testing.T) {
	t.Parallel()

	defaults, err := models.NewPagination("", "")
	require.NoError(t, err)
	assert.Equal(t, models.DefaultPageSize, defaults.Limit)
	assert.Nil(t, defaults.Cursor)

	max, err := models.NewPagination("100", "")
	require.NoError(t, err)
	assert.Equal(t, 100, max.Limit)

	for _, raw := range []string{"0", "101", "-1", "abc"} {
		_, err := models.NewPagination(raw, "")
		require.Error(t, err, raw)
	}
}

// TestNextCursor covers the look-ahead row convention.
func TestNextCursor(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)
	rows := []models.AuditLogEntry{{ID: "a", CreatedAt: now}, {ID: "b", CreatedAt: now.Add(-time.Hour)}}
	position := func(entry models.AuditLogEntry) models.Cursor {
		return models.Cursor{Timestamp: entry.CreatedAt, ID: entry.ID}
	}

	assert.Nil(t, models.NextCursor(rows, false, position))
	assert.Nil(t, models.NextCursor(nil, true, position))

	next := models.NextCursor(rows, true, position)
	require.NotNil(t, next)
	decoded, err := models.DecodeCursor(*next)
	require.NoError(t, err)
	assert.Equal(t, "b", decoded.ID)
}

// TestDenialErrorMapping pins the denial_reason to 403 code table.
func TestDenialErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reason string
		code   string
		status int
	}{
		{reason: "missing_permission", code: "insufficient_permission", status: http.StatusForbidden},
		{reason: "target_outranks_actor", code: "target_outranks_actor", status: http.StatusForbidden},
		{reason: "target_is_owner", code: "target_is_owner", status: http.StatusForbidden},
		{reason: "target_is_self", code: "target_is_self", status: http.StatusForbidden},
		{reason: "actor_not_member", code: "insufficient_permission", status: http.StatusForbidden},
		{reason: "", code: "insufficient_permission", status: http.StatusForbidden},
		{reason: "unknown_reason", code: "insufficient_permission", status: http.StatusForbidden},
	}

	for _, test := range tests {
		assert.Equal(t, test.code, models.DenialError(test.reason).Code, test.reason)
		assert.Equal(t, test.status, models.DenialError(test.reason).Status, test.reason)
	}
}

// TestAsErrorHidesUnknownFailures proves an unexpected error never leaks detail.
func TestAsErrorHidesUnknownFailures(t *testing.T) {
	t.Parallel()

	assert.Nil(t, models.AsError(nil))
	assert.Equal(t, models.ErrInternal, models.AsError(assert.AnError))
	assert.Equal(t, models.ErrSanctionNotFound, models.AsError(models.ErrSanctionNotFound))

	detailed := models.ErrSanctionNotFound.WithDetail("gone")
	assert.Equal(t, "sanction_not_found", models.AsError(detailed).Code)
	assert.Equal(t, "gone", models.AsError(detailed).Detail)
	assert.Equal(t, "sanction_not_found: gone", detailed.Error())
	assert.Equal(t, "x", models.ErrInternal.WithDetailf("%s", "x").Detail)
}
