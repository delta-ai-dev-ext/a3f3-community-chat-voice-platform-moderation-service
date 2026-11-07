package models

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// Page size bounds for every paginated listing.
const (
	DefaultPageSize = 50
	MaxPageSize     = 100
)

// Cursor is the opaque keyset position of a listing: rows strictly older than
// (Timestamp, ID) are returned next.
type Cursor struct {
	Timestamp time.Time
	ID        string
}

// Encode renders the cursor as an opaque, URL-safe token.
func (c Cursor) Encode() string {
	raw := c.Timestamp.UTC().Format(time.RFC3339Nano) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses an opaque cursor produced by Cursor.Encode. An empty
// token means "start from the beginning" and returns a nil cursor.
func DecodeCursor(token string) (*Cursor, error) {
	if token == "" {
		return nil, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrInvalidCursor
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 || parts[1] == "" {
		return nil, ErrInvalidCursor
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return nil, ErrInvalidCursor
	}
	return &Cursor{Timestamp: ts, ID: parts[1]}, nil
}

// Pagination holds the resolved page size and keyset position of a request.
type Pagination struct {
	Limit  int
	Cursor *Cursor
}

// NewPagination resolves the raw limit and cursor query parameters. A missing
// limit defaults to DefaultPageSize; a limit outside [1, MaxPageSize] is a
// bad request and a malformed cursor is reported as ErrInvalidCursor.
func NewPagination(rawLimit, rawCursor string) (Pagination, error) {
	page := Pagination{Limit: DefaultPageSize}
	if rawLimit != "" {
		limit, err := strconv.Atoi(rawLimit)
		if err != nil || limit < 1 || limit > MaxPageSize {
			return Pagination{}, ErrInvalidRequest.WithDetailf("limit must be an integer between 1 and %d.", MaxPageSize)
		}
		page.Limit = limit
	}
	cursor, err := DecodeCursor(rawCursor)
	if err != nil {
		return Pagination{}, err
	}
	page.Cursor = cursor
	return page, nil
}

// NextCursor returns the cursor for the page following rows, or nil when the
// page was not full. hasMore is the repository's "one extra row" signal.
func NextCursor[T any](rows []T, hasMore bool, position func(T) Cursor) *string {
	if !hasMore || len(rows) == 0 {
		return nil
	}
	token := position(rows[len(rows)-1]).Encode()
	return &token
}
