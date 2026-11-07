package models

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// AuditRetention is the platform commitment: audit entries are readable and
// retained for exactly one year (ADR-002).
const AuditRetention = 365 * 24 * time.Hour

// NewUUID returns a random RFC 4122 version 4 UUID string. Ids are generated in
// the service rather than by the database so an event envelope can carry the
// same id as the row it describes.
func NewUUID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// crypto/rand does not fail on supported platforms; the timestamp keeps
		// the fallback unique enough for a single process.
		return hex.EncodeToString([]byte(time.Now().UTC().Format("20060102150405.000000000")[:16]))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	encoded := hex.EncodeToString(buf[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}

// IsUUID reports whether value is a canonical 8-4-4-4-12 hex UUID string. Path
// parameters are validated with it so a malformed id is a client error rather
// than a database casting failure.
func IsUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, r := range value {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !isHexDigit(r) {
				return false
			}
		}
	}
	return true
}

// isHexDigit reports whether r is a lowercase or uppercase hexadecimal digit.
func isHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// IsHexID reports whether value is a non-empty hex string of at most
// maxLength characters, used to validate propagated trace ids.
func IsHexID(value string, maxLength int) bool {
	if value == "" || len(value) > maxLength {
		return false
	}
	return strings.Trim(value, "0123456789abcdefABCDEF") == ""
}
