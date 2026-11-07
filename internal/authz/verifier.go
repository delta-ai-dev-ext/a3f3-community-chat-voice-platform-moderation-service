// Package authz holds the two authentication/authorization concerns of this
// service: local verification of identity_service access tokens, and the
// real-time, fail-closed authorization client for community_service API-001.
package authz

import (
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"moderation_service/internal/models"
)

// Identity is the authenticated caller derived from a verified access token.
type Identity struct {
	// UserID is the token subject, i.e. the calling user id.
	UserID string
	// SessionID is the identity_service session id ("sid" claim).
	SessionID string
}

// verificationKey is one identity_service public key.
type verificationKey struct {
	kid       string
	algorithm string
	publicKey any
}

// Verifier verifies identity_service access tokens locally; there is no runtime
// call to identity_service. Nothing is cached across requests.
type Verifier struct {
	keys     []verificationKey
	issuer   string
	audience string
}

// claims is the subset of identity_service token claims this service consumes.
type claims struct {
	jwt.RegisteredClaims
	SessionID     string `json:"sid"`
	EmailVerified bool   `json:"email_verified"`
}

// NewVerifier parses the configured verification keys and expected issuer /
// audience. Configuration errors are returned so startup fails fast rather than
// silently rejecting every request.
func NewVerifier(jwksRaw, issuer, audience string) (*Verifier, error) {
	keys, err := parseVerificationKeys(jwksRaw)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, errors.New("IDENTITY_JWKS_KEYS contains no usable verification key")
	}
	if issuer == "" || audience == "" {
		return nil, errors.New("IDENTITY_TOKEN_ISSUER and IDENTITY_TOKEN_AUDIENCE are required")
	}
	return &Verifier{keys: keys, issuer: issuer, audience: audience}, nil
}

// Verify validates signature, iss, aud and exp of an access token and returns
// the caller identity. Every failure is the same generic 401 so no verification
// detail leaks to clients.
func (v *Verifier) Verify(rawToken string) (Identity, error) {
	if strings.TrimSpace(rawToken) == "" {
		return Identity{}, models.ErrUnauthorized
	}
	options := []jwt.ParserOption{
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithExpirationRequired(),
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "EdDSA"}),
	}
	for _, key := range v.candidateKeys(rawToken) {
		parsed := &claims{}
		token, err := jwt.ParseWithClaims(rawToken, parsed, func(*jwt.Token) (any, error) {
			return key.publicKey, nil
		}, options...)
		if err != nil || !token.Valid {
			continue
		}
		identity := Identity{UserID: parsed.Subject, SessionID: parsed.SessionID}
		if identity.UserID == "" {
			return Identity{}, models.ErrUnauthorized
		}
		return identity, nil
	}
	return Identity{}, models.ErrUnauthorized
}

// candidateKeys orders the configured keys so a token is first tried against
// the key its "kid" header names, falling back to all keys when it is absent or
// unknown (single-key rotation windows).
func (v *Verifier) candidateKeys(rawToken string) []verificationKey {
	kid, err := tokenKeyID(rawToken)
	if err != nil || kid == "" {
		return v.keys
	}
	ordered := make([]verificationKey, 0, len(v.keys))
	for _, key := range v.keys {
		if key.kid == kid {
			ordered = append(ordered, key)
		}
	}
	if len(ordered) == 0 {
		return v.keys
	}
	for _, key := range v.keys {
		if key.kid != kid {
			ordered = append(ordered, key)
		}
	}
	return ordered
}

// tokenKeyID reads the unverified "kid" header of a compact JWS.
func tokenKeyID(rawToken string) (string, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 {
		return "", errors.New("malformed token")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("decode token header: %w", err)
	}
	var header struct {
		Kid string `json:"kid"`
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", fmt.Errorf("parse token header: %w", err)
	}
	return header.Kid, nil
}

// jwksDocument is the standard JSON Web Key Set shape.
type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

// jwk is one JSON Web Key (RSA or OKP/Ed25519).
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
}

// pemKeyEntry is the alternative "list of PEM keys" configuration shape.
type pemKeyEntry struct {
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	PEM string `json:"pem"`
}

// parseVerificationKeys accepts either a JWKS document ({"keys":[...]}) or a
// JSON array of {kid, alg, pem} entries, so key rotation can be configured
// without re-encoding anything.
func parseVerificationKeys(raw string) ([]verificationKey, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("IDENTITY_JWKS_KEYS is empty")
	}
	if strings.HasPrefix(trimmed, "[") {
		return parsePEMKeyList(trimmed)
	}
	var document jwksDocument
	if err := json.Unmarshal([]byte(trimmed), &document); err != nil {
		return nil, fmt.Errorf("parse IDENTITY_JWKS_KEYS: %w", err)
	}
	keys := make([]verificationKey, 0, len(document.Keys))
	for _, key := range document.Keys {
		publicKey, algorithm, err := key.publicKey()
		if err != nil {
			return nil, err
		}
		keys = append(keys, verificationKey{kid: key.Kid, algorithm: algorithm, publicKey: publicKey})
	}
	return keys, nil
}

// parsePEMKeyList parses the JSON-array PEM configuration shape.
func parsePEMKeyList(raw string) ([]verificationKey, error) {
	var entries []pemKeyEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("parse IDENTITY_JWKS_KEYS: %w", err)
	}
	keys := make([]verificationKey, 0, len(entries))
	for _, entry := range entries {
		block, _ := pem.Decode([]byte(entry.PEM))
		if block == nil {
			return nil, fmt.Errorf("parse identity key %q: no PEM block found", entry.Kid)
		}
		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse identity key %q: %w", entry.Kid, err)
		}
		keys = append(keys, verificationKey{kid: entry.Kid, algorithm: entry.Alg, publicKey: parsed})
	}
	return keys, nil
}

// publicKey converts a JWK into a Go crypto public key.
func (k jwk) publicKey() (any, string, error) {
	switch strings.ToUpper(k.Kty) {
	case "RSA":
		modulus := new(big.Int)
		decodedN, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			return nil, "", fmt.Errorf("parse jwk %q modulus: %w", k.Kid, err)
		}
		modulus.SetBytes(decodedN)
		decodedE, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil {
			return nil, "", fmt.Errorf("parse jwk %q exponent: %w", k.Kid, err)
		}
		exponent := 0
		for _, b := range decodedE {
			exponent = exponent<<8 | int(b)
		}
		algorithm := k.Alg
		if algorithm == "" {
			algorithm = "RS256"
		}
		return &rsa.PublicKey{N: modulus, E: exponent}, algorithm, nil
	case "OKP":
		if !strings.EqualFold(k.Crv, "Ed25519") {
			return nil, "", fmt.Errorf("unsupported jwk %q curve %q", k.Kid, k.Crv)
		}
		decodedX, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, "", fmt.Errorf("parse jwk %q public key: %w", k.Kid, err)
		}
		if len(decodedX) != ed25519.PublicKeySize {
			return nil, "", fmt.Errorf("jwk %q public key has %d bytes, want %d", k.Kid, len(decodedX), ed25519.PublicKeySize)
		}
		return ed25519.PublicKey(decodedX), "EdDSA", nil
	default:
		return nil, "", fmt.Errorf("unsupported jwk %q key type %q", k.Kid, k.Kty)
	}
}
