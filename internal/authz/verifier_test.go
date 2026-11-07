package authz_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/authz"
	"moderation_service/internal/models"
)

const (
	testIssuer   = "identity_service"
	testAudience = "moderation_service"
	testSubject  = "44444444-4444-4444-8444-444444444444"
	testSession  = "55555555-5555-4555-8555-555555555555"
)

// signingFixture holds one RSA key pair and the JWKS document that advertises
// its public half, mimicking identity_service's current+next key set.
type signingFixture struct {
	key  *rsa.PrivateKey
	kid  string
	jwks string
}

// newSigningFixture generates a fresh RSA key and JWKS document per test.
func newSigningFixture(t *testing.T, kid string) *signingFixture {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	document := map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"kid": kid,
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}},
	}
	encoded, err := json.Marshal(document)
	require.NoError(t, err)
	return &signingFixture{key: key, kid: kid, jwks: string(encoded)}
}

// mint signs an access token with the fixture key.
func (f *signingFixture) mint(t *testing.T, mutate func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub":            testSubject,
		"sid":            testSession,
		"iss":            testIssuer,
		"aud":            []string{testAudience},
		"exp":            time.Now().Add(time.Hour).Unix(),
		"iat":            time.Now().Unix(),
		"email_verified": true,
	}
	if mutate != nil {
		mutate(claims)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = f.kid
	signed, err := token.SignedString(f.key)
	require.NoError(t, err)
	return signed
}

// TestVerifyAcceptsValidToken checks the happy path: signature, iss, aud and exp
// all validate and the caller identity is taken from sub/sid.
func TestVerifyAcceptsValidToken(t *testing.T) {
	t.Parallel()

	fixture := newSigningFixture(t, "key-1")
	verifier, err := authz.NewVerifier(fixture.jwks, testIssuer, testAudience)
	require.NoError(t, err)

	identity, err := verifier.Verify(fixture.mint(t, nil))

	require.NoError(t, err)
	assert.Equal(t, testSubject, identity.UserID)
	assert.Equal(t, testSession, identity.SessionID)
}

// TestVerifyRejectsInvalidTokens enumerates every reason an access token must be
// rejected; all of them produce the same generic 401 so nothing leaks.
func TestVerifyRejectsInvalidTokens(t *testing.T) {
	t.Parallel()

	fixture := newSigningFixture(t, "key-1")
	otherKey := newSigningFixture(t, "key-2")

	tests := []struct {
		name  string
		token string
	}{
		{name: "empty token", token: ""},
		{name: "not a jwt", token: "definitely-not-a-token"},
		{name: "expired token", token: fixture.mint(t, func(c jwt.MapClaims) {
			c["exp"] = time.Now().Add(-time.Minute).Unix()
		})},
		{name: "missing exp", token: fixture.mint(t, func(c jwt.MapClaims) {
			delete(c, "exp")
		})},
		{name: "wrong issuer", token: fixture.mint(t, func(c jwt.MapClaims) {
			c["iss"] = "another_service"
		})},
		{name: "wrong audience", token: fixture.mint(t, func(c jwt.MapClaims) {
			c["aud"] = []string{"another_service"}
		})},
		{name: "signed by an unknown key", token: otherKey.mint(t, nil)},
		{name: "missing subject", token: fixture.mint(t, func(c jwt.MapClaims) {
			delete(c, "sub")
		})},
	}

	verifier, err := authz.NewVerifier(fixture.jwks, testIssuer, testAudience)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			identity, err := verifier.Verify(test.token)
			require.Error(t, err)
			assert.True(t, errors.Is(err, models.ErrUnauthorized), "got %v", err)
			assert.Empty(t, identity.UserID)
		})
	}
}

// TestVerifyAcceptsPEMKeyList proves the alternative PEM configuration shape
// works, so key rotation can be configured without a JWKS document.
func TestVerifyAcceptsPEMKeyList(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	configuration, err := json.Marshal([]map[string]string{{
		"kid": "pem-key",
		"alg": "RS256",
		"pem": string(pemBytes),
	}})
	require.NoError(t, err)

	verifier, err := authz.NewVerifier(string(configuration), testIssuer, testAudience)
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": testSubject,
		"sid": testSession,
		"iss": testIssuer,
		"aud": []string{testAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = "pem-key"
	signed, err := token.SignedString(key)
	require.NoError(t, err)

	identity, err := verifier.Verify(signed)
	require.NoError(t, err)
	assert.Equal(t, testSubject, identity.UserID)
}

// TestVerifyAcceptsEd25519Key proves the OKP branch of JWKS parsing works.
func TestVerifyAcceptsEd25519Key(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	configuration, err := json.Marshal(map[string]any{
		"keys": []map[string]any{{
			"kty": "OKP",
			"crv": "Ed25519",
			"kid": "ed-key",
			"alg": "EdDSA",
			"x":   base64.RawURLEncoding.EncodeToString(publicKey),
		}},
	})
	require.NoError(t, err)

	verifier, err := authz.NewVerifier(string(configuration), testIssuer, testAudience)
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"sub": testSubject,
		"iss": testIssuer,
		"aud": []string{testAudience},
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = "ed-key"
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)

	identity, err := verifier.Verify(signed)
	require.NoError(t, err)
	assert.Equal(t, testSubject, identity.UserID)
	assert.Empty(t, identity.SessionID)
}

// TestNewVerifierRejectsBadConfiguration ensures a broken key configuration
// fails startup instead of rejecting every request at runtime.
func TestNewVerifierRejectsBadConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keys     string
		issuer   string
		audience string
	}{
		{name: "empty keys", keys: "", issuer: testIssuer, audience: testAudience},
		{name: "no keys in document", keys: `{"keys":[]}`, issuer: testIssuer, audience: testAudience},
		{name: "malformed json", keys: `{"keys":`, issuer: testIssuer, audience: testAudience},
		{name: "missing issuer", keys: `{"keys":[{"kty":"RSA","kid":"k","n":"AQAB","e":"AQAB"}]}`, audience: testAudience},
		{name: "missing audience", keys: `{"keys":[{"kty":"RSA","kid":"k","n":"AQAB","e":"AQAB"}]}`, issuer: testIssuer},
		{name: "unsupported key type", keys: `{"keys":[{"kty":"EC","kid":"k"}]}`, issuer: testIssuer, audience: testAudience},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			verifier, err := authz.NewVerifier(test.keys, test.issuer, test.audience)
			require.Error(t, err)
			assert.Nil(t, verifier)
		})
	}
}
