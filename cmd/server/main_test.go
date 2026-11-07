package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestRunFailsFastWithoutConfiguration proves the entrypoint validates the
// 12-factor configuration before it touches PostgreSQL or Kafka. With every
// required variable cleared, startup must return a missing-configuration error
// instead of half-starting the process.
func TestRunFailsFastWithoutConfiguration(t *testing.T) {
	for _, name := range []string{
		"DATABASE_URL",
		"KAFKA_BROKERS",
		"COMMUNITY_SERVICE_URL",
		"COMMUNITY_SERVICE_TOKEN",
		"IDENTITY_JWKS_KEYS",
		"IDENTITY_TOKEN_ISSUER",
		"IDENTITY_TOKEN_AUDIENCE",
	} {
		t.Setenv(name, "")
	}

	err := run()
	if err == nil {
		t.Fatal("run() must fail when the required configuration is missing")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Errorf("run() error = %q, want it to name the missing variables", err)
	}
}

// TestWaitLoopsRespectsDeadline proves the drain cannot outlive the shutdown
// budget: a loop that never returns makes waitLoops report the timeout instead
// of blocking the process.
func TestWaitLoopsRespectsDeadline(t *testing.T) {
	var loops sync.WaitGroup
	loops.Add(1)
	blocked := make(chan struct{})
	defer close(blocked)
	go func() {
		defer loops.Done()
		<-blocked
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	if waitLoops(ctx, &loops) {
		t.Fatal("waitLoops() must report false when the deadline passes first")
	}
}

// TestWaitLoopsReturnsWhenLoopsStop proves the happy path drains without
// waiting for the shutdown deadline once every loop has returned.
func TestWaitLoopsReturnsWhenLoopsStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var loops sync.WaitGroup
	loops.Add(1)
	go func() { defer loops.Done() }()

	if !waitLoops(ctx, &loops) {
		t.Fatal("waitLoops() must report true when every loop returned before the deadline")
	}
}

// TestServeReturnsNilOnGracefulShutdown proves a closed listener is not reported
// as a failure, which is what lets the signal path exit 0.
func TestServeReturnsNilOnGracefulShutdown(t *testing.T) {
	addr := freeAddress(t)

	server := &http.Server{Addr: addr, Handler: http.NewServeMux(),
		ReadHeaderTimeout: readHeaderTimeout}
	done := make(chan error, 1)
	go func() { done <- serve(server) }()
	waitForListener(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("serve() = %v, want nil after a graceful shutdown", err)
	}
}

// TestServeReturnsBindError proves a port that is already taken is reported
// instead of being mistaken for a drained shutdown.
func TestServeReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	server := &http.Server{Addr: listener.Addr().String()}
	if err := serve(server); err == nil {
		t.Fatal("serve() must return the bind error when the address is taken")
	}
}

// freeAddress reserves and releases a loopback port so a test can bind it.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

// waitForListener blocks until addr accepts TCP connections, so a shutdown is
// never issued against a listener that has not bound yet.
func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("listener %s never accepted a connection", addr)
}
