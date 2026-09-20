package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestComposeStopGracePeriodExceedsServerDrain(t *testing.T) {
	for _, name := range []string{"docker-compose.yml", "docker-compose.release.yml"} {
		content, err := os.ReadFile(filepath.Join("..", "..", name))
		if err != nil {
			t.Fatal(err)
		}
		var configured time.Duration
		for _, line := range strings.Split(string(content), "\n") {
			value, found := strings.CutPrefix(strings.TrimSpace(line), "stop_grace_period:")
			if !found {
				continue
			}
			configured, err = time.ParseDuration(strings.TrimSpace(value))
			if err != nil {
				t.Fatalf("%s stop_grace_period: %v", name, err)
			}
			break
		}
		if configured <= gracefulShutdownTimeout {
			t.Fatalf("%s stop_grace_period = %s, want more than %s", name, configured, gracefulShutdownTimeout)
		}
	}
}

type drainingServer struct {
	started       chan struct{}
	shutdownStart chan struct{}
	drain         chan struct{}
	stopped       chan struct{}
	once          sync.Once
}

func newDrainingServer() *drainingServer {
	return &drainingServer{
		started:       make(chan struct{}),
		shutdownStart: make(chan struct{}),
		drain:         make(chan struct{}),
		stopped:       make(chan struct{}),
	}
}

func (server *drainingServer) ListenAndServe() error {
	close(server.started)
	<-server.stopped
	return http.ErrServerClosed
}

func (server *drainingServer) Shutdown(ctx context.Context) error {
	close(server.shutdownStart)
	select {
	case <-server.drain:
		server.once.Do(func() { close(server.stopped) })
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestServeUntilShutdownWaitsForDrain(t *testing.T) {
	server := newDrainingServer()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveUntilShutdown(ctx, server, time.Second)
	}()

	waitForTestValue(t, server.started, "server start")
	cancel()
	waitForTestValue(t, server.shutdownStart, "shutdown start")
	select {
	case err := <-done:
		t.Fatalf("serve returned before shutdown drained: %v", err)
	default:
	}

	close(server.drain)
	if err := waitForTestValue(t, done, "graceful shutdown completion"); err != nil {
		t.Fatalf("serve after graceful shutdown: %v", err)
	}
}

type failingServer struct {
	err         error
	shutdownErr error
}

func (server failingServer) ListenAndServe() error { return server.err }
func (server failingServer) Shutdown(context.Context) error {
	return server.shutdownErr
}

func TestServeUntilShutdownReturnsServeFailure(t *testing.T) {
	want := errors.New("listen failed")
	if err := serveUntilShutdown(context.Background(), failingServer{err: want}, time.Second); !errors.Is(err, want) {
		t.Fatalf("serve error = %v, want %v", err, want)
	}
}

func TestServeUntilShutdownReturnsShutdownFailure(t *testing.T) {
	want := errors.New("drain failed")
	ctx, cancel := context.WithCancel(context.Background())
	server := &shutdownFailingServer{started: make(chan struct{}), stopped: make(chan struct{}), err: want}
	done := make(chan error, 1)
	go func() { done <- serveUntilShutdown(ctx, server, time.Second) }()
	waitForTestValue(t, server.started, "server start")
	cancel()
	if err := waitForTestValue(t, done, "failed shutdown completion"); !errors.Is(err, want) {
		t.Fatalf("shutdown error = %v, want %v", err, want)
	}
}

func waitForTestValue[T any](t *testing.T, values <-chan T, label string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
		var zero T
		return zero
	}
}

type shutdownFailingServer struct {
	started chan struct{}
	stopped chan struct{}
	err     error
}

func (server *shutdownFailingServer) ListenAndServe() error {
	close(server.started)
	<-server.stopped
	return http.ErrServerClosed
}

func (server *shutdownFailingServer) Shutdown(context.Context) error {
	close(server.stopped)
	return server.err
}
