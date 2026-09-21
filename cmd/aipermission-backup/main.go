package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aipermission/backup/internal/api"
	"github.com/aipermission/backup/internal/config"
	"github.com/aipermission/backup/internal/store"
)

var version = "dev"

const gracefulShutdownTimeout = 15 * time.Second

type shutdownServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) == 2 && os.Args[1] == "--healthcheck" {
		if err := healthcheck(); err != nil {
			logger.Error("healthcheck failed", "error", err)
			os.Exit(1)
		}
		return
	}
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	storage, err := store.Open(cfg.DataDir, store.Options{
		MaxStorageBytes:     cfg.MaxStorageBytes,
		MaxUploadOperations: cfg.MaxUploadOperations,
	})
	if err != nil {
		logger.Error("open backup store", "error", err)
		os.Exit(1)
	}
	defer storage.Close()

	handler := api.New(api.Config{
		Token:          cfg.Token,
		MaxUploadBytes: cfg.MaxUploadBytes,
		Version:        version,
	}, storage, logger)
	cfg.Token = ""
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadTimeout:       15 * time.Minute,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		WriteTimeout:      15 * time.Minute,
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("backup service started", "address", cfg.ListenAddr, "version", version)
	if err := serveUntilShutdown(shutdownCtx, server, gracefulShutdownTimeout); err != nil {
		logger.Error("serve backup API", "error", err)
		os.Exit(1)
	}
}

func serveUntilShutdown(ctx context.Context, server shutdownServer, timeout time.Duration) error {
	waitCtx, stopWaiting := context.WithCancel(context.Background())
	defer stopWaiting()

	shutdownDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			shutdownDone <- server.Shutdown(shutdownCtx)
		case <-waitCtx.Done():
			shutdownDone <- nil
		}
	}()

	serveErr := server.ListenAndServe()
	stopWaiting()
	shutdownErr := <-shutdownDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, shutdownErr)
}

func healthcheck() error {
	url := os.Getenv("AIPERMISSION_BACKUP_HEALTHCHECK_URL")
	if url == "" {
		url = "http://127.0.0.1:8080/healthz"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("health endpoint returned " + response.Status)
	}
	return nil
}
