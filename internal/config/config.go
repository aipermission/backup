package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultListenAddr     = "127.0.0.1:8080"
	defaultDataDir        = "/data"
	defaultMaxUploadBytes = int64(2 << 30) // 2 GiB
)

type Config struct {
	ListenAddr     string
	DataDir        string
	Token          string
	MaxUploadBytes int64
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:     envOrDefault("AIPERMISSION_BACKUP_LISTEN_ADDR", defaultListenAddr),
		DataDir:        envOrDefault("AIPERMISSION_BACKUP_DATA_DIR", defaultDataDir),
		MaxUploadBytes: defaultMaxUploadBytes,
	}

	if raw := strings.TrimSpace(os.Getenv("AIPERMISSION_BACKUP_MAX_UPLOAD_BYTES")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 {
			return Config{}, fmt.Errorf("AIPERMISSION_BACKUP_MAX_UPLOAD_BYTES must be a positive integer")
		}
		cfg.MaxUploadBytes = value
	}

	token, err := loadToken()
	if err != nil {
		return Config{}, err
	}
	cfg.Token = token
	return cfg, nil
}

func loadToken() (string, error) {
	if path := strings.TrimSpace(os.Getenv("AIPERMISSION_BACKUP_TOKEN_FILE")); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read backup token file: %w", err)
		}
		return validateToken(strings.TrimSpace(string(raw)))
	}
	return validateToken(strings.TrimSpace(os.Getenv("AIPERMISSION_BACKUP_TOKEN")))
}

func validateToken(token string) (string, error) {
	if token == "" {
		return "", errors.New("AIPERMISSION_BACKUP_TOKEN_FILE or AIPERMISSION_BACKUP_TOKEN is required")
	}
	if len(token) < 32 {
		return "", errors.New("backup token must contain at least 32 characters")
	}
	if strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("backup token must not contain whitespace")
	}
	return token, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
