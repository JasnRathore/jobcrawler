package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type databaseConfig struct {
	mode       string
	localPath  string
	tursoURL   string
	tursoToken string
}

func loadDatabaseConfig(envPath string) (databaseConfig, error) {
	if err := loadDotEnv(envPath); err != nil {
		return databaseConfig{}, err
	}

	config := databaseConfig{
		mode:      strings.ToLower(strings.TrimSpace(os.Getenv("DB_MODE"))),
		localPath: strings.TrimSpace(os.Getenv("LOCAL_DB_PATH")),
		tursoURL:  strings.TrimSpace(os.Getenv("TURSO_DATABASE_URL")),
	}
	config.tursoToken = strings.TrimSpace(os.Getenv("TURSO_AUTH_TOKEN"))
	if config.mode == "" {
		config.mode = "local"
	}
	switch config.mode {
	case "local":
		if config.localPath == "" {
			config.localPath = "local.db"
		}
	case "remote":
		if config.localPath == "" {
			config.localPath = "local.db"
		}
		if config.tursoURL == "" {
			return databaseConfig{}, errors.New("TURSO_DATABASE_URL is required when DB_MODE=remote")
		}
		if config.tursoToken == "" {
			return databaseConfig{}, errors.New("TURSO_AUTH_TOKEN is required when DB_MODE=remote")
		}
	default:
		return databaseConfig{}, fmt.Errorf("DB_MODE must be local or remote, got %q", config.mode)
	}
	return config, nil
}

func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open environment file %q: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return fmt.Errorf("invalid environment assignment in %s at line %d", path, lineNumber)
		}
		value = strings.TrimSpace(value)
		value, err = parseDotEnvValue(value)
		if err != nil {
			return fmt.Errorf("invalid value in %s at line %d: %w", path, lineNumber, err)
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set environment variable %q: %w", key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read environment file %q: %w", path, err)
	}
	return nil
}

func parseDotEnvValue(value string) (string, error) {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		return strconv.Unquote(value)
	}
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1], nil
	}
	if comment := strings.Index(value, " #"); comment >= 0 {
		value = strings.TrimSpace(value[:comment])
	}
	return value, nil
}
