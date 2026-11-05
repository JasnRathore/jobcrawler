package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDatabaseConfigLocalDefaults(t *testing.T) {
	withoutEnv(t, "DB_MODE", "LOCAL_DB_PATH", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")
	config, err := loadDatabaseConfig(filepath.Join(t.TempDir(), ".env"))
	if err != nil {
		t.Fatal(err)
	}
	if config.mode != "local" || config.localPath != "local.db" {
		t.Fatalf("config = %+v, want local defaults", config)
	}
}

func TestLoadDatabaseConfigRemoteFromDotEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DB_MODE=remote\nLOCAL_DB_PATH=\"replica.db\"\nTURSO_DATABASE_URL=libsql://example.turso.io\nTURSO_AUTH_TOKEN='token value'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	withoutEnv(t, "DB_MODE", "LOCAL_DB_PATH", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")
	config, err := loadDatabaseConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.mode != "remote" || config.localPath != "replica.db" ||
		config.tursoURL != "libsql://example.turso.io" || config.tursoToken != "token value" {
		t.Fatalf("unexpected remote config: %+v", config)
	}
}

func TestLoadDatabaseConfigEnvironmentOverridesDotEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DB_MODE=remote\nTURSO_DATABASE_URL=libsql://file.turso.io\nTURSO_AUTH_TOKEN=file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	withEnv(t, map[string]string{
		"DB_MODE": "local", "LOCAL_DB_PATH": "environment.db",
		"TURSO_DATABASE_URL": "", "TURSO_AUTH_TOKEN": "",
	})
	config, err := loadDatabaseConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.mode != "local" || config.localPath != "environment.db" {
		t.Fatalf("environment did not override .env: %+v", config)
	}
}

func TestLoadDatabaseConfigRequiresRemoteCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DB_MODE=remote\nLOCAL_DB_PATH=replica.db\n"), 0600); err != nil {
		t.Fatal(err)
	}
	withoutEnv(t, "DB_MODE", "LOCAL_DB_PATH", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")
	_, err := loadDatabaseConfig(path)
	if err == nil || !strings.Contains(err.Error(), "TURSO_DATABASE_URL") {
		t.Fatal("remote mode accepted missing URL and token")
	}
}

func TestLoadDatabaseConfigRemoteDoesNotChooseLocalPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("DB_MODE=remote\nTURSO_DATABASE_URL=libsql://example.turso.io\nTURSO_AUTH_TOKEN=test-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	withoutEnv(t, "DB_MODE", "LOCAL_DB_PATH", "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN")
	config, err := loadDatabaseConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.localPath != "" {
		t.Fatalf("remote config local path = %q, want empty", config.localPath)
	}
}

func withEnv(t *testing.T, values map[string]string) {
	t.Helper()
	old := make(map[string]string, len(values))
	existed := make(map[string]bool, len(values))
	for key := range values {
		old[key], existed[key] = os.LookupEnv(key)
	}
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	t.Cleanup(func() {
		for key := range values {
			if existed[key] {
				if err := os.Setenv(key, old[key]); err != nil {
					t.Errorf("restore %s: %v", key, err)
				}
			} else if err := os.Unsetenv(key); err != nil {
				t.Errorf("unset %s: %v", key, err)
			}
		}
	})
}

func withoutEnv(t *testing.T, keys ...string) {
	t.Helper()
	old := make(map[string]string, len(keys))
	existed := make(map[string]bool, len(keys))
	for _, key := range keys {
		old[key], existed[key] = os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
	t.Cleanup(func() {
		for _, key := range keys {
			if existed[key] {
				if err := os.Setenv(key, old[key]); err != nil {
					t.Errorf("restore %s: %v", key, err)
				}
			} else if err := os.Unsetenv(key); err != nil {
				t.Errorf("unset %s: %v", key, err)
			}
		}
	})
}
