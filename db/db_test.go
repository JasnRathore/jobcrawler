package db

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReplaceDatabaseContentsReplacesLocalRows(t *testing.T) {
	ctx := context.Background()
	source, err := New(ctx, filepath.Join(t.TempDir(), "remote.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()

	destination, err := New(ctx, filepath.Join(t.TempDir(), "local.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()

	if _, err := source.conn.ExecContext(ctx, `INSERT INTO provider_companies(provider, name) VALUES ('lever', 'remote-company')`); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.conn.ExecContext(ctx, `INSERT INTO provider_companies(provider, name) VALUES ('greenhouse', 'stale-company')`); err != nil {
		t.Fatal(err)
	}

	if err := replaceDatabaseContents(ctx, source.conn, destination.conn); err != nil {
		t.Fatal(err)
	}

	rows, err := destination.conn.QueryContext(ctx, `SELECT provider, name FROM provider_companies`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected remote company row")
	}
	var provider, name string
	if err := rows.Scan(&provider, &name); err != nil {
		t.Fatal(err)
	}
	if provider != "lever" || name != "remote-company" {
		t.Fatalf("copied row = (%q, %q), want (lever, remote-company)", provider, name)
	}
	if rows.Next() {
		t.Fatal("local stale rows were not replaced")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
