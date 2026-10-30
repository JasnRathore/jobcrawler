package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	_ "turso.tech/database/tursogo"
)

type DB struct {
	conn *sql.DB
}

// New opens or creates the local SQLite database and initializes the schema.
func New(ctx context.Context, path string) (*DB, error) {
	if strings.TrimSpace(path) == "" {
		path = "local.db"
	}

	conn, err := sql.Open("turso", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	const schema = `
		CREATE TABLE IF NOT EXISTS company (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE
		);
	`

	if _, err := conn.ExecContext(ctx, schema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	return &DB{conn: conn}, nil
}

// AddCompany inserts a company into the company table.
func (d *DB) AddCompany(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("company name cannot be empty")
	}

	_, err := d.conn.ExecContext(
		ctx,
		"INSERT INTO company (name) VALUES (?)",
		name,
	)
	if err != nil {
		return fmt.Errorf("add company %q: %w", name, err)
	}

	return nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.conn.Close()
}
