module crawler

go 1.27.3

require db v0.0.0

replace db => ./db

require (
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/tursodatabase/turso-go-platform-libs v0.8.3 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	turso.tech/database/tursogo v0.8.3 // indirect
)
