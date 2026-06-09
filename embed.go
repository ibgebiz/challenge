// Package notifier is the module root. It exposes embedded assets (database
// migrations) so binaries can run migrations without external files.
package notifier

import "embed"

// MigrationsFS holds the versioned SQL migrations under the "migrations" dir.
//
//go:embed migrations/*.sql
var MigrationsFS embed.FS
