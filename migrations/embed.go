package migrations

import "embed"

// FS contains embedded goose migration SQL files.
//
// This package exists so PolicyFS can apply migrations at runtime without relying on
// external files being present on the target machine.
//
//go:embed *.sql
var FS embed.FS
