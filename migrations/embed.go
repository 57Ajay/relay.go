// Package migrations bakes the .sql files into the binary,
// so the deployed image doesn't need the files on disk.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
