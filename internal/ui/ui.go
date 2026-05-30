// Package ui embeds the manager's static web UI. The assets in dist/ are plain
// ES-module JS/HTML/CSS with no build step, so `go build` needs no JS toolchain.
// The embed boundary (dist/) means a future framework build can be dropped in
// here without changing the manager.
package ui

import (
	"embed"
	"io/fs"
)

//go:embed dist
var embedded embed.FS

// Assets returns the UI file tree rooted at dist/.
func Assets() fs.FS {
	sub, err := fs.Sub(embedded, "dist")
	if err != nil {
		panic(err) // dist is embedded at build time; this cannot fail
	}
	return sub
}
