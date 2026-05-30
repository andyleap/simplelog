// Package buildinfo exposes version metadata for log output. Values are set by
// GoReleaser via -ldflags -X; for a plain `go build` they fall back to the VCS
// info Go embeds automatically, so local builds still report a commit.
package buildinfo

import "runtime/debug"

var (
	// Version is the release version (e.g. "v1.2.3"), or "dev" for local builds.
	Version = "dev"
	// Commit is the git revision.
	Commit = ""
	// Date is the build timestamp (RFC3339).
	Date = ""
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if Commit == "" {
				Commit = s.Value
			}
		case "vcs.time":
			if Date == "" {
				Date = s.Value
			}
		}
	}
}

// String renders a one-line version summary like
// "v1.2.3 (commit abcdef012345, built 2026-05-30T12:00:00Z)".
func String() string {
	out := Version
	commit := Commit
	if len(commit) > 12 {
		commit = commit[:12]
	}
	switch {
	case commit != "" && Date != "":
		out += " (commit " + commit + ", built " + Date + ")"
	case commit != "":
		out += " (commit " + commit + ")"
	}
	return out
}
