package query

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

func mkRec() *model.Record {
	body := map[string]json.RawMessage{}
	put := func(k string, v any) { b, _ := json.Marshal(v); body[k] = b }
	put("level", "error")
	put("status", 500)
	put("ok", false)
	put("user", map[string]any{"id": 42, "name": "ada"})
	return &model.Record{
		Namespace: "prod",
		Pod:       "web-abc",
		Container: "app",
		Node:      "node-1",
		Stream:    model.StreamStdout,
		Message:   "request failed: panic in handler",
		Timestamp: time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
		Labels:    map[string]string{"app": "web", "app.kubernetes.io/name": "frontend"},
		Body:      body,
	}
}

func match(t *testing.T, expr string, want bool) {
	t.Helper()
	n, err := Parse(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	if got := n.Match(mkRec()); got != want {
		t.Errorf("%q => %v, want %v", expr, got, want)
	}
}

func TestEvaluation(t *testing.T) {
	match(t, ``, true)
	match(t, `_namespace="prod"`, true)
	match(t, `_namespace="dev"`, false)
	match(t, `_namespace != "dev"`, true)
	match(t, `_pod =~ /web-.*/`, true)
	match(t, `_pod !~ /api-.*/`, true)

	// Body fields, bare and explicit.
	match(t, `level="error"`, true)
	match(t, `body.level="error"`, true)
	match(t, `level="info"`, false)

	// Numeric coercion: status stored as JSON number; literal numeric.
	match(t, `status >= 500`, true)
	match(t, `status > 500`, false)
	match(t, `status = 500`, true)
	match(t, `status < 400`, false)

	// Bool.
	match(t, `ok = false`, true)
	match(t, `ok = true`, false)

	// Nested path.
	match(t, `user.id = 42`, true)
	match(t, `body.user.name = "ada"`, true)

	// Contains and message.
	match(t, `_message : "panic"`, true)
	match(t, `_message : "success"`, false)

	// Labels, including bracket form for dotted keys.
	match(t, `label.app = "web"`, true)
	match(t, `label["app.kubernetes.io/name"] = "frontend"`, true)

	// Absent field semantics.
	match(t, `missing = "x"`, false)
	match(t, `missing != "x"`, true)
	match(t, `missing`, false)
	match(t, `level`, true)

	// Boolean composition + precedence (and binds tighter than or).
	match(t, `_namespace="dev" or level="error"`, true)
	match(t, `_namespace="prod" and level="error"`, true)
	match(t, `_namespace="prod" and level="info"`, false)
	match(t, `not level="info"`, true)
	match(t, `_namespace="dev" or _namespace="prod" and level="error"`, true)
	match(t, `(_namespace="dev" or _namespace="prod") and level="info"`, false)
}

func TestParseErrors(t *testing.T) {
	for _, expr := range []string{
		`_pod =~ /[/`,    // bad regex
		`level =`,        // missing value
		`level = `,       // missing value
		`(level="error"`, // unbalanced paren
		`= "x"`,          // missing field
		`level == "x"`,   // invalid operator sequence
	} {
		if _, err := Parse(expr); err == nil {
			t.Errorf("expected parse error for %q", expr)
		}
	}
}
