package query

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
	"github.com/andyleap/simplelog/internal/store"
)

func buildStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(store.Config{ManagerID: "m", BaseDir: dir}, store.NewMemBlobs())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)

	// Two segments, interleaved namespaces, multiple blocks each.
	for s := 0; s < 2; s++ {
		path := filepath.Join(dir, "in.seg")
		f, _ := os.Create(path)
		w, _ := segment.NewWriter(f, base.UnixNano())
		for i := 0; i < 50; i++ {
			ns := "prod"
			if i%2 == 0 {
				ns = "staging"
			}
			ts := base.Add(time.Duration(s*50+i) * time.Second)
			body := map[string]json.RawMessage{}
			lvl, _ := json.Marshal(map[bool]string{true: "error", false: "info"}[i%5 == 0])
			body["level"] = lvl
			w.Append(&model.Record{
				Namespace: ns, Pod: "web", Container: "app", Node: "n1",
				Stream: model.StreamStdout, Timestamp: ts, Message: "m", Body: body,
			})
		}
		meta, _ := w.Finish()
		f.Close()
		if _, err := st.Seal(path, "SEG"+string(rune('A'+s)), meta); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func runQuery(t *testing.T, st *store.Store, expr string, desc bool, limit int) []*model.Record {
	t.Helper()
	node, err := Parse(expr)
	if err != nil {
		t.Fatal(err)
	}
	ex := &Executor{Store: st}
	var out []*model.Record
	err = ex.Run(Request{Expr: node, Desc: desc, Limit: limit}, func(r *model.Record) error {
		out = append(out, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPlannerMergeAndFilter(t *testing.T) {
	st := buildStore(t)
	defer st.Close()

	// All 100 records, ascending and time-ordered.
	all := runQuery(t, st, "", false, 0)
	if len(all) != 100 {
		t.Fatalf("got %d records, want 100", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].Timestamp.Before(all[i-1].Timestamp) {
			t.Fatalf("not ascending at %d", i)
		}
	}

	// Descending.
	desc := runQuery(t, st, "", true, 0)
	if len(desc) != 100 {
		t.Fatalf("desc got %d, want 100", len(desc))
	}
	for i := 1; i < len(desc); i++ {
		if desc[i].Timestamp.After(desc[i-1].Timestamp) {
			t.Fatalf("not descending at %d", i)
		}
	}

	// Filter by namespace.
	prod := runQuery(t, st, `_namespace="prod"`, false, 0)
	if len(prod) != 50 {
		t.Fatalf("prod got %d, want 50", len(prod))
	}

	// Structured field filter.
	errs := runQuery(t, st, `level="error"`, false, 0)
	if len(errs) != 20 { // every 5th of 100
		t.Fatalf("errors got %d, want 20", len(errs))
	}

	// Limit.
	lim := runQuery(t, st, "", false, 7)
	if len(lim) != 7 {
		t.Fatalf("limit got %d, want 7", len(lim))
	}
}

func TestPlannerOpenBuffer(t *testing.T) {
	st := buildStore(t)
	defer st.Close()
	base := time.Date(2026, 5, 30, 13, 0, 0, 0, time.UTC) // after sealed segments
	open := []*model.Record{
		{Namespace: "prod", Pod: "web", Stream: model.StreamStdout, Timestamp: base, Message: "live"},
	}
	node, _ := Parse(`_namespace="prod"`)
	ex := &Executor{Store: st, Open: func(_, _ time.Time) []*model.Record { return open }}
	var got int
	ex.Run(Request{Expr: node}, func(r *model.Record) error { got++; return nil })
	if got != 51 { // 50 sealed prod + 1 open
		t.Fatalf("got %d, want 51", got)
	}
}

func TestPrunablePredicates(t *testing.T) {
	mustReq := func(expr string, want int) {
		n, _ := Parse(expr)
		if got := len(prunablePredicates(n)); got != want {
			t.Errorf("%q: %d predicates, want %d", expr, got, want)
		}
	}
	mustReq(`_namespace="prod"`, 1)
	mustReq(`_namespace="prod" and _pod="web"`, 2)
	mustReq(`_namespace="prod" or _pod="web"`, 0) // different fields, no common requirement
	mustReq(`_namespace="prod" or _namespace="prod"`, 1)
	mustReq(`not _namespace="prod"`, 0)
	mustReq(`level="error"`, 0) // body field, not prunable
}

func TestHub(t *testing.T) {
	h := NewHub(4)
	node, _ := Parse(`_namespace="prod"`)
	sub := h.Subscribe(node)
	defer sub.Close()

	h.Publish(&model.Record{Namespace: "prod", Timestamp: time.Now()})
	h.Publish(&model.Record{Namespace: "dev", Timestamp: time.Now()}) // filtered out

	select {
	case r := <-sub.C:
		if r.Namespace != "prod" {
			t.Fatalf("got namespace %q", r.Namespace)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a published record")
	}
	select {
	case r := <-sub.C:
		t.Fatalf("unexpected second record: %v", r)
	default:
	}
}
