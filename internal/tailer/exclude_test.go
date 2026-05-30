package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

// fakeFilterEnricher implements Enricher and Filter; it excludes the listed UIDs.
type fakeFilterEnricher struct{ excluded map[string]bool }

func (f *fakeFilterEnricher) Enrich(*model.Record, string) {}
func (f *fakeFilterEnricher) ShouldIngest(uid string) bool { return !f.excluded[uid] }

func writeCRIFile(t *testing.T, root, ns, pod, uid, container string, lines int) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprintf("%s_%s_%s", ns, pod, uid), container)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "0.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < lines; i++ {
		ts := time.Date(2026, 5, 30, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano)
		fmt.Fprintf(f, "%s stdout F line %d\n", ts, i)
	}
}

func runTailer(t *testing.T, cfg Config, enr Enricher) *captureSink {
	t.Helper()
	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cp.Close() })
	sink := &captureSink{}
	cfg.PollInterval = 30 * time.Millisecond
	tr := New(cfg, sink, enr, cp)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	tr.Run(ctx)
	return sink
}

func TestNamespaceExclusion(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pods")
	writeCRIFile(t, root, "prod", "web", "uid-prod", "app", 3)
	writeCRIFile(t, root, "simplelog", "manager", "uid-sl", "manager", 3)

	sink := runTailer(t, Config{Root: root, Node: "n1", ExcludeNamespaces: []string{"simplelog"}}, nil)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.recs {
		if r.Namespace == "simplelog" {
			t.Fatalf("ingested an excluded-namespace record: %+v", r)
		}
	}
	if len(sink.recs) != 3 {
		t.Fatalf("expected 3 prod records, got %d", len(sink.recs))
	}
}

func TestAnnotationExclusion(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pods")
	writeCRIFile(t, root, "prod", "keep", "uid-keep", "app", 4)
	writeCRIFile(t, root, "prod", "drop", "uid-drop", "app", 4)

	enr := &fakeFilterEnricher{excluded: map[string]bool{"uid-drop": true}}
	sink := runTailer(t, Config{Root: root, Node: "n1"}, enr)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.recs) != 4 {
		t.Fatalf("expected 4 records (only the kept pod), got %d", len(sink.recs))
	}
	for _, r := range sink.recs {
		if r.Pod != "keep" {
			t.Fatalf("ingested a record from an annotation-excluded pod: %+v", r)
		}
	}
}
