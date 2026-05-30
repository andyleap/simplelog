package tailer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

// gatedEnricher implements Enricher + ReadyChecker + Filter. It reports not-ready
// until markReady is called, and stamps a label on enrich so tests can confirm
// records were enriched before being shipped.
type gatedEnricher struct {
	mu    sync.Mutex
	ready bool
}

func (g *gatedEnricher) markReady() {
	g.mu.Lock()
	g.ready = true
	g.mu.Unlock()
}

func (g *gatedEnricher) Ready(string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ready
}

func (g *gatedEnricher) ShouldIngest(string) bool { return true }

func (g *gatedEnricher) Enrich(r *model.Record, _ string) {
	if r.Labels == nil {
		r.Labels = map[string]string{}
	}
	r.Labels["enriched"] = "yes"
}

func TestHoldsUntilMetadataReady(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pods")
	writeCRIFile(t, root, "prod", "web", "uid1", "app", 5)

	g := &gatedEnricher{}
	cp, _ := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	sink := &captureSink{}
	tr := New(Config{Root: root, Node: "n1", PollInterval: 20 * time.Millisecond, MetadataWait: 5 * time.Second}, sink, g, cp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)

	// While metadata is unavailable, nothing should be shipped.
	time.Sleep(300 * time.Millisecond)
	if n := sink.count(); n != 0 {
		t.Fatalf("shipped %d records before metadata was ready, want 0", n)
	}

	// Once metadata is ready, records flow — and are enriched.
	g.markReady()
	waitFor(t, func() bool { return sink.count() >= 5 }, 3*time.Second)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, r := range sink.recs {
		if r.Labels["enriched"] != "yes" {
			t.Fatalf("record shipped without enrichment: %+v", r)
		}
	}
}

func TestProceedsAfterMetadataTimeout(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pods")
	writeCRIFile(t, root, "prod", "web", "uid1", "app", 3)

	g := &gatedEnricher{} // never becomes ready
	cp, _ := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	sink := &captureSink{}
	// Short grace: after it elapses, the tail proceeds (un-enriched) rather than
	// pinning the descriptor forever (e.g. logs of an already-deleted pod).
	tr := New(Config{Root: root, Node: "n1", PollInterval: 20 * time.Millisecond, MetadataWait: 150 * time.Millisecond}, sink, g, cp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tr.Run(ctx)

	waitFor(t, func() bool { return sink.count() >= 3 }, 3*time.Second)
}
