package tailer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

func TestParsePath(t *testing.T) {
	p, ok := parsePath("/var/log/pods/prod_web-abc_1234-uid/app/0.log")
	if !ok {
		t.Fatal("expected match")
	}
	if p.Namespace != "prod" || p.Pod != "web-abc" || p.UID != "1234-uid" || p.Container != "app" {
		t.Fatalf("bad parse: %+v", p)
	}
	if _, ok := parsePath("/var/log/something/else.log"); ok {
		t.Fatal("expected no match")
	}
}

func TestParseCRILine(t *testing.T) {
	cl, ok := parseCRILine(`2026-05-30T12:00:00.123456789Z stdout F hello world`)
	if !ok {
		t.Fatal("expected ok")
	}
	if cl.stream != "stdout" || cl.partial || cl.message != "hello world" {
		t.Fatalf("bad line: %+v", cl)
	}
	if _, ok := parseCRILine(`garbage`); ok {
		t.Fatal("expected not ok")
	}
}

func TestBodyForJSONAndText(t *testing.T) {
	j := bodyFor(criLine{stream: "stdout", message: `{"level":"error","msg":"boom"}`})
	if _, ok := j["level"]; !ok {
		t.Fatalf("expected structured body, got %v", j)
	}
	txt := bodyFor(criLine{stream: "stderr", message: "plain text"})
	if _, ok := txt["stderr"]; !ok {
		t.Fatalf("expected stderr body, got %v", txt)
	}
}

// captureSink collects enqueued records.
type captureSink struct {
	mu   sync.Mutex
	recs []*model.Record
}

func (c *captureSink) Enqueue(r *model.Record) error {
	c.mu.Lock()
	c.recs = append(c.recs, r)
	c.mu.Unlock()
	return nil
}

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.recs)
}

func TestTailerEndToEnd(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pods")
	logDir := filepath.Join(root, "prod_web_uid1", "app")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logDir, "0.log")
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}

	writeLine := func(i int) {
		ts := time.Date(2026, 5, 30, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano)
		fmt.Fprintf(f, "%s stdout F line %d\n", ts, i)
	}
	for i := 0; i < 5; i++ {
		writeLine(i)
	}
	f.Sync()

	cp, err := OpenCheckpoint(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()

	sink := &captureSink{}
	tr := New(Config{Root: root, Node: "n1", PollInterval: 50 * time.Millisecond}, sink, nil, cp)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { tr.Run(ctx); close(done) }()

	waitFor(t, func() bool { return sink.count() >= 5 }, 3*time.Second)

	// Append more lines; the running tail should pick them up.
	for i := 5; i < 8; i++ {
		writeLine(i)
	}
	f.Sync()
	waitFor(t, func() bool { return sink.count() >= 8 }, 3*time.Second)

	cancel()
	<-done
	f.Close()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	for i, r := range sink.recs {
		if r.Namespace != "prod" || r.Pod != "web" || r.Container != "app" {
			t.Fatalf("record %d not enriched from path: %+v", i, r)
		}
		if r.Offset <= 0 {
			t.Fatalf("record %d has non-positive offset", i)
		}
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
