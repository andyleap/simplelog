package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
)

// writeSegment builds a small sealed segment file and returns its path + meta.
func writeSegment(t *testing.T, dir string, base time.Time, ns string, n int) (string, model.SegmentMeta) {
	t.Helper()
	path := filepath.Join(dir, "incoming.seg")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := segment.NewWriter(f, base.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		r := &model.Record{
			Namespace: ns, Pod: "web", Container: "app", Node: "n1",
			Stream: model.StreamStdout, Message: "m",
			Timestamp: base.Add(time.Duration(i) * time.Second),
		}
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	return path, meta
}

func TestSealAndQuery(t *testing.T) {
	dir := t.TempDir()
	blobs := NewMemBlobs()
	st, err := Open(Config{ManagerID: "mgr1", BaseDir: dir, CacheMaxByte: 1 << 30}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	path, meta := writeSegment(t, dir, base, "prod", 100)
	sealed, err := st.Seal(path, "ID0001", meta)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.S3Key == "" {
		t.Fatal("expected S3Key set")
	}
	// Both .seg and .seg.meta must be in S3.
	if _, err := blobs.Get(sealed.S3Key); err != nil {
		t.Fatalf("seg missing in S3: %v", err)
	}
	if _, err := blobs.Get(sealed.S3Key + metaSuffix); err != nil {
		t.Fatalf("sidecar missing in S3: %v", err)
	}

	segs, err := st.Overlapping(base, base.Add(time.Hour))
	if err != nil || len(segs) != 1 {
		t.Fatalf("overlapping = %v, %v", segs, err)
	}
	rd, c, err := st.Reader(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var count int
	for i := range rd.Blocks() {
		rd.ReadBlock(i, func(r *model.Record) error { count++; return nil })
	}
	if count != 100 {
		t.Fatalf("read %d records, want 100", count)
	}
}

func TestRecoverFromS3(t *testing.T) {
	blobs := NewMemBlobs()
	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)

	// First manager seals two segments, then "loses" its emptyDir.
	dir1 := t.TempDir()
	st1, err := Open(Config{ManagerID: "mgr1", BaseDir: dir1}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	for i, ns := range []string{"prod", "staging"} {
		p, m := writeSegment(t, dir1, base.Add(time.Duration(i)*time.Minute), ns, 10)
		if _, err := st1.Seal(p, "ID000"+string(rune('1'+i)), m); err != nil {
			t.Fatal(err)
		}
	}
	st1.Snapshot("SNAP01")
	st1.Close()

	// Fresh manager on a clean emptyDir recovers purely from S3.
	dir2 := t.TempDir()
	st2, err := Open(Config{ManagerID: "mgr1", BaseDir: dir2}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if err := st2.Recover(); err != nil {
		t.Fatal(err)
	}
	segs, err := st2.all()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 2 {
		t.Fatalf("recovered %d segments, want 2", len(segs))
	}
	// And we can still read a recovered segment (downloaded from S3, not cached).
	rd, c, err := st2.Reader(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if rd.Meta().RecordCount != 10 {
		t.Fatalf("record count = %d, want 10", rd.Meta().RecordCount)
	}
}

// all is a test helper exposing catalog.all.
func (s *Store) all() ([]model.SegmentMeta, error) { return s.cat.all() }
