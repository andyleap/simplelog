package segment

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

func rec(ns, pod string, ts time.Time, body map[string]any) *model.Record {
	r := &model.Record{
		Namespace: ns,
		Pod:       pod,
		Node:      "node-a",
		Container: "app",
		Stream:    model.StreamStdout,
		Timestamp: ts,
		Message:   "hi",
		Labels:    map[string]string{"app": pod},
	}
	if body != nil {
		r.Body = map[string]json.RawMessage{}
		for k, v := range body {
			b, _ := json.Marshal(v)
			r.Body[k] = b
		}
	}
	return r
}

func TestWriteReadRoundTrip(t *testing.T) {
	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	w, err := NewWriter(&buf, base.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	w.blockTarget = 256 // tiny to force multiple blocks

	const n = 500
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		if err := w.Append(rec("prod", "web", ts, map[string]any{"level": "info", "i": i})); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Blocks) < 2 {
		t.Fatalf("expected multiple blocks, got %d", len(meta.Blocks))
	}
	if meta.RecordCount != n {
		t.Fatalf("record count = %d, want %d", meta.RecordCount, n)
	}

	rd, err := Open(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	if got := rd.Meta().RecordCount; got != n {
		t.Fatalf("reader record count = %d, want %d", got, n)
	}
	if !contains(rd.Meta().Namespaces, "prod") {
		t.Fatalf("namespaces summary missing prod: %v", rd.Meta().Namespaces)
	}
	if !contains(rd.Meta().FieldKeys, "level") {
		t.Fatalf("field keys missing level: %v", rd.Meta().FieldKeys)
	}

	var seen int
	var last time.Time
	for i := range rd.Blocks() {
		err := rd.ReadBlock(i, func(r *model.Record) error {
			if !last.IsZero() && r.Timestamp.Before(last) {
				t.Fatalf("records out of order at %d", seen)
			}
			last = r.Timestamp
			seen++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if seen != n {
		t.Fatalf("read %d records, want %d", seen, n)
	}
}

func TestUnsealedDetection(t *testing.T) {
	base := time.Now()
	var buf bytes.Buffer
	w, _ := NewWriter(&buf, base.UnixNano())
	_ = w.Append(rec("prod", "web", base, nil))
	_, _ = w.Finish()

	// Truncate the footer: should be detected as unsealed.
	trunc := buf.Bytes()[:buf.Len()-10]
	if _, err := Open(bytes.NewReader(trunc), int64(len(trunc))); err != ErrUnsealed {
		t.Fatalf("expected ErrUnsealed, got %v", err)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
