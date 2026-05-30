package store

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
	s3 "github.com/jhunt/go-s3"
	"github.com/oklog/ulid/v2"
)

// These tests run only when SL_TEST_MINIO is set, against a MinIO endpoint
// (default 127.0.0.1:9100, minioadmin/minioadmin). They exercise the real
// jhunt/go-s3 wire path, which the in-process integration test (MemBlobs) does
// not cover.
func minioConfig(t *testing.T) (S3Config, string) {
	t.Helper()
	if os.Getenv("SL_TEST_MINIO") == "" {
		t.Skip("set SL_TEST_MINIO to run MinIO-backed tests")
	}
	endpoint := envOr("SL_TEST_MINIO_ENDPOINT", "127.0.0.1:9100")
	bucket := "simplelog-test-" + strings.ToLower(ulid.Make().String())
	cfg := S3Config{
		AccessKeyID:     envOr("SL_TEST_MINIO_KEY", "minioadmin"),
		SecretAccessKey: envOr("SL_TEST_MINIO_SECRET", "minioadmin"),
		Region:          "us-east-1",
		Bucket:          bucket,
		Domain:          endpoint,
		Protocol:        "http",
		UsePathBuckets:  true,
	}
	// Create the bucket via go-s3 directly.
	c, err := s3.NewClient(&s3.Client{
		AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.SecretAccessKey,
		Region: cfg.Region, Bucket: cfg.Bucket, Domain: cfg.Domain,
		Protocol: cfg.Protocol, UsePathBuckets: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateBucket(bucket, "us-east-1", ""); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return cfg, bucket
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestMinIOBlobsRoundTrip(t *testing.T) {
	cfg, _ := minioConfig(t)
	b, err := NewS3Blobs(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Small object.
	if err := b.Put("a/b/small.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get("a/b/small.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("get small: %q, %v", got, err)
	}

	// Large object (exercises multipart streaming).
	big := make([]byte, 6<<20)
	rand.Read(big)
	if err := b.Put("a/big.bin", big); err != nil {
		t.Fatal(err)
	}
	got, err = b.Get("a/big.bin")
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("big mismatch: len=%d err=%v", len(got), err)
	}

	// List by prefix.
	keys, err := b.List("a/")
	if err != nil || len(keys) != 2 {
		t.Fatalf("list: %v keys=%v", err, keys)
	}

	// Delete + not-found.
	if err := b.Delete("a/b/small.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get("a/b/small.txt"); !IsNotFound(err) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestMinIOSealAndRecover(t *testing.T) {
	cfg, _ := minioConfig(t)
	blobs, err := NewS3Blobs(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)

	// Seal a segment to real S3.
	dir1 := t.TempDir()
	st1, err := Open(Config{ManagerID: "mgr1", BaseDir: dir1}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir1, "in.seg")
	f, _ := os.Create(path)
	w, _ := segment.NewWriter(f, base.UnixNano())
	for i := 0; i < 100; i++ {
		w.Append(&model.Record{
			Namespace: "prod", Pod: "web", Container: "app", Node: "n1",
			Stream: model.StreamStdout, Timestamp: base.Add(time.Duration(i) * time.Second),
			Message: "m",
		})
	}
	meta, _ := w.Finish()
	f.Close()
	if _, err := st1.Seal(path, ulid.Make().String(), meta); err != nil {
		t.Fatal(err)
	}
	st1.Snapshot(ulid.Make().String())
	st1.Close()

	// Fresh manager, empty local dir, recovers purely from S3.
	dir2 := t.TempDir()
	st2, err := Open(Config{ManagerID: "mgr1", BaseDir: dir2}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if err := st2.Recover(); err != nil {
		t.Fatal(err)
	}
	segs, err := st2.Overlapping(base, base.Add(time.Hour))
	if err != nil || len(segs) != 1 {
		t.Fatalf("overlapping: %v segs=%d", err, len(segs))
	}
	rd, c, err := st2.Reader(segs[0]) // downloads whole .seg from S3
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var count int
	for i := range rd.Blocks() {
		rd.ReadBlock(i, func(*model.Record) error { count++; return nil })
	}
	if count != 100 {
		t.Fatalf("read %d records from S3-recovered segment, want 100", count)
	}
}
