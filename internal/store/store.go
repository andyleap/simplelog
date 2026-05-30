// Package store ties together the segment catalog (bbolt on emptyDir), the S3
// object store (authoritative), and a local LRU cache. It implements the
// seal/upload state machine, catalog recovery/reconcile from S3, and read
// access to segments for the query layer.
package store

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
)

const metaSuffix = ".meta"

// Config configures a Store.
type Config struct {
	ManagerID    string
	BaseDir      string // emptyDir root for cache + catalog
	CacheMaxByte int64  // LRU cache cap
}

// Store is the manager's durable-segment layer.
type Store struct {
	cfg   Config
	blobs Blobs
	cat   *catalog
	cache *diskCache

	mu sync.Mutex // serializes seal/upload and catalog snapshot
}

// Open creates a Store, opening the catalog and cache under cfg.BaseDir.
func Open(cfg Config, blobs Blobs) (*Store, error) {
	if cfg.CacheMaxByte <= 0 {
		cfg.CacheMaxByte = 2 << 30 // 2 GiB default
	}
	if err := os.MkdirAll(cfg.BaseDir, 0o755); err != nil {
		return nil, err
	}
	cat, err := openCatalog(filepath.Join(cfg.BaseDir, "catalog.db"))
	if err != nil {
		return nil, err
	}
	cache, err := newDiskCache(filepath.Join(cfg.BaseDir, "cache"), cfg.CacheMaxByte)
	if err != nil {
		cat.close()
		return nil, err
	}
	return &Store{cfg: cfg, blobs: blobs, cat: cat, cache: cache}, nil
}

// Close releases the catalog.
func (s *Store) Close() error { return s.cat.close() }

func (s *Store) segPrefix() string  { return "segments/" + s.cfg.ManagerID + "/" }
func (s *Store) snapPrefix() string { return "catalog/" + s.cfg.ManagerID + "/" }

// segKeyFor builds the S3 key for a segment: a date-prefixed, time-sortable path.
func (s *Store) segKeyFor(m model.SegmentMeta) string {
	d := m.MinTS.UTC()
	return fmt.Sprintf("%s%04d/%02d/%02d/%d-%d-%s.seg",
		s.segPrefix(), d.Year(), d.Month(), d.Day(),
		m.MinTS.UnixMilli(), m.MaxTS.UnixMilli(), m.ID)
}

// Seal takes a freshly written segment file (already containing a valid footer)
// plus its metadata, uploads the .seg and .seg.meta sidecar to S3, registers it
// in the catalog and local cache, and returns the completed metadata. Only
// after Seal returns successfully may the manager ack the contained offsets:
// with no PVC, S3 is the only durable store.
func (s *Store) Seal(localPath, id string, meta model.SegmentMeta) (model.SegmentMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta.ID = id
	meta.S3Key = s.segKeyFor(meta)

	// Upload the segment body, then the sidecar. Ordering matters: a segment is
	// only discoverable via its .meta sidecar, so uploading .seg first means a
	// crash between the two leaves an orphan .seg (harmless; reconcile ignores
	// it) rather than a .meta pointing at a missing body.
	f, err := os.Open(localPath)
	if err != nil {
		return meta, err
	}
	if err := s.blobs.PutReader(meta.S3Key, f); err != nil {
		f.Close()
		return meta, err
	}
	f.Close()

	sidecar, err := marshalSidecar(meta)
	if err != nil {
		return meta, err
	}
	if err := s.blobs.Put(meta.S3Key+metaSuffix, sidecar); err != nil {
		return meta, err
	}

	// Move the file into the cache dir and register it.
	cachePath := s.cache.pathFor(id)
	if localPath != cachePath {
		if err := os.Rename(localPath, cachePath); err != nil {
			// Fall back to leaving it where it is; record that path.
			cachePath = localPath
		}
	}
	meta.LocalPath = cachePath
	if err := s.cat.put(meta); err != nil {
		return meta, err
	}
	s.cache.admit(id, meta.SizeBytes)
	return meta, nil
}

// Overlapping returns cataloged segments intersecting [start, end], ascending.
func (s *Store) Overlapping(start, end time.Time) ([]model.SegmentMeta, error) {
	return s.cat.overlapping(start, end)
}

// SourceWatermarks returns the highest committed (sealed) offset per source key
// across all cataloged segments. The manager seeds its dedup watermark from
// this after recovery so already-sealed records that get redelivered (because
// their ack was lost on restart) are dropped rather than re-sealed.
func (s *Store) SourceWatermarks() (map[string]int64, error) {
	segs, err := s.cat.all()
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, m := range segs {
		for key, off := range m.SourceOffsets {
			if off > out[key] {
				out[key] = off
			}
		}
	}
	return out, nil
}

// Reader returns a segment.Reader for the segment, ensuring it is present in the
// local cache (downloading the whole .seg from S3 if necessary). The returned
// closer releases the underlying file handle.
func (s *Store) Reader(m model.SegmentMeta) (*segment.Reader, io.Closer, error) {
	path, err := s.ensureLocal(m)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	rd, err := segment.Open(f, fi.Size())
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return rd, f, nil
}

// ensureLocal returns a local path for the segment, downloading it from S3 if
// not cached.
func (s *Store) ensureLocal(m model.SegmentMeta) (string, error) {
	if path, ok := s.cache.touch(m.ID); ok {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	data, err := s.blobs.Get(m.S3Key)
	if err != nil {
		return "", err
	}
	path := s.cache.pathFor(m.ID)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", err
	}
	s.cache.admit(m.ID, int64(len(data)))
	return path, nil
}

// Recover rebuilds the catalog after a pod reschedule wiped the emptyDir. It
// loads the newest catalog snapshot (fast path) if present, then lists segment
// .meta sidecars in S3 and backfills any segment missing from the catalog
// (ground truth). The catalog is therefore fully derivable from S3.
func (s *Store) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.loadLatestSnapshot(); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	}

	keys, err := s.blobs.List(s.segPrefix())
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, metaSuffix) {
			continue
		}
		segKey := strings.TrimSuffix(k, metaSuffix)
		id := idFromSegKey(segKey)
		if id == "" {
			continue
		}
		if ok, err := s.cat.has(id); err != nil {
			return err
		} else if ok {
			continue
		}
		raw, err := s.blobs.Get(k)
		if err != nil {
			if IsNotFound(err) {
				continue
			}
			return err
		}
		meta, err := unmarshalSidecar(raw)
		if err != nil {
			return fmt.Errorf("parse sidecar %s: %w", k, err)
		}
		meta.ID = id
		meta.S3Key = segKey
		meta.LocalPath = "" // not cached locally after reschedule
		if err := s.cat.put(meta); err != nil {
			return err
		}
	}
	return nil
}

// loadLatestSnapshot downloads the newest catalog snapshot from S3 and merges
// its entries into the local catalog. Missing snapshots are not an error.
func (s *Store) loadLatestSnapshot() error {
	keys, err := s.blobs.List(s.snapPrefix())
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	// Keys are ulid-suffixed, so lexical max is the newest.
	latest := keys[0]
	for _, k := range keys[1:] {
		if k > latest {
			latest = k
		}
	}
	raw, err := s.blobs.Get(latest)
	if err != nil {
		if IsNotFound(err) {
			return nil
		}
		return err
	}
	// The snapshot is a bbolt file; open it read-only from a temp path and copy
	// its entries in. We avoid replacing our live db so an older snapshot can't
	// clobber newer reconciled entries.
	tmp, err := os.CreateTemp(s.cfg.BaseDir, "snap-*.db")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()

	other, err := openCatalog(tmpPath)
	if err != nil {
		return err
	}
	defer other.close()
	metas, err := other.all()
	if err != nil {
		return err
	}
	for _, m := range metas {
		m.LocalPath = "" // emptyDir wiped; nothing cached
		if ok, err := s.cat.has(m.ID); err == nil && !ok {
			if err := s.cat.put(m); err != nil {
				return err
			}
		}
	}
	return nil
}

// Snapshot uploads a consistent copy of the catalog to S3 under a ULID key.
func (s *Store) Snapshot(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, err := s.cat.snapshot()
	if err != nil {
		return err
	}
	return s.blobs.PutReader(s.snapPrefix()+id+".snap", bytes.NewReader(buf))
}

// idFromSegKey extracts the ULID from a segment key of the form
// .../<minMs>-<maxMs>-<ulid>.seg.
func idFromSegKey(segKey string) string {
	base := filepath.Base(segKey)
	base = strings.TrimSuffix(base, ".seg")
	i := strings.LastIndexByte(base, '-')
	if i < 0 {
		return ""
	}
	return base[i+1:]
}
