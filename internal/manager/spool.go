// Package manager implements the log manager: the in-memory open segment
// (ingest sink + dedup + commit watermarks), the rotation/seal/upload loop, and
// the HTTP query/streaming API. Durability lives in package store (S3); this
// package keeps the hot path in memory and seals to store on size/age/shutdown.
package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/query"
	"github.com/andyleap/simplelog/internal/segment"
	"github.com/andyleap/simplelog/internal/store"
	"github.com/oklog/ulid/v2"
)

// SpoolConfig configures the open-segment buffer and rotation policy.
type SpoolConfig struct {
	WorkDir     string        // scratch dir for in-progress segment files
	MaxSegBytes int64         // seal when the open buffer reaches this (approx JSON bytes)
	MaxSegAge   time.Duration // seal when the oldest open record is this old
}

func (c *SpoolConfig) defaults() {
	if c.MaxSegBytes <= 0 {
		c.MaxSegBytes = 64 << 20
	}
	if c.MaxSegAge <= 0 {
		c.MaxSegAge = 5 * time.Minute
	}
}

// Spool is the manager's open segment: it accepts records (deduplicated by
// (source, offset)), serves them to live queries, and seals them to the store.
// It implements ingest.Receiver and query.OpenSnapshot.
type Spool struct {
	cfg   SpoolConfig
	store *store.Store
	hub   *query.Hub

	mu        sync.Mutex
	buf       []*model.Record
	bufBytes  int64
	openSince time.Time
	received  map[string]int64 // dedup watermark: highest accepted offset per source
	committed map[string]int64 // highest S3-sealed offset per source
}

// NewSpool creates a Spool, seeding its dedup/commit watermark from the
// recovered catalog so already-sealed records that get redelivered after a
// restart are dropped (and re-acked) rather than re-sealed.
func NewSpool(cfg SpoolConfig, st *store.Store, hub *query.Hub) (*Spool, error) {
	cfg.defaults()
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		return nil, err
	}
	watermarks, err := st.SourceWatermarks()
	if err != nil {
		return nil, err
	}
	received := make(map[string]int64, len(watermarks))
	committed := make(map[string]int64, len(watermarks))
	for k, v := range watermarks {
		received[k] = v
		committed[k] = v
	}
	return &Spool{
		cfg:       cfg,
		store:     st,
		hub:       hub,
		received:  received,
		committed: committed,
	}, nil
}

// Accept ingests a record, dropping duplicates. It is safe for concurrent use.
func (s *Spool) Accept(r *model.Record) {
	key := r.Source.Key()
	s.mu.Lock()
	if hw, ok := s.received[key]; ok && r.Offset <= hw {
		s.mu.Unlock()
		return // duplicate (re-sent after reconnect)
	}
	s.received[key] = r.Offset
	rec := *r
	s.buf = append(s.buf, &rec)
	s.bufBytes += int64(estimateSize(&rec))
	if len(s.buf) == 1 {
		s.openSince = time.Now()
	}
	s.mu.Unlock()

	if s.hub != nil {
		s.hub.Publish(&rec)
	}
}

// Committed returns a snapshot of the committed offsets per source.
func (s *Spool) Committed() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.committed))
	for k, v := range s.committed {
		out[k] = v
	}
	return out
}

// Snapshot returns open-buffer records in [start,end], ascending by timestamp.
// Implements query.OpenSnapshot.
func (s *Spool) Snapshot(start, end time.Time) []*model.Record {
	s.mu.Lock()
	var out []*model.Record
	for _, r := range s.buf {
		if !start.IsZero() && r.Timestamp.Before(start) {
			continue
		}
		if !end.IsZero() && r.Timestamp.After(end) {
			continue
		}
		out = append(out, r)
	}
	s.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	return out
}

// shouldSeal reports whether the open buffer has hit a rotation threshold.
func (s *Spool) shouldSeal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buf) == 0 {
		return false
	}
	if s.bufBytes >= s.cfg.MaxSegBytes {
		return true
	}
	return time.Since(s.openSince) >= s.cfg.MaxSegAge
}

// Seal writes the current open buffer to a sealed segment, uploads it via the
// store, and advances the committed watermark. Returns the number of records
// sealed. A no-op (returns 0) when the buffer is empty.
func (s *Spool) Seal() (int, error) {
	s.mu.Lock()
	if len(s.buf) == 0 {
		s.mu.Unlock()
		return 0, nil
	}
	batch := s.buf
	s.buf = nil
	s.bufBytes = 0
	s.mu.Unlock()

	sort.Slice(batch, func(i, j int) bool { return batch[i].Timestamp.Before(batch[j].Timestamp) })

	id := ulid.Make().String()
	tmpPath := filepath.Join(s.cfg.WorkDir, id+".seg.tmp")
	f, err := os.Create(tmpPath)
	if err != nil {
		s.requeue(batch)
		return 0, err
	}
	w, err := segment.NewWriter(f, time.Now().UnixNano())
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		s.requeue(batch)
		return 0, err
	}
	// Track the max offset per source so we can advance committed after upload.
	maxOff := map[string]int64{}
	for _, r := range batch {
		if err := w.Append(r); err != nil {
			f.Close()
			os.Remove(tmpPath)
			s.requeue(batch)
			return 0, err
		}
		if r.Offset > maxOff[r.Source.Key()] {
			maxOff[r.Source.Key()] = r.Offset
		}
	}
	meta, err := w.Finish()
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		s.requeue(batch)
		return 0, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		s.requeue(batch)
		return 0, err
	}

	if _, err := s.store.Seal(tmpPath, id, meta); err != nil {
		os.Remove(tmpPath)
		s.requeue(batch)
		return 0, fmt.Errorf("seal segment: %w", err)
	}

	// Durable now: advance committed offsets so acks can release agent data.
	s.mu.Lock()
	for key, off := range maxOff {
		if off > s.committed[key] {
			s.committed[key] = off
		}
	}
	s.mu.Unlock()
	return len(batch), nil
}

// requeue puts a failed batch back at the front of the open buffer so a later
// seal retries it. Records remain deduplicated (received watermark unchanged).
func (s *Spool) requeue(batch []*model.Record) {
	s.mu.Lock()
	s.buf = append(batch, s.buf...)
	for _, r := range batch {
		s.bufBytes += int64(estimateSize(r))
	}
	if s.openSince.IsZero() {
		s.openSince = time.Now()
	}
	s.mu.Unlock()
}

// estimateSize approximates a record's serialized size for rotation accounting.
func estimateSize(r *model.Record) int {
	n := len(r.Message) + len(r.Namespace) + len(r.Pod) + len(r.Container) + len(r.Node) + 64
	for k, v := range r.Labels {
		n += len(k) + len(v)
	}
	for k, v := range r.Body {
		n += len(k) + len(v)
	}
	return n
}
