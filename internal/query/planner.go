package query

import (
	"io"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
)

// SegmentStore is the subset of *store.Store the executor needs. It is an
// interface so the planner can be tested with a fake.
type SegmentStore interface {
	// Overlapping returns sealed segments intersecting [start,end], ascending.
	Overlapping(start, end time.Time) ([]model.SegmentMeta, error)
	// Reader opens a segment for block-level reads, transparently fetching from
	// S3 if not locally cached.
	Reader(m model.SegmentMeta) (*segment.Reader, io.Closer, error)
}

// OpenSnapshot returns the manager's in-memory open-segment records that fall
// in [start,end], in ascending timestamp order. It lets queries see records
// that have been received but not yet sealed.
type OpenSnapshot func(start, end time.Time) []*model.Record

// Request is a compiled query.
type Request struct {
	Expr  Node
	Start time.Time
	End   time.Time
	Limit int  // 0 = unlimited
	Desc  bool // true = newest first
}

// Executor runs queries against sealed segments plus the open buffer.
type Executor struct {
	Store SegmentStore
	Open  OpenSnapshot
}

// Run streams matching records to emit in timestamp order (ascending, or
// descending if Req.Desc). It honours the limit and closes all opened segment
// readers before returning. emit returning an error stops the scan.
func (e *Executor) Run(req Request, emit func(*model.Record) error) error {
	if req.Expr == nil {
		req.Expr = matchAll{}
	}

	segs, err := e.Store.Overlapping(req.Start, req.End)
	if err != nil {
		return err
	}

	var iters []recIter
	var closers []io.Closer
	defer func() {
		for _, c := range closers {
			c.Close()
		}
	}()

	for _, m := range segs {
		if !segmentMatches(m, req.Expr) {
			continue // catalog/source-summary prune
		}
		rd, closer, err := e.Store.Reader(m)
		if err != nil {
			return err
		}
		closers = append(closers, closer)
		it := newSegIter(rd, req.Start, req.End, req.Desc)
		if it.peek() != nil {
			iters = append(iters, it)
		}
	}

	if e.Open != nil {
		open := e.Open(req.Start, req.End)
		if len(open) > 0 {
			si := &sliceIter{recs: open, desc: req.Desc}
			if req.Desc {
				si.pos = len(open) - 1
			}
			if si.peek() != nil {
				iters = append(iters, si)
			}
		}
	}

	count := 0
	for {
		best := pickNext(iters, req.Desc)
		if best == nil {
			break
		}
		rec := best.peek()
		best.pop()
		if rec.Timestamp.Before(req.Start) && !req.Start.IsZero() {
			continue
		}
		if !req.End.IsZero() && rec.Timestamp.After(req.End) {
			continue
		}
		if !req.Expr.Match(rec) {
			continue
		}
		if err := emit(rec); err != nil {
			return err
		}
		count++
		if req.Limit > 0 && count >= req.Limit {
			break
		}
	}
	for _, it := range iters {
		if err := it.fail(); err != nil {
			return err
		}
	}
	return nil
}

// segmentMatches applies coarse source-summary pruning: if the expression
// requires an exact _namespace/_pod/_node/_container that the segment's summary
// does not contain, the segment can be skipped. Anything not statically
// prunable conservatively keeps the segment.
func segmentMatches(m model.SegmentMeta, expr Node) bool {
	for _, p := range prunablePredicates(expr) {
		var set []string
		switch p.field {
		case "_namespace":
			set = m.Namespaces
		case "_pod":
			set = m.Pods
		case "_node":
			set = m.Nodes
		case "_container":
			set = m.Containers
		default:
			continue
		}
		if len(set) > 0 && !contains(set, p.value) {
			return false
		}
	}
	return true
}

func contains(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// pickNext returns the iterator whose head sorts first (earliest for ascending,
// latest for descending). N is the number of candidate segments + 1, small at
// this scale, so a linear scan beats heap bookkeeping.
func pickNext(iters []recIter, desc bool) recIter {
	var best recIter
	for _, it := range iters {
		h := it.peek()
		if h == nil {
			continue
		}
		if best == nil {
			best = it
			continue
		}
		bh := best.peek()
		if desc {
			if h.Timestamp.After(bh.Timestamp) {
				best = it
			}
		} else {
			if h.Timestamp.Before(bh.Timestamp) {
				best = it
			}
		}
	}
	return best
}
