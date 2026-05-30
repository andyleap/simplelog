package query

import (
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/segment"
)

// recIter is a record cursor yielding records in (ascending or descending)
// timestamp order. peek returns the current record without consuming it; pop
// advances; fail reports any deferred read error.
type recIter interface {
	peek() *model.Record
	pop()
	fail() error
}

// sliceIter iterates a pre-sorted (ascending) slice of records, forward or
// backward.
type sliceIter struct {
	recs []*model.Record
	pos  int
	desc bool
}

func (s *sliceIter) peek() *model.Record {
	if s.pos < 0 || s.pos >= len(s.recs) {
		return nil
	}
	return s.recs[s.pos]
}

func (s *sliceIter) pop() {
	if s.desc {
		s.pos--
	} else {
		s.pos++
	}
}

func (s *sliceIter) fail() error { return nil }

// segIter lazily reads the time-overlapping blocks of a segment, one block at a
// time, yielding records in timestamp order.
type segIter struct {
	rd         *segment.Reader
	blocks     []int // overlapping block indices, in traversal order
	bi         int
	buf        []*model.Record
	pos        int
	desc       bool
	start, end time.Time
	err        error
}

func newSegIter(rd *segment.Reader, start, end time.Time, desc bool) *segIter {
	var blocks []int
	all := rd.Blocks()
	for i, b := range all {
		if b.Overlaps(start, end) {
			blocks = append(blocks, i)
		}
	}
	if desc {
		// Reverse block order for descending traversal.
		for i, j := 0, len(blocks)-1; i < j; i, j = i+1, j-1 {
			blocks[i], blocks[j] = blocks[j], blocks[i]
		}
	}
	return &segIter{rd: rd, blocks: blocks, desc: desc, start: start, end: end}
}

// load fills buf from the next block until it finds a non-empty one.
func (s *segIter) load() {
	for s.pos >= len(s.buf) && s.bi < len(s.blocks) {
		idx := s.blocks[s.bi]
		s.bi++
		s.buf = s.buf[:0]
		err := s.rd.ReadBlock(idx, func(r *model.Record) error {
			rec := *r
			s.buf = append(s.buf, &rec)
			return nil
		})
		if err != nil {
			s.err = err
			s.buf = nil
			return
		}
		if s.desc {
			for i, j := 0, len(s.buf)-1; i < j; i, j = i+1, j-1 {
				s.buf[i], s.buf[j] = s.buf[j], s.buf[i]
			}
		}
		s.pos = 0
	}
}

func (s *segIter) peek() *model.Record {
	if s.err != nil {
		return nil
	}
	s.load()
	if s.pos >= len(s.buf) {
		return nil
	}
	return s.buf[s.pos]
}

func (s *segIter) pop() { s.pos++ }

func (s *segIter) fail() error { return s.err }
