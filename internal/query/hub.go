package query

import (
	"sync"

	"github.com/andyleap/simplelog/internal/model"
)

// Hub fans out newly-ingested records to live-tail subscribers. The manager
// calls Publish for every accepted record; a follow query Subscribes, replays
// history up to its registration point, then drains its channel. Slow
// subscribers are dropped (their channel closed) rather than blocking ingest.
type Hub struct {
	mu      sync.Mutex
	subs    map[int]*subscriber
	nextID  int
	bufSize int
}

type subscriber struct {
	ch      chan *model.Record
	expr    Node
	dropped bool
}

// NewHub creates a hub whose subscriber channels buffer bufSize records.
func NewHub(bufSize int) *Hub {
	if bufSize <= 0 {
		bufSize = 1024
	}
	return &Hub{subs: map[int]*subscriber{}, bufSize: bufSize}
}

// Publish delivers r to all matching subscribers without blocking. A subscriber
// whose buffer is full is marked dropped and its channel closed.
func (h *Hub) Publish(r *model.Record) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, s := range h.subs {
		if s.expr != nil && !s.expr.Match(r) {
			continue
		}
		select {
		case s.ch <- r:
		default:
			s.dropped = true
			close(s.ch)
			delete(h.subs, id)
		}
	}
}

// Sub is a live subscription handle.
type Sub struct {
	hub *Hub
	id  int
	C   <-chan *model.Record
}

// Subscribe registers a follow subscriber filtered by expr (nil = all).
func (h *Hub) Subscribe(expr Node) *Sub {
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextID
	h.nextID++
	s := &subscriber{ch: make(chan *model.Record, h.bufSize), expr: expr}
	h.subs[id] = s
	return &Sub{hub: h, id: id, C: s.ch}
}

// Close removes the subscription.
func (s *Sub) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if sub, ok := s.hub.subs[s.id]; ok {
		close(sub.ch)
		delete(s.hub.subs, s.id)
	}
}
