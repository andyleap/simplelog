package manager

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/query"
	"github.com/andyleap/simplelog/internal/ui"
)

// Handler returns the manager's HTTP API plus the embedded web UI:
//
//	GET /v1/query?expr=...&start=...&end=...&limit=...&direction=asc|desc&follow=true
//	GET /v1/fields                              field/value suggestions for autocomplete
//	GET /v1/histogram?expr=...&start=...&end=...&buckets=...   time-bucketed counts
//	GET /healthz
//	GET /                                        embedded UI
//
// /v1/query streams matching records as newline-delimited JSON (NDJSON).
func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", m.handleQuery)
	mux.HandleFunc("/v1/fields", m.handleFields)
	mux.HandleFunc("/v1/histogram", m.handleHistogram)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
	mux.Handle("/", http.FileServer(http.FS(ui.Assets())))
	return mux
}

// handleFields returns field keys and low-cardinality value suggestions for the
// query-editor autocomplete, aggregated from the catalog's per-segment summaries.
func (m *Manager) handleFields(w http.ResponseWriter, _ *http.Request) {
	segs, err := m.Store.Overlapping(time.Time{}, time.Time{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ns, pods, nodes, containers := set{}, set{}, set{}, set{}
	labels, fields := set{}, set{}
	for _, s := range segs {
		ns.addAll(s.Namespaces)
		pods.addAll(s.Pods)
		nodes.addAll(s.Nodes)
		containers.addAll(s.Containers)
		labels.addAll(s.LabelKeys)
		fields.addAll(s.FieldKeys)
	}
	writeJSON(w, map[string]any{
		"envelope":   []string{"_namespace", "_pod", "_container", "_node", "_image", "_stream", "_ts", "_message"},
		"labels":     labels.sorted(),
		"fields":     fields.sorted(),
		"namespaces": ns.sorted(),
		"pods":       pods.sorted(),
		"nodes":      nodes.sorted(),
		"containers": containers.sorted(),
	})
}

// handleHistogram returns time-bucketed match counts for a query. start/end
// default to the last 24h; counts has `buckets` entries each `intervalMs` wide.
func (m *Manager) handleHistogram(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	node, err := query.Parse(q.Get("expr"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start, _ := parseTime(q.Get("start"))
	end, _ := parseTime(q.Get("end"))
	if end.IsZero() {
		end = time.Now()
	}
	if start.IsZero() {
		start = end.Add(-24 * time.Hour)
	}
	buckets, _ := strconv.Atoi(q.Get("buckets"))
	if buckets <= 0 {
		buckets = 60
	}
	span := end.Sub(start)
	if span <= 0 {
		span = time.Second
	}
	interval := span / time.Duration(buckets)
	if interval <= 0 {
		interval = time.Millisecond
	}

	counts := make([]int64, buckets)
	var total int64
	err = m.Executor.Run(query.Request{Expr: node, Start: start, End: end}, func(rec *model.Record) error {
		i := int(rec.Timestamp.Sub(start) / interval)
		if i < 0 {
			i = 0
		}
		if i >= buckets {
			i = buckets - 1
		}
		counts[i]++
		total++
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{
		"start":      start.UTC().Format(time.RFC3339Nano),
		"end":        end.UTC().Format(time.RFC3339Nano),
		"intervalMs": interval.Milliseconds(),
		"counts":     counts,
		"total":      total,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// set is a small string-set helper for field aggregation.
type set map[string]struct{}

func (s set) addAll(vs []string) {
	for _, v := range vs {
		if v != "" {
			s[v] = struct{}{}
		}
	}
}

func (s set) sorted() []string {
	out := make([]string, 0, len(s))
	for v := range s {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (m *Manager) handleQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	node, err := query.Parse(q.Get("expr"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	start, err := parseTime(q.Get("start"))
	if err != nil {
		http.Error(w, "bad start: "+err.Error(), http.StatusBadRequest)
		return
	}
	end, err := parseTime(q.Get("end"))
	if err != nil {
		http.Error(w, "bad end: "+err.Error(), http.StatusBadRequest)
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	follow := q.Get("follow") == "true" || q.Get("follow") == "1"
	desc := q.Get("direction") == "desc"
	if follow {
		desc = false // live tail is always forward in time
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)

	// Track the highest emitted offset per source for boundary dedup between
	// the historical scan and the live feed.
	maxEmitted := map[string]int64{}
	emit := func(rec *model.Record) error {
		if err := enc.Encode(rec); err != nil {
			return err
		}
		if rec.Offset > maxEmitted[rec.Source.Key()] {
			maxEmitted[rec.Source.Key()] = rec.Offset
		}
		return nil
	}

	if !follow {
		req := query.Request{Expr: node, Start: start, End: end, Limit: limit, Desc: desc}
		if err := m.Executor.Run(req, emit); err != nil {
			// Headers may already be sent; best effort.
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
		return
	}

	m.streamFollow(w, r, node, start, emit, maxEmitted, flusher)
}

// streamFollow implements gapless live tail: subscribe first, replay history up
// to the subscription instant, then drain the live feed (deduping the overlap).
func (m *Manager) streamFollow(
	w http.ResponseWriter, r *http.Request, node query.Node, start time.Time,
	emit func(*model.Record) error, maxEmitted map[string]int64, flusher http.Flusher,
) {
	sub := m.Hub.Subscribe(node)
	defer sub.Close()
	treg := time.Now()

	// History: [start, treg], ascending.
	req := query.Request{Expr: node, Start: start, End: treg, Desc: false}
	if err := m.Executor.Run(req, emit); err != nil {
		return
	}
	if flusher != nil {
		flusher.Flush()
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-sub.C:
			if !ok {
				return // dropped: subscriber lagged
			}
			if !start.IsZero() && rec.Timestamp.Before(start) {
				continue
			}
			// Boundary dedup: skip records already emitted by the history scan.
			if off, seen := maxEmitted[rec.Source.Key()]; seen && rec.Offset <= off {
				continue
			}
			if err := emit(rec); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// parseTime accepts RFC3339, a unix-seconds integer, or "" (zero time).
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0).UTC(), nil
	}
	return time.Time{}, errBadTime
}

var errBadTime = &timeError{}

type timeError struct{}

func (*timeError) Error() string { return "expected RFC3339 or unix seconds" }
