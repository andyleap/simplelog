package tailer

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

// Sink receives records from the tailer. *ingest.Client satisfies it; Enqueue
// blocks under backpressure, which is what makes the held-open log files act as
// the buffer.
type Sink interface {
	Enqueue(*model.Record) error
}

// Enricher attaches Kubernetes metadata (labels, annotations, node, image) to a
// record given its pod UID. A nil/no-op Enricher leaves only path-derived
// fields populated.
type Enricher interface {
	Enrich(rec *model.Record, uid string)
}

// Filter decides, per pod UID, whether a pod's logs should be ingested. The
// enricher implements it via a configurable pod annotation; if the Enricher
// passed to New also implements Filter, the tailer consults it at discovery.
type Filter interface {
	ShouldIngest(uid string) bool
}

// ReadyChecker reports whether pod metadata is available for a UID. When the
// Enricher implements it, the tailer waits for readiness before forwarding a
// pod's logs, so records are never shipped un-enriched. The enricher implements
// this; a nil/no-op enricher means no waiting.
type ReadyChecker interface {
	Ready(uid string) bool
}

// Config configures the tailer.
type Config struct {
	Root         string        // e.g. /var/log/pods
	Node         string        // node name (for Source.Node)
	Glob         string        // file glob under Root; default */*/*.log
	PollInterval time.Duration // discovery + read poll; default 1s

	// ExcludeNamespaces are namespaces whose pod logs are never tailed. simplelog
	// MUST exclude its own namespace here: the agent tails every pod's logs
	// (including its own and the manager's), so without this a single log line
	// simplelog emits would be ingested, potentially re-logged, and loop.
	ExcludeNamespaces []string

	// MetadataWait bounds how long a tail waits for pod metadata (when the
	// Enricher implements ReadyChecker) before proceeding un-enriched. This
	// prevents blocking forever on logs of an already-deleted pod the informer
	// will never see. Default 30s.
	MetadataWait time.Duration
}

func (c *Config) defaults() {
	if c.Glob == "" {
		c.Glob = "*/*/*.log"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.MetadataWait <= 0 {
		c.MetadataWait = 30 * time.Second
	}
}

// Tailer discovers CRI log files under Root and tails each in its own goroutine.
type Tailer struct {
	cfg     Config
	sink    Sink
	enr     Enricher
	cp      *Checkpoint
	exclude map[string]bool // namespaces to skip

	mu     sync.Mutex
	active map[string]*fileTail // keyed by Source.Key()
}

// New creates a Tailer.
func New(cfg Config, sink Sink, enr Enricher, cp *Checkpoint) *Tailer {
	cfg.defaults()
	exclude := make(map[string]bool, len(cfg.ExcludeNamespaces))
	for _, ns := range cfg.ExcludeNamespaces {
		if ns != "" {
			exclude[ns] = true
		}
	}
	return &Tailer{cfg: cfg, sink: sink, enr: enr, cp: cp, exclude: exclude, active: map[string]*fileTail{}}
}

// Run scans for log files and starts a tail goroutine per file generation,
// until ctx is cancelled. Each tail exits on its own when its file is fully
// drained after rotation/deletion.
func (t *Tailer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	tick := time.NewTicker(t.cfg.PollInterval)
	defer tick.Stop()
	for {
		t.discover(ctx, &wg)
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// discover globs for log files and starts a tail for any new file generation.
func (t *Tailer) discover(ctx context.Context, wg *sync.WaitGroup) {
	matches, err := filepath.Glob(filepath.Join(t.cfg.Root, t.cfg.Glob))
	if err != nil {
		log.Printf("tailer glob: %v", err)
		return
	}
	for _, path := range matches {
		pod, ok := parsePath(path)
		if !ok {
			continue
		}
		if t.exclude[pod.Namespace] {
			continue // never ingest excluded namespaces (e.g. simplelog's own)
		}
		if f, ok := t.enr.(Filter); ok && !f.ShouldIngest(pod.UID) {
			continue // pod opted out via annotation
		}
		fi, err := os.Stat(path)
		if err != nil {
			continue
		}
		src := sourceFromStat(t.cfg.Node, path, fi)
		key := src.Key()

		t.mu.Lock()
		if _, running := t.active[key]; running {
			t.mu.Unlock()
			continue
		}
		ft := &fileTail{path: path, pod: pod, src: src, offset: t.cp.Get(key)}
		t.active[key] = ft
		t.mu.Unlock()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ft.run(ctx, t)
			t.mu.Lock()
			delete(t.active, key)
			t.mu.Unlock()
		}()
	}
}

// fileTail tails a single file generation.
type fileTail struct {
	path   string
	pod    PodInfo
	src    model.Source
	offset int64
	f      *os.File
	// partial holds an accumulated CRI partial-line (tag "P") message.
	partial strings.Builder
	pts     time.Time
	pstream string
}

func (ft *fileTail) run(ctx context.Context, t *Tailer) {
	f, err := os.Open(ft.path)
	if err != nil {
		return
	}
	ft.f = f
	defer f.Close()
	if ft.offset > 0 {
		if _, err := f.Seek(ft.offset, io.SeekStart); err != nil {
			ft.offset = 0
			f.Seek(0, io.SeekStart)
		}
	}

	// With enrichment enabled, hold this pod's logs (the fd keeps the data alive)
	// until its metadata is available, so records are never shipped un-enriched
	// and the exclude annotation is known before anything is forwarded. Bounded
	// by MetadataWait so already-deleted pods (never in the informer) don't pin
	// the descriptor forever.
	if rc, ok := t.enr.(ReadyChecker); ok && !ft.awaitMetadata(ctx, t, rc) {
		return // context cancelled
	}
	// Now that metadata is (likely) present, honor the per-pod exclude annotation.
	if fl, ok := t.enr.(Filter); ok && !fl.ShouldIngest(ft.pod.UID) {
		return
	}

	reader := bufio.NewReader(f)
	for {
		if err := ft.pump(ctx, t, reader); err != nil {
			return // context cancelled or unrecoverable
		}
		// At EOF: decide whether the file was rotated/removed (then drain & exit)
		// or is still live (then poll again). Because we hold the fd open, any
		// data written before replacement is still readable via this descriptor.
		if ft.rotatedOrGone() {
			// One final drain to catch any last bytes, then exit.
			ft.pump(ctx, t, reader)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.cfg.PollInterval):
		}
	}
}

// awaitMetadata blocks until the enricher has metadata for this pod, the grace
// period elapses, or the context is cancelled. It returns false only on context
// cancellation; on timeout it returns true so the tail proceeds un-enriched.
func (ft *fileTail) awaitMetadata(ctx context.Context, t *Tailer, rc ReadyChecker) bool {
	if rc.Ready(ft.pod.UID) {
		return true
	}
	deadline := time.Now().Add(t.cfg.MetadataWait)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(t.cfg.PollInterval):
			if rc.Ready(ft.pod.UID) || time.Now().After(deadline) {
				return true
			}
		}
	}
}

// pump reads and emits all currently-available complete lines.
func (ft *fileTail) pump(ctx context.Context, t *Tailer, reader *bufio.Reader) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line, err := reader.ReadString('\n')
		if len(line) > 0 && strings.HasSuffix(line, "\n") {
			ft.offset += int64(len(line))
			if rec := ft.consume(strings.TrimRight(line, "\n")); rec != nil {
				t.enrichAndSend(rec, ft.pod.UID)
			}
			continue
		}
		// Incomplete trailing line (no newline yet): rewind so we re-read it once
		// the writer finishes the line. ReadString consumed it into `line`.
		if len(line) > 0 {
			if _, serr := ft.f.Seek(ft.offset, io.SeekStart); serr == nil {
				reader.Reset(ft.f)
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// consume folds CRI partial lines and returns a record for completed lines.
func (ft *fileTail) consume(line string) *model.Record {
	cl, ok := parseCRILine(line)
	if !ok {
		return nil
	}
	if cl.partial {
		if ft.partial.Len() == 0 {
			ft.pts = cl.ts
			ft.pstream = cl.stream
		}
		ft.partial.WriteString(cl.message)
		return nil
	}
	if ft.partial.Len() > 0 {
		ft.partial.WriteString(cl.message)
		cl.message = ft.partial.String()
		cl.ts = ft.pts
		cl.stream = ft.pstream
		ft.partial.Reset()
	}
	return buildRecord(ft.src, ft.pod, cl, ft.offset)
}

// rotatedOrGone reports whether the path now refers to a different inode than
// this tail's (rotation) or no longer exists (deletion).
func (ft *fileTail) rotatedOrGone() bool {
	fi, err := os.Stat(ft.path)
	if err != nil {
		return true
	}
	cur := sourceFromStat(ft.src.Node, ft.path, fi)
	return cur.Inode != ft.src.Inode || cur.Device != ft.src.Device
}

func (t *Tailer) enrichAndSend(rec *model.Record, uid string) {
	if t.enr != nil {
		t.enr.Enrich(rec, uid)
	}
	if err := t.sink.Enqueue(rec); err != nil {
		// Enqueue only errors on shutdown; drop silently.
		return
	}
}

// sourceFromStat builds a Source from a path and its stat info. The generation
// identity uses dev+inode plus the file's birth time (btime), which is stable
// across appends — using ctime/mtime here would churn the key on every write.
func sourceFromStat(node, path string, fi os.FileInfo) model.Source {
	src := model.Source{Node: node, Path: path}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		src.Device = uint64(st.Dev)
		src.Inode = st.Ino
	}
	src.CreatedUnixNano = statBirthNano(path)
	return src
}
