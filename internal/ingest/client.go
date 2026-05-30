package ingest

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/proto/ingestpb"
	"storj.io/drpc/drpcconn"
)

// CommitFunc is called when the manager acks a source up to offset; the agent
// uses it to advance its on-disk checkpoint and release held data.
type CommitFunc func(sourceKey string, offset int64)

// ClientConfig configures the agent-side ingest client.
type ClientConfig struct {
	Addr         string        // manager host:port
	MaxHeldBytes int64         // soft cap: Enqueue blocks above this
	HardMaxBytes int64         // hard cap: drop oldest unacked above this (0 = 2x soft)
	BatchBytes   int           // flush a batch at this many bytes
	BatchWait    time.Duration // or after this long
	Backoff      time.Duration // reconnect backoff
}

func (c *ClientConfig) defaults() {
	if c.MaxHeldBytes <= 0 {
		c.MaxHeldBytes = 64 << 20
	}
	if c.HardMaxBytes <= 0 {
		c.HardMaxBytes = 2 * c.MaxHeldBytes
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = 256 << 10
	}
	if c.BatchWait <= 0 {
		c.BatchWait = 200 * time.Millisecond
	}
	if c.Backoff <= 0 {
		c.Backoff = time.Second
	}
}

// Client streams records to the manager. It keeps an in-memory window of
// unacked records (the in-flight buffer) and re-sends it on reconnect; durable
// recovery across an agent restart comes from the tailer re-reading files from
// the last-committed checkpoint, so this window need not survive a crash.
type Client struct {
	cfg    ClientConfig
	commit CommitFunc

	mu       sync.Mutex
	cond     *sync.Cond
	unacked  []envelope // ordered by send order
	heldByte int64
	dropped  int64 // count of records dropped at the hard cap
	closed   bool

	notify chan struct{} // signals the send loop that new data is available
}

type envelope struct {
	sourceKey string
	offset    int64
	raw       []byte
	sent      bool // sent on the current session; reset on reconnect
}

// NewClient creates an ingest client. commit is invoked from the ack reader.
func NewClient(cfg ClientConfig, commit CommitFunc) *Client {
	cfg.defaults()
	c := &Client{cfg: cfg, commit: commit, notify: make(chan struct{}, 1)}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// wake signals the send loop without blocking.
func (c *Client) wake() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// Enqueue appends a record to the send buffer, blocking while the buffer is
// over the soft cap (backpressure → the tailer stops reading and holds fds).
// If the hard cap is exceeded while blocked, the oldest unacked records are
// dropped to protect node memory/disk and the loss is counted.
func (c *Client) Enqueue(r *model.Record) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	env := envelope{sourceKey: r.Source.Key(), offset: r.Offset, raw: raw}

	c.mu.Lock()
	defer c.mu.Unlock()
	for c.heldByte+int64(len(raw)) > c.cfg.MaxHeldBytes && !c.closed {
		if c.heldByte+int64(len(raw)) > c.cfg.HardMaxBytes {
			c.dropOldestLocked()
			break
		}
		c.cond.Wait()
	}
	if c.closed {
		return context.Canceled
	}
	c.unacked = append(c.unacked, env)
	c.heldByte += int64(len(raw))
	c.cond.Broadcast()
	c.wake()
	return nil
}

// dropOldestLocked sheds the oldest unacked record (data loss) and advances the
// checkpoint past it so the tailer won't re-read it. Caller holds c.mu.
func (c *Client) dropOldestLocked() {
	if len(c.unacked) == 0 {
		return
	}
	old := c.unacked[0]
	c.unacked = c.unacked[1:]
	c.heldByte -= int64(len(old.raw))
	c.dropped++
	// Advance checkpoint so the dropped data is not redelivered.
	if c.commit != nil {
		c.commit(old.sourceKey, old.offset)
	}
}

// Dropped returns the number of records dropped at the hard cap.
func (c *Client) Dropped() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped
}

// Close unblocks any Enqueue callers.
func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

// Run maintains the connection: dial, (re)send the unacked window, stream new
// records, and apply acks, reconnecting with backoff until ctx is cancelled.
func (c *Client) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.session(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.Backoff):
			}
		}
	}
}

func (c *Client) session(ctx context.Context) error {
	var d net.Dialer
	rawConn, err := d.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return err
	}
	return c.sessionConn(ctx, rawConn)
}

// sessionConn runs one ingest session over an established connection. It is
// also the test entry point (bypassing the dialer).
func (c *Client) sessionConn(ctx context.Context, rawConn net.Conn) error {
	conn := drpcconn.New(rawConn)
	defer conn.Close()

	client := ingestpb.NewDRPCIngestClient(conn)
	stream, err := client.Stream(ctx)
	if err != nil {
		return err
	}

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Ack reader.
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				cancel()
				return
			}
			c.applyAck(ack)
		}
	}()

	return c.sendLoop(sctx, stream)
}

// sendLoop re-sends the unacked window on (re)connect, then streams newly
// enqueued records in batches until the context is cancelled or a send fails.
// sentUpTo is an index into the unacked window marking what has been sent this
// session; a reconnect starts a fresh sendLoop with sentUpTo=0, re-sending all
// in-flight records (the manager dedups by (source, offset)).
func (c *Client) sendLoop(ctx context.Context, stream ingestpb.DRPCIngest_StreamClient) error {
	ticker := time.NewTicker(c.cfg.BatchWait)
	defer ticker.Stop()

	// flush sends every not-yet-sent record in batches. It collects a batch and
	// marks those envelopes sent under the lock, then sends outside it.
	flush := func() error {
		for {
			c.mu.Lock()
			var batch [][]byte
			var idx []int
			var nbytes int
			for i := range c.unacked {
				if c.unacked[i].sent {
					continue
				}
				batch = append(batch, c.unacked[i].raw)
				idx = append(idx, i)
				nbytes += len(c.unacked[i].raw)
				if nbytes >= c.cfg.BatchBytes {
					break
				}
			}
			for _, i := range idx {
				c.unacked[i].sent = true
			}
			c.mu.Unlock()
			if len(batch) == 0 {
				return nil
			}
			if err := stream.Send(&ingestpb.RecordBatch{Records: batch}); err != nil {
				return err
			}
		}
	}

	// On (re)connect, mark the whole in-flight window unsent so it is resent.
	c.mu.Lock()
	for i := range c.unacked {
		c.unacked[i].sent = false
	}
	c.mu.Unlock()

	if err := flush(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-c.notify:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (c *Client) applyAck(ack *ingestpb.Ack) {
	c.mu.Lock()
	defer c.mu.Unlock()
	committed := map[string]int64{}
	for _, so := range ack.Committed {
		committed[so.SourceKey] = so.Offset
		if c.commit != nil {
			c.commit(so.SourceKey, so.Offset)
		}
	}
	// Trim unacked records whose offset is committed.
	kept := c.unacked[:0]
	var held int64
	for _, e := range c.unacked {
		if off, ok := committed[e.sourceKey]; ok && e.offset <= off {
			continue // acked
		}
		kept = append(kept, e)
		held += int64(len(e.raw))
	}
	c.unacked = kept
	c.heldByte = held
	c.cond.Broadcast()
}
