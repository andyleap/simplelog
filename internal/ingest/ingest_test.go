package ingest

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/proto/ingestpb"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// fakeReceiver records accepted records, dedups by (source,offset), and commits
// each accepted offset immediately (simulating an instant S3 seal).
type fakeReceiver struct {
	mu        sync.Mutex
	committed map[string]int64
	count     int
	dups      int
}

func newFakeReceiver() *fakeReceiver {
	return &fakeReceiver{committed: map[string]int64{}}
}

func (f *fakeReceiver) Accept(r *model.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := r.Source.Key()
	if r.Offset <= f.committed[key] && f.committed[key] != 0 {
		f.dups++
		return
	}
	f.count++
	if r.Offset > f.committed[key] {
		f.committed[key] = r.Offset
	}
}

func (f *fakeReceiver) Committed() map[string]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]int64, len(f.committed))
	for k, v := range f.committed {
		out[k] = v
	}
	return out
}

func (f *fakeReceiver) stats() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count, f.dups
}

func TestClientServerRoundTrip(t *testing.T) {
	rcv := newFakeReceiver()
	srv := NewServer(rcv)
	srv.ackEvery = 20 * time.Millisecond

	mux := drpcmux.New()
	if err := ingestpb.DRPCRegisterIngest(mux, srv); err != nil {
		t.Fatal(err)
	}
	dsrv := drpcserver.New(mux)

	cConn, sConn := net.Pipe()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	go dsrv.ServeOne(srvCtx, sConn)

	// Client driven manually over the same pipe (bypass dial).
	var committedMu sync.Mutex
	committed := map[string]int64{}
	c := NewClient(ClientConfig{BatchWait: 10 * time.Millisecond}, func(key string, off int64) {
		committedMu.Lock()
		if off > committed[key] {
			committed[key] = off
		}
		committedMu.Unlock()
	})

	src := model.Source{Node: "n1", Path: "/var/log/a.log"}
	go func() {
		for i := 1; i <= 200; i++ {
			c.Enqueue(&model.Record{
				Source:    src,
				Offset:    int64(i),
				Timestamp: time.Unix(int64(i), 0),
				Namespace: "prod", Pod: "web", Stream: model.StreamStdout,
				Message: "m",
			})
		}
	}()

	clientErr := make(chan error, 1)
	go func() { clientErr <- c.sessionConn(srvCtx, cConn) }()

	deadline := time.After(5 * time.Second)
	for {
		n, _ := rcv.stats()
		committedMu.Lock()
		gotCommit := committed[src.Key()]
		committedMu.Unlock()
		if n >= 200 && gotCommit >= 200 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: accepted=%d committed=%d", n, gotCommit)
		case <-time.After(10 * time.Millisecond):
		}
	}

	_, dups := rcv.stats()
	if dups != 0 {
		t.Fatalf("unexpected dups: %d", dups)
	}
}
