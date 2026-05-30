// Package integration exercises the full agent -> DRPC -> manager -> store ->
// query path in-process (S3 faked by store.MemBlobs), including a manager
// restart drill that verifies catalog recovery from S3 and redelivery of
// un-acked data.
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/ingest"
	"github.com/andyleap/simplelog/internal/manager"
	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/query"
	"github.com/andyleap/simplelog/internal/store"
	"github.com/andyleap/simplelog/internal/tailer"
)

type mgrHandle struct {
	mgr    *manager.Manager
	st     *store.Store
	lis    net.Listener
	cancel context.CancelFunc
}

func startManager(t *testing.T, blobs store.Blobs, baseDir, addr string) *mgrHandle {
	t.Helper()
	st, err := store.Open(store.Config{ManagerID: "mgr1", BaseDir: baseDir}, blobs)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Recover(); err != nil {
		t.Fatal(err)
	}
	mgr, err := manager.New(st, manager.SpoolConfig{WorkDir: filepath.Join(baseDir, "work")})
	if err != nil {
		t.Fatal(err)
	}
	lis := listenRetry(t, addr)
	ctx, cancel := context.WithCancel(context.Background())
	go mgr.ServeIngest(ctx, lis)
	return &mgrHandle{mgr: mgr, st: st, lis: lis, cancel: cancel}
}

func (h *mgrHandle) stop() {
	h.cancel()
	h.lis.Close()
	h.st.Close()
}

func listenRetry(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis
		}
		if time.Now().After(deadline) {
			t.Fatalf("listen %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// queryMessages runs an empty query and returns the set of record messages.
func queryMessages(t *testing.T, m *manager.Manager) map[string]int {
	t.Helper()
	out := map[string]int{}
	err := m.Executor.Run(query.Request{}, func(r *model.Record) error {
		out[r.Message]++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", msg)
}

func TestFullPipelineAndManagerRestart(t *testing.T) {
	blobs := store.NewMemBlobs() // shared "S3" across manager restarts

	// Stable address the agent reconnects to across the manager restart.
	probe := listenRetry(t, "127.0.0.1:0")
	addr := probe.Addr().String()
	probe.Close()

	// --- Manager 1 ---
	dir1 := t.TempDir()
	m1 := startManager(t, blobs, dir1, addr)

	// --- Agent: tailer + ingest client ---
	root := filepath.Join(t.TempDir(), "pods")
	logDir := filepath.Join(root, "prod_web_uid1", "app")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(logDir, "0.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	writeLine := func(i int) {
		ts := time.Date(2026, 5, 30, 12, 0, 0, i*1000, time.UTC).Format(time.RFC3339Nano)
		fmt.Fprintf(f, "%s stdout F line-%02d\n", ts, i)
		f.Sync()
	}

	cp, err := tailer.OpenCheckpoint(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()

	agentCtx, agentCancel := context.WithCancel(context.Background())
	defer agentCancel()
	client := ingest.NewClient(ingest.ClientConfig{Addr: addr, BatchWait: 30 * time.Millisecond, Backoff: 200 * time.Millisecond},
		func(key string, off int64) { cp.Advance(key, off) })
	defer client.Close()
	go client.Run(agentCtx)

	tr := tailer.New(tailer.Config{Root: root, Node: "node-1", PollInterval: 30 * time.Millisecond}, client, nil, cp)
	go tr.Run(agentCtx)

	// Write 10 lines; they should arrive in the open buffer.
	for i := 0; i < 10; i++ {
		writeLine(i)
	}
	waitFor(t, func() bool { return len(queryMessages(t, m1.mgr)) >= 10 }, 5*time.Second, "first 10 records delivered")

	// Seal them to S3 (durable) and confirm still queryable.
	if _, err := m1.mgr.Spool.Seal(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(queryMessages(t, m1.mgr)) == 10 }, 2*time.Second, "10 records after seal")

	// Write 5 more lines — delivered to m1's open buffer but NOT sealed/acked.
	for i := 10; i < 15; i++ {
		writeLine(i)
	}
	waitFor(t, func() bool { return len(queryMessages(t, m1.mgr)) >= 15 }, 5*time.Second, "15 records visible on m1")

	// --- Kill manager 1 WITHOUT sealing the last 5 (simulating a crash). ---
	m1.stop()
	time.Sleep(100 * time.Millisecond)

	// --- Manager 2 on a fresh emptyDir, same S3, same address. ---
	dir2 := t.TempDir()
	m2 := startManager(t, blobs, dir2, addr)
	defer m2.stop()

	// Recovery rebuilt the first 10 (sealed) from S3.
	waitFor(t, func() bool { return len(queryMessages(t, m2.mgr)) == 10 }, 3*time.Second, "10 recovered from S3")

	// The agent reconnects and redelivers the un-acked last 5. Seal periodically
	// so the redelivered records become durable and queryable.
	waitFor(t, func() bool {
		m2.mgr.Spool.Seal()
		return len(queryMessages(t, m2.mgr)) == 15
	}, 8*time.Second, "all 15 present on m2 after redelivery")

	// Verify exactly lines 0..14, each exactly once (no loss, no duplication).
	msgs := queryMessages(t, m2.mgr)
	if len(msgs) != 15 {
		t.Fatalf("expected 15 unique messages, got %d", len(msgs))
	}
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("line-%02d", i)
		if msgs[key] != 1 {
			t.Fatalf("message %q appeared %d times, want 1", key, msgs[key])
		}
	}
}
