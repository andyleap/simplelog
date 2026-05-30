package manager

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/andyleap/simplelog/internal/ingest"
	"github.com/andyleap/simplelog/internal/query"
	"github.com/andyleap/simplelog/internal/store"
	"github.com/andyleap/simplelog/proto/ingestpb"
	"github.com/oklog/ulid/v2"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

// Manager wires the store, the open-segment spool, the live-tail hub, and the
// query executor together.
type Manager struct {
	Store    *store.Store
	Spool    *Spool
	Hub      *query.Hub
	Executor *query.Executor

	snapEvery time.Duration
}

// New builds a Manager from an opened store and spool config.
func New(st *store.Store, spoolCfg SpoolConfig) (*Manager, error) {
	hub := query.NewHub(4096)
	spool, err := NewSpool(spoolCfg, st, hub)
	if err != nil {
		return nil, err
	}
	m := &Manager{
		Store:     st,
		Spool:     spool,
		Hub:       hub,
		Executor:  &query.Executor{Store: st, Open: spool.Snapshot},
		snapEvery: time.Minute,
	}
	return m, nil
}

// RunSealLoop periodically seals the open segment when it hits a size/age
// threshold and snapshots the catalog to S3. It returns when ctx is cancelled.
func (m *Manager) RunSealLoop(ctx context.Context) {
	check := time.NewTicker(time.Second)
	defer check.Stop()
	snap := time.NewTicker(m.snapEvery)
	defer snap.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-check.C:
			if m.Spool.shouldSeal() {
				// No success log here: a routine seal happens as a direct result
				// of processing logs, and logging it would be exactly the kind of
				// processing-triggered output we avoid. Errors are still logged.
				if _, err := m.Spool.Seal(); err != nil {
					log.Printf("seal error: %v", err)
				}
			}
		case <-snap.C:
			if err := m.Store.Snapshot(ulid.Make().String()); err != nil {
				log.Printf("catalog snapshot error: %v", err)
			}
		}
	}
}

// Shutdown seals any buffered records (so SIGTERM doesn't force agents to
// redeliver) and snapshots the catalog. Acks for the freshly committed offsets
// flow out on the ingest server's ack loop before the process exits.
func (m *Manager) Shutdown() {
	if n, err := m.Spool.Seal(); err != nil {
		log.Printf("shutdown seal error: %v", err)
	} else if n > 0 {
		log.Printf("shutdown sealed %d records", n)
	}
	if err := m.Store.Snapshot(ulid.Make().String()); err != nil {
		log.Printf("shutdown snapshot error: %v", err)
	}
}

// ServeIngest serves the DRPC ingest service on lis until ctx is cancelled.
func (m *Manager) ServeIngest(ctx context.Context, lis net.Listener) error {
	mux := drpcmux.New()
	if err := ingestpb.DRPCRegisterIngest(mux, ingest.NewServer(m.Spool)); err != nil {
		return err
	}
	srv := drpcserver.New(mux)
	return srv.Serve(ctx, lis)
}
