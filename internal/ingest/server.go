// Package ingest implements the agent->manager log feed over DRPC: the manager
// side server with per-source dedup and S3-commit-gated acks, and the agent
// side client that resumes from the last acked offset.
package ingest

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/proto/ingestpb"
)

// Receiver is the manager-side sink the ingest server delivers records to. It
// owns dedup (Accept ignores already-seen offsets) and the committed-offset
// watermark (advanced only when a segment is sealed to S3).
type Receiver interface {
	// Accept ingests one decoded record. Implementations must be safe for
	// concurrent use and must drop duplicates (offset <= received watermark).
	Accept(r *model.Record)
	// Committed returns a snapshot of the highest S3-committed offset per
	// source key. The server acks these so the agent can advance its checkpoint.
	Committed() map[string]int64
}

// Server adapts a Receiver to the generated DRPCIngestServer interface.
type Server struct {
	ingestpb.DRPCIngestUnimplementedServer
	rcv      Receiver
	ackEvery time.Duration
}

// NewServer creates an ingest server delivering to rcv.
func NewServer(rcv Receiver) *Server {
	return &Server{rcv: rcv, ackEvery: time.Second}
}

// Stream handles one agent connection: it reads batches in one goroutine and
// periodically sends acks for the sources seen on this stream in another.
func (s *Server) Stream(stream ingestpb.DRPCIngest_StreamStream) error {
	ctx := stream.Context()

	var mu sync.Mutex
	seen := map[string]struct{}{} // source keys observed on this stream

	// Ack loop.
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(s.ackEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-t.C:
				mu.Lock()
				ack := buildAck(seen, s.rcv.Committed())
				mu.Unlock()
				if len(ack.Committed) > 0 {
					if err := stream.Send(ack); err != nil {
						return
					}
				}
			}
		}
	}()

	for {
		batch, err := stream.Recv()
		if err != nil {
			return err // includes io.EOF on clean close
		}
		for _, raw := range batch.Records {
			var rec model.Record
			if err := json.Unmarshal(raw, &rec); err != nil {
				continue // skip malformed record rather than killing the stream
			}
			s.rcv.Accept(&rec)
			mu.Lock()
			seen[rec.Source.Key()] = struct{}{}
			mu.Unlock()
		}
		// Opportunistic immediate ack after a batch.
		mu.Lock()
		ack := buildAck(seen, s.rcv.Committed())
		mu.Unlock()
		if len(ack.Committed) > 0 {
			if err := stream.Send(ack); err != nil {
				return err
			}
		}
	}
}

func buildAck(seen map[string]struct{}, committed map[string]int64) *ingestpb.Ack {
	ack := &ingestpb.Ack{}
	for key := range seen {
		if off, ok := committed[key]; ok {
			ack.Committed = append(ack.Committed, &ingestpb.SourceOffset{
				SourceKey: key,
				Offset:    off,
			})
		}
	}
	return ack
}
