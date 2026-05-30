// Command agent is the simplelog node agent: a DaemonSet that tails CRI
// container logs, enriches them with Kubernetes metadata, and streams them to
// the manager over DRPC. The container log files themselves are the buffer —
// the agent holds fds open across rotation and checkpoints a per-file
// high-water mark, advancing it only when the manager acks (S3-durable).
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/andyleap/simplelog/internal/buildinfo"
	"github.com/andyleap/simplelog/internal/enrich"
	"github.com/andyleap/simplelog/internal/ingest"
	"github.com/andyleap/simplelog/internal/tailer"
)

func main() {
	var (
		managerAddr = flag.String("manager-addr", env("SL_MANAGER_ADDR", "simplelog-manager:9000"), "manager DRPC address")
		logRoot     = flag.String("log-root", env("SL_LOG_ROOT", "/var/log/pods"), "CRI pod log root")
		checkpoint  = flag.String("checkpoint", env("SL_CHECKPOINT", "/var/lib/simplelog/checkpoint.db"), "checkpoint db path (hostPath)")
		nodeName    = flag.String("node", env("NODE_NAME", ""), "node name (downward API)")
		maxHeld     = flag.Int64("max-held-bytes", envInt64("SL_MAX_HELD_BYTES", 64<<20), "soft cap on un-acked held bytes")
		enrichK8s   = flag.Bool("enrich", os.Getenv("SL_ENRICH") != "false", "enrich via Kubernetes API")
		excludeNS   = flag.String("exclude-namespaces", env("SL_EXCLUDE_NAMESPACES", ""), "comma-separated namespaces to never tail")
		ownNS       = flag.String("namespace", env("POD_NAMESPACE", ""), "this pod's namespace (downward API); always self-excluded to prevent log loops")
		excludeAnno = flag.String("exclude-annotation", env("SL_EXCLUDE_ANNOTATION", "simplelog.io/ingest"), "pod annotation key; a pod with <key>=\"false\" is not ingested (requires enrichment)")
		metaWait    = flag.Duration("metadata-wait", envDuration("SL_METADATA_WAIT", 30*time.Second), "when enriching, max time to hold a pod's logs awaiting its metadata")
	)
	flag.Parse()

	log.Printf("simplelog-agent %s", buildinfo.String())

	if *nodeName == "" {
		log.Fatal("NODE_NAME (or -node) is required")
	}

	cp, err := tailer.OpenCheckpoint(*checkpoint)
	if err != nil {
		log.Fatalf("checkpoint: %v", err)
	}
	defer cp.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Ingest client: commit advances the on-disk checkpoint when the manager
	// acks an offset as S3-durable.
	client := ingest.NewClient(ingest.ClientConfig{
		Addr:         *managerAddr,
		MaxHeldBytes: *maxHeld,
	}, func(sourceKey string, offset int64) {
		if err := cp.Advance(sourceKey, offset); err != nil {
			log.Printf("checkpoint advance: %v", err)
		}
	})
	defer client.Close()
	go func() {
		if err := client.Run(ctx); err != nil && ctx.Err() == nil {
			log.Printf("ingest client stopped: %v", err)
		}
	}()

	var enr tailer.Enricher
	if *enrichK8s {
		e, err := enrich.NewInCluster(ctx, *nodeName, *excludeAnno)
		if err != nil {
			log.Fatalf("enrich: %v", err)
		}
		enr = e
	}

	// Always exclude our own namespace so simplelog's logs never re-enter the
	// pipeline (which could otherwise loop), plus any operator-configured ones.
	exclude := splitCSV(*excludeNS)
	if *ownNS != "" {
		exclude = append(exclude, *ownNS)
	}

	t := tailer.New(tailer.Config{
		Root:              *logRoot,
		Node:              *nodeName,
		ExcludeNamespaces: exclude,
		MetadataWait:      *metaWait,
	}, client, enr, cp)
	if len(exclude) > 0 {
		log.Printf("excluding namespaces from tailing: %v", exclude)
	}

	log.Printf("agent tailing %s on node %s -> %s", *logRoot, *nodeName, *managerAddr)
	if err := t.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("tailer: %v", err)
	}

	// Drain briefly so in-flight acks can land before exit.
	time.Sleep(500 * time.Millisecond)
	if d := client.Dropped(); d > 0 {
		log.Printf("WARNING: dropped %d records at the held-bytes cap", d)
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
