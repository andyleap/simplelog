// Command manager is the simplelog log manager: it ingests records from node
// agents over DRPC, seals them into compressed segments on local scratch,
// archives them to S3, and serves an HTTP query/streaming API.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/andyleap/simplelog/internal/buildinfo"
	"github.com/andyleap/simplelog/internal/manager"
	"github.com/andyleap/simplelog/internal/store"
)

func main() {
	var (
		ingestAddr = flag.String("ingest-addr", env("SL_INGEST_ADDR", ":9000"), "DRPC ingest listen address")
		httpAddr   = flag.String("http-addr", env("SL_HTTP_ADDR", ":8080"), "HTTP query API listen address")
		baseDir    = flag.String("base-dir", env("SL_BASE_DIR", "/data"), "emptyDir scratch/cache/catalog root")
		managerID  = flag.String("manager-id", env("SL_MANAGER_ID", "mgr1"), "stable manager identity for S3 keys")
		segBytes   = flag.Int64("seg-bytes", envInt64("SL_SEG_BYTES", 64<<20), "seal segment at this size")
		segAge     = flag.Duration("seg-age", envDuration("SL_SEG_AGE", 5*time.Minute), "seal segment at this age")
		cacheBytes = flag.Int64("cache-bytes", envInt64("SL_CACHE_BYTES", 2<<30), "local segment cache cap")

		s3Bucket = flag.String("s3-bucket", env("SL_S3_BUCKET", ""), "S3 bucket")
		s3Domain = flag.String("s3-domain", env("SL_S3_DOMAIN", "s3.amazonaws.com"), "S3 endpoint host")
		s3Region = flag.String("s3-region", env("SL_S3_REGION", "us-east-1"), "S3 region")
		s3Proto  = flag.String("s3-protocol", env("SL_S3_PROTOCOL", "https"), "S3 protocol (http/https)")
		s3Path   = flag.Bool("s3-path-style", os.Getenv("SL_S3_PATH_STYLE") == "true", "use path-style addressing (MinIO)")
	)
	flag.Parse()

	log.Printf("simplelog-manager %s", buildinfo.String())

	if *s3Bucket == "" {
		log.Fatal("SL_S3_BUCKET (or -s3-bucket) is required")
	}
	blobs, err := store.NewS3Blobs(store.S3Config{
		AccessKeyID:     os.Getenv("SL_S3_ACCESS_KEY"),
		SecretAccessKey: os.Getenv("SL_S3_SECRET_KEY"),
		Region:          *s3Region,
		Bucket:          *s3Bucket,
		Domain:          *s3Domain,
		Protocol:        *s3Proto,
		UsePathBuckets:  *s3Path,
	})
	if err != nil {
		log.Fatalf("s3: %v", err)
	}

	st, err := store.Open(store.Config{
		ManagerID:    *managerID,
		BaseDir:      *baseDir,
		CacheMaxByte: *cacheBytes,
	}, blobs)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	log.Printf("recovering catalog from S3...")
	if err := st.Recover(); err != nil {
		log.Fatalf("recover: %v", err)
	}

	mgr, err := manager.New(st, manager.SpoolConfig{
		WorkDir:     *baseDir + "/work",
		MaxSegBytes: *segBytes,
		MaxSegAge:   *segAge,
	})
	if err != nil {
		log.Fatalf("manager: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// DRPC ingest server.
	lis, err := net.Listen("tcp", *ingestAddr)
	if err != nil {
		log.Fatalf("listen ingest: %v", err)
	}
	go func() {
		log.Printf("ingest (DRPC) listening on %s", *ingestAddr)
		if err := mgr.ServeIngest(ctx, lis); err != nil && ctx.Err() == nil {
			log.Fatalf("ingest serve: %v", err)
		}
	}()

	// HTTP query API.
	httpSrv := &http.Server{Addr: *httpAddr, Handler: mgr.Handler()}
	go func() {
		log.Printf("query API (HTTP) listening on %s", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	// Rotation + snapshot loop.
	go mgr.RunSealLoop(ctx)

	<-ctx.Done()
	log.Printf("shutting down: sealing open segment...")
	shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	mgr.Shutdown()
	httpSrv.Shutdown(shutCtx)
	// Give the ingest ack loop a moment to flush final acks to agents.
	time.Sleep(time.Second)
	log.Printf("bye")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
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

func envDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
