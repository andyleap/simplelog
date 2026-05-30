# simplelog

A lightweight, self-hosted distributed logging system for a small Kubernetes
cluster, in Go. Two services:

- **agent** (DaemonSet) — tails CRI container logs on each node, enriches them
  with Kubernetes metadata, and streams them to the manager over DRPC.
- **manager** (single-replica StatefulSet) — buffers records in memory, seals
  them into compressed segments on ephemeral local disk, archives to S3, and
  serves an HTTP query/streaming API. S3 is the only durable store; the local
  catalog is rebuilt from S3 on startup.

## Design

Guiding principle: for a small cluster the on-disk container logs plus a single
manager are enough — don't build a distributed datastore. Durability lives in
S3; the hot path stays in memory.

```
 node                                    manager (1 replica, emptyDir + S3)
┌───────────────┐                       ┌─────────────────────────────────┐
│ agent         │   DRPC stream         │ ingest server ─ dedup ─ open buf │
│  tailer ──────┼──── records ────────► │        │                         │
│  (holds fds,  │ ◄─── acks ────────────┤        ▼ seal (size/age/SIGTERM) │
│  checkpoints) │                       │  segment (zstd blocks + footer)  │
│  enrich (k8s) │                       │        │  + .seg.meta sidecar    │
└───────────────┘                       │        ▼                         │
                                        │  S3  ◄── catalog (bbolt cache)   │
   HTTP query/follow ◄──────────────────┤  query: prune → merge → filter   │
                                        └─────────────────────────────────┘
```

Key decisions:

- **Transport: DRPC** (`storj.io/drpc`) — bidirectional streaming + acks,
  lightweight (~3k LOC, few deps).
- **Agent buffering: the log files are the buffer.** The agent holds file
  descriptors open across rotation (an open fd to an unlinked file stays
  readable, so kubelet can't delete un-forwarded logs) and persists a per-file
  high-water mark to a hostPath checkpoint. It resumes from the last *acked*
  offset after a restart. A configurable `MaxHeldBytes` soft cap applies
  backpressure; a hard cap drops oldest data to protect node memory.
- **No PVC.** Manager local disk is an `emptyDir` scratch/cache. S3 is
  authoritative; the bbolt catalog is rebuilt from per-segment `.seg.meta`
  sidecars (plus a periodic catalog snapshot for fast recovery).
- **Ack-on-durability.** The manager acks an offset only after the segment
  containing it is sealed and uploaded to S3. A crash loses only un-acked data,
  which the agent redelivers; the dedup watermark (seeded from sealed segments'
  per-source max offsets) prevents re-sealed duplicates. Effectively-once for
  the common case.
- **Segment format: NDJSON in independently-decompressible zstd blocks**, each a
  whole number of time-sorted records, with a block index and footer-written-last
  (so a crash mid-seal is detected as unsealed). `go-s3` has no ranged GET, so a
  query downloads a whole segment once into an LRU cache; the segment age cap
  bounds the over-fetch.
- **Records** wrap a metadata envelope (`_namespace`, `_pod`, …) around a `body`:
  the parsed JSON object for JSON log lines, or `{stdout|stderr: "…"}` for plain
  text.

## Query language

A small boolean expression language, evaluated against each record:

```
expr    := orExpr
orExpr  := andExpr ("or" andExpr)*
andExpr := notExpr ("and" notExpr)*
notExpr := "not" notExpr | primary
primary := "(" expr ")" | field op value | field     // bare field = exists/non-empty
op      := "=" | "!=" | "=~" | "!~" | "<" | "<=" | ">" | ">=" | ":"   (: = contains)
field   := IDENT ("." IDENT | "[" STRING "]")*
value   := STRING | NUMBER | BOOL | /regex/
```

- Envelope fields: `_namespace _pod _container _node _image _stream _ts _message`.
- `label.<k>` / `annotation.<k>` (use `label["app.kubernetes.io/name"]` for dotted keys).
- Any other path resolves into the structured `body` (e.g. `body.level`, `level`,
  `stdout`, `user.id`).
- The literal's type drives coercion: a numeric literal forces numeric
  comparison, a string literal compares as strings. Errors surface at parse
  time; evaluation never errors (a non-matching coercion is just `false`).

Examples:

```
_namespace="prod" and level="error"
_pod =~ /web-.*/ and status >= 500
_message : "panic" or level="fatal"
label["app.kubernetes.io/name"]="frontend" and not _container="istio-proxy"
```

### HTTP API

```
GET /v1/query?expr=<expr>&start=<t>&end=<t>&limit=<n>&direction=asc|desc&follow=true
GET /v1/fields                                            field/value autocomplete data
GET /v1/histogram?expr=<expr>&start=<t>&end=<t>&buckets=<n>   time-bucketed match counts
GET /healthz
GET /                                                     embedded web UI
```

`start`/`end` accept RFC3339 or unix seconds. `/v1/query` responses are
newline-delimited JSON (one record per line). `follow=true` replays history then
streams live (gapless: it subscribes before replaying and dedups the boundary).

### Web UI

The manager serves a built-in web UI at `/`, compiled into the binary via
`embed.FS` ([internal/ui](internal/ui/)) — no separate service, no JS build step
(plain ES modules). It provides a query editor for the expression language with a
clickable field/value helper, time-range presets + custom range, a results table
with a column picker, a time histogram, live tail, and saved queries
(localStorage). The `internal/ui/dist` embed boundary lets a framework build
(Vite/Svelte/etc.) be dropped in later without backend changes.

```sh
# last 100 errors in prod, newest first
curl 'http://manager:8080/v1/query?expr=_namespace%3D%22prod%22%20and%20level%3D%22error%22&limit=100&direction=desc'

# live tail a pod
curl -N 'http://manager:8080/v1/query?expr=_pod%3D%22web-abc%22&follow=true'
```

## Excluding pods / avoiding log feedback loops

The agent tails every pod's logs under `/var/log/pods`, *including simplelog's
own pods*. To prevent a feedback loop (a log line about processing a log line):

- **No per-record logging.** Nothing in the ingest/seal/query hot paths logs as
  a result of processing a record; the binaries log only startup, shutdown, and
  genuine errors. So even if a simplelog line were ingested, it could not
  amplify.
- **Per-pod opt-out annotation** (configurable). A pod carrying
  `simplelog.io/ingest: "false"` is never tailed. The agent and manager
  manifests set it on themselves. The annotation key is configurable via
  `-exclude-annotation` / `SL_EXCLUDE_ANNOTATION` (requires enrichment, since it
  reads the annotation from the Pod informer).
- **Metadata gating.** When enrichment is enabled, the agent holds a pod's logs
  (keeping the file descriptor open so nothing is lost) until that pod's
  metadata is available from the informer, so records are never shipped
  un-enriched and the exclude annotation is always known before anything is
  forwarded. A configurable grace period (`-metadata-wait` /
  `SL_METADATA_WAIT`, default 30s) bounds the wait so logs of an
  already-deleted pod (which the informer will never report) eventually flush
  rather than pinning the descriptor forever.
- **Namespace exclusion** (configurable, race-free). The agent always
  self-excludes its own namespace (`POD_NAMESPACE`, downward API) so simplelog's
  logs are skipped even before the informer has synced; additional namespaces
  can be excluded via `-exclude-namespaces` / `SL_EXCLUDE_NAMESPACES`.

Each binary logs its version at startup (injected by GoReleaser via
`internal/buildinfo`, falling back to Go's embedded VCS info for local builds):

```
simplelog-manager v1.2.3 (commit abcdef012345, built 2026-05-30T21:00:00Z)
```

## Build & test

```sh
make build         # build all packages
make test          # unit + in-process integration tests
make proto         # regenerate DRPC code (needs protoc + the two plugins)
make agent manager # static binaries into bin/

# MinIO-backed S3 tests (start MinIO first, then):
SL_TEST_MINIO=1 go test ./internal/store/ -run MinIO
```

## Deploy

```sh
cp deploy/s3-secret.example.yaml deploy/s3-secret.yaml   # fill in S3 creds
kubectl apply -f deploy/agent.yaml      # namespace + RBAC + DaemonSet
kubectl apply -f deploy/s3-secret.yaml
kubectl apply -f deploy/manager.yaml    # StatefulSet + headless Service
```

Build images with `docker build --target agent .` and `--target manager .`.

## Layout

```
cmd/agent, cmd/manager   service entrypoints
internal/model           shared record + segment-metadata types
internal/segment         zstd-block segment writer/reader
internal/store           bbolt catalog, S3 (jhunt/go-s3), LRU cache, recovery
internal/ingest          DRPC server (manager) + client (agent)
internal/query           lexer/parser/evaluator, planner, k-way merge, live-tail hub
internal/manager         open-segment spool, seal loop, HTTP API
internal/ui              embedded web UI (no build step)
internal/tailer          CRI log tailing, fd-hold, checkpoints
internal/enrich          node-scoped Pod informer
proto/                   DRPC ingest service definition + generated code
deploy/                  Kubernetes manifests
```
