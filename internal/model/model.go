// Package model holds the core types shared across the agent and manager:
// log records as they flow over the wire and segment metadata as tracked in
// the catalog.
package model

import (
	"encoding/json"
	"strconv"
	"time"
)

// Stream identifies which output stream a log line came from.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// Source uniquely identifies a specific generation of a container log file on a
// node. It is both the dedup key on the manager and the checkpoint key on the
// agent. Device+Inode+CreatedUnixNano distinguish file generations: when
// kubelet rotation creates a fresh file at the same path, its inode (and ctime)
// differ, yielding a new Source whose offsets correctly restart at zero rather
// than being deduped against the previous generation's high watermark.
type Source struct {
	Node            string `json:"node"`
	Path            string `json:"path"`
	Device          uint64 `json:"dev"`
	Inode           uint64 `json:"ino"`
	CreatedUnixNano int64  `json:"ctime"`
}

// Key returns a stable per-generation string key for maps and checkpoints.
func (s Source) Key() string {
	return s.Node + "\x00" + s.Path + "\x00" +
		strconv.FormatUint(s.Device, 10) + "\x00" +
		strconv.FormatUint(s.Inode, 10) + "\x00" +
		strconv.FormatInt(s.CreatedUnixNano, 10)
}

// Record is one enriched log entry. The envelope fields (prefixed with _ when
// addressed in queries) wrap the Body, which is the application's structured
// log object for JSON lines, or {"stdout"/"stderr": "<text>"} for plain text.
type Record struct {
	// Offset is the byte offset of the END of this line within Source's file.
	// The manager acks up to and dedups on (Source.Key, Offset).
	Source Source `json:"source"`
	Offset int64  `json:"offset"`

	Timestamp time.Time `json:"_ts"`
	Namespace string    `json:"_namespace"`
	Pod       string    `json:"_pod"`
	Container string    `json:"_container"`
	Node      string    `json:"_node"`
	Image     string    `json:"_image,omitempty"`
	Stream    string    `json:"_stream"`

	Labels      map[string]string `json:"_labels,omitempty"`
	Annotations map[string]string `json:"_annotations,omitempty"`

	// Message is the raw log message (the part after the CRI prefix), always
	// available via the _message query field.
	Message string `json:"_message"`

	// Body is the structured view of the message: the parsed JSON object for
	// JSON lines, or {stream: message} for plain text. Stored as already-decoded
	// JSON so the query evaluator can address nested fields.
	Body map[string]json.RawMessage `json:"body,omitempty"`
}

// SegmentMeta is the catalog entry describing one sealed segment. It is also
// the content of the .seg.meta sidecar object in S3 (minus LocalPath, which is
// node-local cache state). Everything here is derivable from the segment's
// footer, so the catalog is fully rebuildable from S3.
type SegmentMeta struct {
	ID    string `json:"id"`     // ULID; unique and time-sortable
	S3Key string `json:"s3_key"` // key of the .seg object

	MinTS time.Time `json:"min_ts"`
	MaxTS time.Time `json:"max_ts"`

	Namespaces []string `json:"namespaces"`
	Pods       []string `json:"pods"`
	Nodes      []string `json:"nodes"`
	Containers []string `json:"containers"`
	LabelKeys  []string `json:"label_keys"`
	FieldKeys  []string `json:"field_keys"`

	Blocks      []BlockMeta `json:"blocks"`
	SizeBytes   int64       `json:"size_bytes"`
	RecordCount int64       `json:"record_count"`
	HasLate     bool        `json:"has_late"`

	// SourceOffsets is the highest record offset per source key contained in
	// this segment. It makes the dedup watermark durable: a manager recovering
	// from S3 seeds its watermark from these so already-sealed records that get
	// redelivered (their ack was lost on restart) are dropped, not re-sealed.
	SourceOffsets map[string]int64 `json:"source_offsets,omitempty"`

	// LocalPath is the on-disk cache location, or "" if the segment is only in
	// S3. Not serialized into the sidecar.
	LocalPath string `json:"-"`
}

// BlockMeta describes one zstd block within a segment, enabling block-level
// time pruning during query execution.
type BlockMeta struct {
	FileOffset  int64     `json:"file_offset"`
	CompLen     int64     `json:"comp_len"`
	UncompLen   int64     `json:"uncomp_len"`
	MinTS       time.Time `json:"min_ts"`
	MaxTS       time.Time `json:"max_ts"`
	RecordCount int32     `json:"record_count"`
}

// Overlaps reports whether the block's time span intersects [start, end].
// A zero start or end is treated as unbounded.
func (b BlockMeta) Overlaps(start, end time.Time) bool {
	if !start.IsZero() && b.MaxTS.Before(start) {
		return false
	}
	if !end.IsZero() && b.MinTS.After(end) {
		return false
	}
	return true
}

// Overlaps reports whether the segment's time span intersects [start, end].
func (m SegmentMeta) Overlaps(start, end time.Time) bool {
	if !start.IsZero() && m.MaxTS.Before(start) {
		return false
	}
	if !end.IsZero() && m.MinTS.After(end) {
		return false
	}
	return true
}
