// Package tailer reads CRI container log files, holds file descriptors open
// across rotation so kubelet cannot delete un-forwarded logs, checkpoints a
// per-file high-water mark, and emits enriched records to the ingest client.
package tailer

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"time"

	"github.com/andyleap/simplelog/internal/model"
)

// PodInfo is the identity parsed from a CRI log file path.
type PodInfo struct {
	Namespace string
	Pod       string
	UID       string
	Container string
}

// parsePath extracts pod identity from a path of the form
// /var/log/pods/<ns>_<pod>_<uid>/<container>/<n>.log. It returns ok=false for
// paths that don't match that layout.
func parsePath(path string) (PodInfo, bool) {
	parts := strings.Split(filepath.ToSlash(path), "/")
	// Find the "pods" segment; the pod dir and container dir follow it.
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] != "pods" {
			continue
		}
		podDir := parts[i+1]
		container := parts[i+2]
		// <ns>_<pod>_<uid>: namespace and pod are DNS-1123 (no underscores),
		// so split into exactly three on "_".
		seg := strings.SplitN(podDir, "_", 3)
		if len(seg) != 3 {
			return PodInfo{}, false
		}
		return PodInfo{
			Namespace: seg[0],
			Pod:       seg[1],
			UID:       seg[2],
			Container: container,
		}, true
	}
	return PodInfo{}, false
}

// criLine is one parsed CRI log line.
type criLine struct {
	ts      time.Time
	stream  string // stdout|stderr
	partial bool   // tag "P" (partial) vs "F" (full)
	message string
}

// parseCRILine parses a single CRI-format log line:
//
//	<RFC3339Nano> <stdout|stderr> <F|P> <message...>
func parseCRILine(line string) (criLine, bool) {
	parts := strings.SplitN(line, " ", 4)
	if len(parts) < 4 {
		// A line with an empty message still has 3 fields + trailing space.
		if len(parts) == 3 {
			parts = append(parts, "")
		} else {
			return criLine{}, false
		}
	}
	ts, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return criLine{}, false
	}
	return criLine{
		ts:      ts,
		stream:  parts[1],
		partial: parts[2] == "P",
		message: parts[3],
	}, true
}

// buildRecord assembles a Record from pod identity, a parsed line, and the
// byte offset of the end of the line within its file. Enrichment (labels,
// node, image) is applied later by the Enricher.
func buildRecord(src model.Source, pod PodInfo, cl criLine, offset int64) *model.Record {
	r := &model.Record{
		Source:    src,
		Offset:    offset,
		Timestamp: cl.ts,
		Namespace: pod.Namespace,
		Pod:       pod.Pod,
		Container: pod.Container,
		Node:      src.Node,
		Stream:    cl.stream,
		Message:   cl.message,
	}
	r.Body = bodyFor(cl)
	return r
}

// bodyFor produces the structured body: the parsed JSON object if the message
// is a JSON object, otherwise {stdout|stderr: message}.
func bodyFor(cl criLine) map[string]json.RawMessage {
	trimmed := strings.TrimSpace(cl.message)
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &obj); err == nil {
			return obj
		}
	}
	msg, _ := json.Marshal(cl.message)
	stream := cl.stream
	if stream != model.StreamStdout && stream != model.StreamStderr {
		stream = model.StreamStdout
	}
	return map[string]json.RawMessage{stream: msg}
}
