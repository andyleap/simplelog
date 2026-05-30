package store

import (
	"encoding/json"

	"github.com/andyleap/simplelog/internal/model"
)

// marshalSidecar serializes the segment metadata for the .seg.meta object. It
// is just the SegmentMeta as JSON (LocalPath is excluded via its json:"-" tag).
func marshalSidecar(m model.SegmentMeta) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

func unmarshalSidecar(b []byte) (model.SegmentMeta, error) {
	var m model.SegmentMeta
	err := json.Unmarshal(b, &m)
	return m, err
}
