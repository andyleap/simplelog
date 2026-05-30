package segment

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"sort"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/klauspost/compress/zstd"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Writer serializes records into the segment format. Records must be appended
// in non-decreasing timestamp order within a block; the caller (the manager's
// open-segment buffer) sorts by timestamp before sealing, so blocks are
// time-sorted and the reader can do a simple k-way merge.
//
// Writer is not safe for concurrent use.
type Writer struct {
	w   io.Writer
	enc *zstd.Encoder

	blockTarget int

	pos      int64 // bytes written to w so far
	curLines bytes.Buffer
	curCount int32
	curMin   time.Time
	curMax   time.Time

	blocks []model.BlockMeta

	// accumulated source summary
	namespaces map[string]struct{}
	pods       map[string]struct{}
	nodes      map[string]struct{}
	containers map[string]struct{}
	labelKeys  map[string]struct{}
	fieldKeys  map[string]struct{}

	segMin    time.Time
	segMax    time.Time
	total     int64
	hasLate   bool
	lastTS    time.Time
	finished  bool
	srcOffset map[string]int64 // max offset per source key
}

// NewWriter creates a Writer that writes a segment to w and immediately emits
// the fixed header.
func NewWriter(w io.Writer, createdUnixNano int64) (*Writer, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	sw := &Writer{
		w:           w,
		enc:         enc,
		blockTarget: defaultBlockTargetBytes,
		namespaces:  map[string]struct{}{},
		pods:        map[string]struct{}{},
		nodes:       map[string]struct{}{},
		containers:  map[string]struct{}{},
		labelKeys:   map[string]struct{}{},
		fieldKeys:   map[string]struct{}{},
		srcOffset:   map[string]int64{},
	}
	if err := sw.writeHeader(createdUnixNano); err != nil {
		return nil, err
	}
	return sw, nil
}

func (sw *Writer) writeHeader(created int64) error {
	var h [headerSize]byte
	copy(h[0:8], magicHead)
	byteOrder.PutUint16(h[8:10], formatVersion)
	byteOrder.PutUint16(h[10:12], flagZstd)
	byteOrder.PutUint64(h[12:20], uint64(created))
	return sw.emit(h[:])
}

func (sw *Writer) emit(b []byte) error {
	n, err := sw.w.Write(b)
	sw.pos += int64(n)
	return err
}

// Append adds a record to the current block, flushing the block first if it
// has reached the size target.
func (sw *Writer) Append(r *model.Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if sw.curLines.Len() > 0 && sw.curLines.Len()+len(line)+1 > sw.blockTarget {
		if err := sw.flushBlock(); err != nil {
			return err
		}
	}

	sw.curLines.Write(line)
	sw.curLines.WriteByte('\n')
	sw.curCount++

	ts := r.Timestamp
	if sw.curCount == 1 || ts.Before(sw.curMin) {
		sw.curMin = ts
	}
	if sw.curCount == 1 || ts.After(sw.curMax) {
		sw.curMax = ts
	}
	if !sw.lastTS.IsZero() && ts.Before(sw.lastTS) {
		sw.hasLate = true
	}
	sw.lastTS = ts

	if sw.total == 0 || ts.Before(sw.segMin) {
		sw.segMin = ts
	}
	if sw.total == 0 || ts.After(sw.segMax) {
		sw.segMax = ts
	}
	sw.total++

	addKey(sw.namespaces, r.Namespace)
	addKey(sw.pods, r.Pod)
	addKey(sw.nodes, r.Node)
	addKey(sw.containers, r.Container)
	for k := range r.Labels {
		addKey(sw.labelKeys, k)
	}
	for k := range r.Body {
		addKey(sw.fieldKeys, k)
	}
	if key := r.Source.Key(); r.Offset > sw.srcOffset[key] {
		sw.srcOffset[key] = r.Offset
	}
	return nil
}

func (sw *Writer) flushBlock() error {
	if sw.curCount == 0 {
		return nil
	}
	comp := sw.enc.EncodeAll(sw.curLines.Bytes(), nil)
	bm := model.BlockMeta{
		FileOffset:  sw.pos,
		CompLen:     int64(len(comp)),
		UncompLen:   int64(sw.curLines.Len()),
		MinTS:       sw.curMin,
		MaxTS:       sw.curMax,
		RecordCount: sw.curCount,
	}
	if err := sw.emit(comp); err != nil {
		return err
	}
	sw.blocks = append(sw.blocks, bm)

	sw.curLines.Reset()
	sw.curCount = 0
	sw.lastTS = time.Time{}
	return nil
}

// Finish flushes the final block, writes the block index, source summary, and
// footer, and returns the segment metadata (without ID/S3Key/LocalPath, which
// the caller fills in). After Finish the Writer must not be reused.
func (sw *Writer) Finish() (model.SegmentMeta, error) {
	var meta model.SegmentMeta
	if sw.finished {
		return meta, io.ErrClosedPipe
	}
	sw.finished = true
	defer sw.enc.Close()

	if err := sw.flushBlock(); err != nil {
		return meta, err
	}

	// Block index.
	indexOffset := sw.pos
	idx := make([]byte, len(sw.blocks)*blockIndexEntrySize)
	for i, b := range sw.blocks {
		off := i * blockIndexEntrySize
		byteOrder.PutUint64(idx[off+0:], uint64(b.FileOffset))
		byteOrder.PutUint64(idx[off+8:], uint64(b.CompLen))
		byteOrder.PutUint64(idx[off+16:], uint64(b.UncompLen))
		byteOrder.PutUint64(idx[off+24:], uint64(b.MinTS.UnixNano()))
		byteOrder.PutUint64(idx[off+32:], uint64(b.MaxTS.UnixNano()))
		byteOrder.PutUint32(idx[off+40:], uint32(b.RecordCount))
	}
	if err := sw.emit(idx); err != nil {
		return meta, err
	}

	// Source summary (zstd-compressed JSON).
	summaryOffset := sw.pos
	meta = model.SegmentMeta{
		MinTS:         sw.segMin,
		MaxTS:         sw.segMax,
		Namespaces:    sortedKeys(sw.namespaces),
		Pods:          sortedKeys(sw.pods),
		Nodes:         sortedKeys(sw.nodes),
		Containers:    sortedKeys(sw.containers),
		LabelKeys:     sortedKeys(sw.labelKeys),
		FieldKeys:     sortedKeys(sw.fieldKeys),
		Blocks:        sw.blocks,
		RecordCount:   sw.total,
		HasLate:       sw.hasLate,
		SourceOffsets: sw.srcOffset,
	}
	summaryJSON, err := json.Marshal(summary{
		Namespaces: meta.Namespaces,
		Pods:       meta.Pods,
		Nodes:      meta.Nodes,
		Containers: meta.Containers,
		LabelKeys:  meta.LabelKeys,
		FieldKeys:  meta.FieldKeys,
	})
	if err != nil {
		return meta, err
	}
	summaryComp := sw.enc.EncodeAll(summaryJSON, nil)
	if err := sw.emit(summaryComp); err != nil {
		return meta, err
	}

	// Footer (last), with CRC over index+summary region for crash detection.
	crc := crc32.New(crcTable)
	crc.Write(idx)
	crc.Write(summaryComp)
	if err := sw.writeFooter(indexOffset, len(sw.blocks), summaryOffset, len(summaryComp), crc.Sum32()); err != nil {
		return meta, err
	}
	meta.SizeBytes = sw.pos
	return meta, nil
}

func (sw *Writer) writeFooter(indexOffset int64, blockCount int, summaryOffset int64, summaryLen int, crc uint32) error {
	var f [footerSize]byte
	byteOrder.PutUint64(f[0:], uint64(indexOffset))
	byteOrder.PutUint32(f[8:], uint32(blockCount))
	byteOrder.PutUint64(f[12:], uint64(summaryOffset))
	byteOrder.PutUint32(f[20:], uint32(summaryLen))
	byteOrder.PutUint64(f[24:], uint64(sw.segMin.UnixNano()))
	byteOrder.PutUint64(f[32:], uint64(sw.segMax.UnixNano()))
	byteOrder.PutUint32(f[40:], crc)
	// f[44:56] reserved
	copy(f[footerSize-8:], magicFoot)
	return sw.emit(f[:])
}

// summary is the JSON shape of the on-disk source summary block.
type summary struct {
	Namespaces []string `json:"namespaces"`
	Pods       []string `json:"pods"`
	Nodes      []string `json:"nodes"`
	Containers []string `json:"containers"`
	LabelKeys  []string `json:"label_keys"`
	FieldKeys  []string `json:"field_keys"`
}

func addKey(m map[string]struct{}, k string) {
	if k != "" {
		m[k] = struct{}{}
	}
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
