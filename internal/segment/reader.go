package segment

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/klauspost/compress/zstd"
)

// ErrUnsealed indicates a segment whose footer is missing or invalid — i.e. a
// segment that was never fully written (crash mid-seal). Such files must not be
// queried or uploaded; the agent will redeliver their un-acked records.
var ErrUnsealed = errors.New("segment: missing or invalid footer (unsealed)")

// Reader provides random access to a sealed segment via an io.ReaderAt.
type Reader struct {
	r    io.ReaderAt
	size int64

	blocks  []model.BlockMeta
	summary summary
	segMin  time.Time
	segMax  time.Time
}

// Open reads and validates the footer, block index, and source summary of a
// segment of the given size. It returns ErrUnsealed if the footer is absent or
// corrupt.
func Open(r io.ReaderAt, size int64) (*Reader, error) {
	if size < headerSize+footerSize {
		return nil, ErrUnsealed
	}
	foot := make([]byte, footerSize)
	if _, err := r.ReadAt(foot, size-footerSize); err != nil {
		return nil, err
	}
	if string(foot[footerSize-8:]) != magicFoot {
		return nil, ErrUnsealed
	}
	indexOffset := int64(byteOrder.Uint64(foot[0:]))
	blockCount := int(byteOrder.Uint32(foot[8:]))
	summaryOffset := int64(byteOrder.Uint64(foot[12:]))
	summaryLen := int(byteOrder.Uint32(foot[20:]))
	segMin := time.Unix(0, int64(byteOrder.Uint64(foot[24:]))).UTC()
	segMax := time.Unix(0, int64(byteOrder.Uint64(foot[32:]))).UTC()
	wantCRC := byteOrder.Uint32(foot[40:])

	idxLen := blockCount * blockIndexEntrySize
	if indexOffset < headerSize || indexOffset+int64(idxLen) > size ||
		summaryOffset < indexOffset+int64(idxLen) || summaryOffset+int64(summaryLen) > size-footerSize {
		return nil, ErrUnsealed
	}

	idx := make([]byte, idxLen)
	if _, err := r.ReadAt(idx, indexOffset); err != nil {
		return nil, err
	}
	summaryComp := make([]byte, summaryLen)
	if _, err := r.ReadAt(summaryComp, summaryOffset); err != nil {
		return nil, err
	}

	crc := crc32.New(crcTable)
	crc.Write(idx)
	crc.Write(summaryComp)
	if crc.Sum32() != wantCRC {
		return nil, fmt.Errorf("%w: index crc mismatch", ErrUnsealed)
	}

	blocks := make([]model.BlockMeta, blockCount)
	for i := range blocks {
		off := i * blockIndexEntrySize
		blocks[i] = model.BlockMeta{
			FileOffset:  int64(byteOrder.Uint64(idx[off+0:])),
			CompLen:     int64(byteOrder.Uint64(idx[off+8:])),
			UncompLen:   int64(byteOrder.Uint64(idx[off+16:])),
			MinTS:       time.Unix(0, int64(byteOrder.Uint64(idx[off+24:]))).UTC(),
			MaxTS:       time.Unix(0, int64(byteOrder.Uint64(idx[off+32:]))).UTC(),
			RecordCount: int32(byteOrder.Uint32(idx[off+40:])),
		}
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	summaryJSON, err := dec.DecodeAll(summaryComp, nil)
	if err != nil {
		return nil, err
	}
	var sum summary
	if err := json.Unmarshal(summaryJSON, &sum); err != nil {
		return nil, err
	}

	return &Reader{r: r, size: size, blocks: blocks, summary: sum, segMin: segMin, segMax: segMax}, nil
}

// Blocks returns the block index.
func (rd *Reader) Blocks() []model.BlockMeta { return rd.blocks }

// Meta reconstructs the SegmentMeta from the footer/index/summary. ID, S3Key,
// and LocalPath are left empty for the caller to fill.
func (rd *Reader) Meta() model.SegmentMeta {
	var count int64
	for _, b := range rd.blocks {
		count += int64(b.RecordCount)
	}
	return model.SegmentMeta{
		MinTS:       rd.segMin,
		MaxTS:       rd.segMax,
		Namespaces:  rd.summary.Namespaces,
		Pods:        rd.summary.Pods,
		Nodes:       rd.summary.Nodes,
		Containers:  rd.summary.Containers,
		LabelKeys:   rd.summary.LabelKeys,
		FieldKeys:   rd.summary.FieldKeys,
		Blocks:      rd.blocks,
		SizeBytes:   rd.size,
		RecordCount: count,
	}
}

// ReadBlock decompresses block i and invokes fn for each record in timestamp
// order. fn may retain the record; a fresh Record is decoded per line.
func (rd *Reader) ReadBlock(i int, fn func(*model.Record) error) error {
	if i < 0 || i >= len(rd.blocks) {
		return fmt.Errorf("segment: block %d out of range", i)
	}
	b := rd.blocks[i]
	comp := make([]byte, b.CompLen)
	if _, err := rd.r.ReadAt(comp, b.FileOffset); err != nil {
		return err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(comp, nil)
	if err != nil {
		return err
	}
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		var rec model.Record
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			return err
		}
		if err := fn(&rec); err != nil {
			return err
		}
	}
	return sc.Err()
}
