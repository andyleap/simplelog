// Package segment implements simplelog's on-disk/S3 segment format: NDJSON
// records grouped into independently-decompressible zstd blocks, with a
// block index and a footer written last so that a crash mid-seal is detectable
// (a segment without a valid footer is treated as unsealed).
//
// Layout:
//
//	HEADER (fixed)                  magic "SLOGSEG1", version, flags, created
//	BLOCK 0 .. BLOCK N-1            each a complete zstd frame of whole NDJSON lines
//	BLOCK INDEX                     fixed-width entries (offset/len/min_ts/max_ts/count)
//	SOURCE SUMMARY                  zstd-compressed JSON (namespaces/pods/.../field keys)
//	FOOTER (fixed, last)            index/summary offsets, seg min/max ts, crc32c, end magic
package segment

import "encoding/binary"

const (
	magicHead = "SLOGSEG1"
	magicFoot = "SLOGEND1"

	formatVersion = uint16(1)

	headerSize = 32 // magic(8) + version(2) + flags(2) + created(8) + reserved(12)
	footerSize = 64 // see writeFooter

	// blockIndexEntrySize is the fixed wire size of one BlockMeta on disk:
	// fileOffset(8) + compLen(8) + uncompLen(8) + minTS(8) + maxTS(8) + count(4)
	blockIndexEntrySize = 44

	flagZstd = uint16(1 << 0)

	// defaultBlockTargetBytes is the uncompressed size at which the writer
	// seals the current block and starts a new one.
	defaultBlockTargetBytes = 2 << 20 // 2 MiB
)

var byteOrder = binary.BigEndian
