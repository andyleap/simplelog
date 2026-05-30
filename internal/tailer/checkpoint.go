package tailer

import (
	"encoding/binary"
	"time"

	bolt "go.etcd.io/bbolt"
)

var checkpointBucket = []byte("offsets")

// Checkpoint persists the per-source committed (acked) offset on a node-local
// hostPath volume. On restart the tailer resumes each file from its checkpoint,
// so only un-acked data is re-read (and the manager dedups any overlap).
type Checkpoint struct {
	db *bolt.DB
}

// OpenCheckpoint opens (creating if needed) the checkpoint database.
func OpenCheckpoint(path string) (*Checkpoint, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(checkpointBucket)
		return e
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Checkpoint{db: db}, nil
}

// Close closes the database.
func (c *Checkpoint) Close() error { return c.db.Close() }

// Get returns the committed offset for a source key (0 if absent).
func (c *Checkpoint) Get(key string) int64 {
	var off int64
	c.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(checkpointBucket).Get([]byte(key))
		if len(v) == 8 {
			off = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return off
}

// Advance records that key has been committed up to offset, only ever moving
// the watermark forward. Safe for concurrent use.
func (c *Checkpoint) Advance(key string, offset int64) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(checkpointBucket)
		if v := b.Get([]byte(key)); len(v) == 8 {
			if cur := int64(binary.BigEndian.Uint64(v)); cur >= offset {
				return nil
			}
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(offset))
		return b.Put([]byte(key), buf[:])
	})
}
