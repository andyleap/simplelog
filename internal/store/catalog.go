package store

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	bolt "go.etcd.io/bbolt"
)

var segmentsBucket = []byte("segments")

// catalog is the bbolt-backed segment index. It lives on the manager's
// emptyDir and is a rebuildable cache: S3 is authoritative (see Store.Recover).
// Keys sort by segment start time so a cursor scan yields time-ordered segments.
type catalog struct {
	db *bolt.DB
}

func openCatalog(path string) (*catalog, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(segmentsBucket)
		return e
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &catalog{db: db}, nil
}

func (c *catalog) close() error { return c.db.Close() }

// segKey builds the time-sortable catalog key: bigendian(minTS unixnano) + id.
func segKey(minTS time.Time, id string) []byte {
	k := make([]byte, 8+len(id))
	binary.BigEndian.PutUint64(k[:8], uint64(minTS.UnixNano()))
	copy(k[8:], id)
	return k
}

// put inserts or updates a segment entry. LocalPath is persisted so cache state
// survives a manager restart that keeps its emptyDir (graceful restart).
func (c *catalog) put(m model.SegmentMeta) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(segmentsBucket)
		buf, err := json.Marshal(catalogEntry{Meta: m, LocalPath: m.LocalPath})
		if err != nil {
			return err
		}
		return b.Put(segKey(m.MinTS, m.ID), buf)
	})
}

func (c *catalog) delete(m model.SegmentMeta) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(segmentsBucket).Delete(segKey(m.MinTS, m.ID))
	})
}

// has reports whether a segment with the given id is already cataloged.
func (c *catalog) has(id string) (bool, error) {
	found := false
	err := c.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(segmentsBucket).ForEach(func(_, v []byte) error {
			var e catalogEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			if e.Meta.ID == id {
				found = true
			}
			return nil
		})
	})
	return found, err
}

// overlapping returns segments whose time span intersects [start, end], in
// ascending start-time order.
func (c *catalog) overlapping(start, end time.Time) ([]model.SegmentMeta, error) {
	var out []model.SegmentMeta
	err := c.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(segmentsBucket).Cursor()
		// Start the scan just before `start`; segments are keyed by MinTS, but a
		// segment starting before `start` can still overlap, so scan all and
		// filter. At small-cluster scale this is cheap; an upper bound on the
		// cursor (MaxTS) is the optimization if it ever matters.
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var e catalogEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			m := e.Meta
			m.LocalPath = e.LocalPath
			if m.Overlaps(start, end) {
				out = append(out, m)
			}
		}
		return nil
	})
	return out, err
}

// all returns every cataloged segment.
func (c *catalog) all() ([]model.SegmentMeta, error) {
	return c.overlapping(time.Time{}, time.Time{})
}

// snapshot returns a consistent copy of the bbolt file for backup to S3.
func (c *catalog) snapshot() ([]byte, error) {
	var buf []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		buf = make([]byte, 0, tx.Size())
		w := byteWriter{&buf}
		_, e := tx.WriteTo(&w)
		return e
	})
	return buf, err
}

type catalogEntry struct {
	Meta      model.SegmentMeta `json:"meta"`
	LocalPath string            `json:"local_path,omitempty"`
}

// byteWriter is a tiny io.Writer that appends to a byte slice.
type byteWriter struct{ b *[]byte }

func (w *byteWriter) Write(p []byte) (int, error) {
	*w.b = append(*w.b, p...)
	return len(p), nil
}
