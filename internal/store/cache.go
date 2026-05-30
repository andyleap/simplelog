package store

import (
	"container/list"
	"os"
	"path/filepath"
	"sync"
)

// diskCache is a size-capped LRU of segment files on the manager's emptyDir.
// Entries are whole .seg files (go-s3 has no ranged GET, so we download a full
// segment on first touch and read blocks locally thereafter). Sealed segments
// are added here directly by the manager and may later be evicted; a query that
// needs an evicted segment re-downloads it from S3.
type diskCache struct {
	dir     string
	maxByte int64

	mu     sync.Mutex
	ll     *list.List               // front = most recently used
	items  map[string]*list.Element // id -> element
	curByt int64
}

type cacheItem struct {
	id   string
	path string
	size int64
}

func newDiskCache(dir string, maxBytes int64) (*diskCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &diskCache{
		dir:     dir,
		maxByte: maxBytes,
		ll:      list.New(),
		items:   map[string]*list.Element{},
	}, nil
}

// pathFor returns the cache file path for a segment id.
func (c *diskCache) pathFor(id string) string {
	return filepath.Join(c.dir, id+".seg")
}

// admit registers an already-written file (e.g. a freshly sealed segment) in
// the cache, evicting LRU entries if over capacity.
func (c *diskCache) admit(id string, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addLocked(id, size)
	c.evictLocked()
}

func (c *diskCache) addLocked(id string, size int64) {
	if el, ok := c.items[id]; ok {
		it := el.Value.(*cacheItem)
		c.curByt += size - it.size
		it.size = size
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&cacheItem{id: id, path: c.pathFor(id), size: size})
	c.items[id] = el
	c.curByt += size
}

// touch marks an entry most-recently-used and reports whether it is cached.
func (c *diskCache) touch(id string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[id]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheItem).path, true
}

func (c *diskCache) evictLocked() {
	for c.curByt > c.maxByte && c.ll.Len() > 0 {
		el := c.ll.Back()
		it := el.Value.(*cacheItem)
		c.ll.Remove(el)
		delete(c.items, it.id)
		c.curByt -= it.size
		os.Remove(it.path)
	}
}

// remove deletes a segment from the cache and disk (used on retention delete).
func (c *diskCache) remove(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[id]; ok {
		it := el.Value.(*cacheItem)
		c.ll.Remove(el)
		delete(c.items, it.id)
		c.curByt -= it.size
	}
	os.Remove(c.pathFor(id))
}
