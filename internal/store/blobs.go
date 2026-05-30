package store

import (
	"io"
	"sort"
	"sync"
)

// Blobs is the object-store abstraction the rest of the manager depends on. All
// S3 access goes through this interface, so a future ranged-GET-capable client
// (or an in-memory fake for tests) can be swapped in one place.
type Blobs interface {
	// Get returns the full contents of an object.
	Get(key string) ([]byte, error)
	// Put stores data under key (whole object).
	Put(key string, data []byte) error
	// PutReader streams r to key.
	PutReader(key string, r io.Reader) error
	// List returns keys with the given prefix.
	List(prefix string) ([]string, error)
	// Delete removes an object. Deleting a missing object is not an error.
	Delete(key string) error
}

// MemBlobs is an in-memory Blobs implementation for tests.
type MemBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemBlobs returns an empty in-memory blob store.
func NewMemBlobs() *MemBlobs { return &MemBlobs{data: map[string][]byte{}} }

func (m *MemBlobs) Get(key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, &NotFoundError{Key: key}
	}
	return append([]byte(nil), b...), nil
}

func (m *MemBlobs) Put(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), data...)
	return nil
}

func (m *MemBlobs) PutReader(key string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return m.Put(key, b)
}

func (m *MemBlobs) List(prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (m *MemBlobs) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// NotFoundError indicates a missing object.
type NotFoundError struct{ Key string }

func (e *NotFoundError) Error() string { return "store: object not found: " + e.Key }

// IsNotFound reports whether err is a NotFoundError.
func IsNotFound(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}
