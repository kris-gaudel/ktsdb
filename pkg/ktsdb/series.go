package ktsdb

import (
	"encoding/json"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
)

// SeriesID is a unique identifier for a time series.
// Computed as xxHash of metric name + sorted tagset.
type SeriesID uint64

// SeriesMeta contains metadata about a series.
type SeriesMeta struct {
	Metric string `json:"m"`
	Tags   Tagset `json:"t,omitempty"`
}

// SeriesHasher computes series IDs without allocations.
// Not thread-safe - use one per goroutine or from a sync.Pool.
type SeriesHasher struct {
	h *xxhash.Digest
}

var hasherPool = sync.Pool{
	New: func() interface{} {
		return &SeriesHasher{h: xxhash.New()}
	},
}

func getHasher() *SeriesHasher {
	return hasherPool.Get().(*SeriesHasher)
}

func putHasher(h *SeriesHasher) {
	hasherPool.Put(h)
}

// ComputeSeriesID computes the series ID for a metric and tagset.
// The tagset must be pre-sorted for consistent results.
func (s *SeriesHasher) ComputeSeriesID(metric string, tags Tagset) SeriesID {
	s.h.Reset()
	s.h.WriteString(metric)
	for _, t := range tags {
		s.h.WriteString(t.Key)
		s.h.WriteString(t.Value)
	}
	return SeriesID(s.h.Sum64())
}

// ComputeSeriesID computes a series ID from a metric and tagset.
// Tags must be sorted for consistent results.
func ComputeSeriesID(metric string, tags Tagset) SeriesID {
	h := getHasher()
	id := h.ComputeSeriesID(metric, tags)
	putHasher(h)
	return id
}

// SeriesRegistry manages series metadata and caches known series.
type SeriesRegistry struct {
	db    *badger.DB
	cache sync.Map // SeriesID -> struct{} for existence check
}

func newSeriesRegistry(db *badger.DB) *SeriesRegistry {
	return &SeriesRegistry{db: db}
}

// GetOrCreate returns the series ID for the given metric and tags.
// Tags are sorted in-place for consistent hashing.
// Returns the series ID and whether the series was newly created.
func (r *SeriesRegistry) GetOrCreate(metric string, tags Tagset) (SeriesID, bool, error) {
	tags.Sort()
	id := ComputeSeriesID(metric, tags)

	if _, exists := r.cache.Load(id); exists {
		return id, false, nil
	}

	keyBuf := make([]byte, SeriesKeySize)
	EncodeSeriesKey(keyBuf, uint64(id))

	var created bool
	err := r.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(keyBuf)
		if err == nil {
			r.cache.Store(id, struct{}{})
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		meta := SeriesMeta{Metric: metric, Tags: tags}
		value, err := json.Marshal(meta)
		if err != nil {
			return err
		}

		if err := txn.Set(keyBuf, value); err != nil {
			return err
		}

		created = true
		r.cache.Store(id, struct{}{})
		return nil
	})

	return id, created, err
}

// Get retrieves the metadata for a series ID.
func (r *SeriesRegistry) Get(id SeriesID) (*SeriesMeta, error) {
	keyBuf := make([]byte, SeriesKeySize)
	EncodeSeriesKey(keyBuf, uint64(id))

	var meta SeriesMeta
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyBuf)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &meta)
		})
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Exists checks if a series ID exists in the registry.
func (r *SeriesRegistry) Exists(id SeriesID) bool {
	if _, exists := r.cache.Load(id); exists {
		return true
	}

	keyBuf := make([]byte, SeriesKeySize)
	EncodeSeriesKey(keyBuf, uint64(id))

	err := r.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(keyBuf)
		return err
	})

	if err == nil {
		r.cache.Store(id, struct{}{})
		return true
	}
	return false
}
