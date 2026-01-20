package ktsdb

import (
	"bytes"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/dgraph-io/badger/v4"
)

// TagIndex is an inverted index mapping tag:value pairs to series IDs.
type TagIndex struct {
	db    *badger.DB
	cache sync.Map // string -> *roaring64.Bitmap
}

func newTagIndex(db *badger.DB) *TagIndex {
	return &TagIndex{db: db}
}

// Index adds a series to the index for all its tags.
func (idx *TagIndex) Index(metric string, tags Tagset, seriesID SeriesID) error {
	idx.indexTag(metric, uint64(seriesID))

	for _, tag := range tags {
		key := formatTagKey(metric, tag.Key, tag.Value)
		idx.indexTag(key, uint64(seriesID))
	}

	return idx.persist(metric, tags)
}

func (idx *TagIndex) indexTag(key string, seriesID uint64) {
	val, _ := idx.cache.LoadOrStore(key, roaring64.New())
	bm := val.(*roaring64.Bitmap)
	bm.Add(seriesID)
}

func (idx *TagIndex) persist(metric string, tags Tagset) error {
	return idx.db.Update(func(txn *badger.Txn) error {
		if err := idx.persistKey(txn, metric); err != nil {
			return err
		}
		for _, tag := range tags {
			key := formatTagKey(metric, tag.Key, tag.Value)
			if err := idx.persistKey(txn, key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (idx *TagIndex) persistKey(txn *badger.Txn, key string) error {
	val, ok := idx.cache.Load(key)
	if !ok {
		return nil
	}
	bm := val.(*roaring64.Bitmap)

	data, err := bm.ToBytes()
	if err != nil {
		return err
	}

	indexKey := make([]byte, 1+len(key))
	indexKey[0] = PrefixIndex
	copy(indexKey[1:], key)

	return txn.Set(indexKey, data)
}

// GetSeriesIDs returns all series IDs matching a metric and tag:value.
func (idx *TagIndex) GetSeriesIDs(metric, tagKey, tagValue string) (*roaring64.Bitmap, error) {
	key := formatTagKey(metric, tagKey, tagValue)
	return idx.getBitmap(key)
}

// GetAllSeriesIDs returns all series IDs for a metric.
func (idx *TagIndex) GetAllSeriesIDs(metric string) (*roaring64.Bitmap, error) {
	return idx.getBitmap(metric)
}

func (idx *TagIndex) getBitmap(key string) (*roaring64.Bitmap, error) {
	if val, ok := idx.cache.Load(key); ok {
		return val.(*roaring64.Bitmap), nil
	}

	indexKey := make([]byte, 1+len(key))
	indexKey[0] = PrefixIndex
	copy(indexKey[1:], key)

	var bm *roaring64.Bitmap
	err := idx.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(indexKey)
		if err == badger.ErrKeyNotFound {
			bm = roaring64.New()
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			bm = roaring64.New()
			_, err := bm.ReadFrom(bytes.NewReader(val))
			return err
		})
	})
	if err != nil {
		return nil, err
	}

	idx.cache.Store(key, bm)
	return bm, nil
}

func formatTagKey(metric, tagKey, tagValue string) string {
	if tagKey == "" {
		return metric
	}
	return metric + "#" + tagKey + ":" + tagValue
}

// Intersect returns the intersection of multiple bitmaps.
func Intersect(bitmaps ...*roaring64.Bitmap) *roaring64.Bitmap {
	if len(bitmaps) == 0 {
		return roaring64.New()
	}
	if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}
	result := bitmaps[0].Clone()
	for _, bm := range bitmaps[1:] {
		result.And(bm)
	}
	return result
}

// Union returns the union of multiple bitmaps.
func Union(bitmaps ...*roaring64.Bitmap) *roaring64.Bitmap {
	if len(bitmaps) == 0 {
		return roaring64.New()
	}
	if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}
	result := bitmaps[0].Clone()
	for _, bm := range bitmaps[1:] {
		result.Or(bm)
	}
	return result
}
