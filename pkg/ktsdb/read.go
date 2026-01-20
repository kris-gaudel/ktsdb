package ktsdb

import (
	"bytes"

	"github.com/dgraph-io/badger/v4"
)

// DataPoint represents a single time series data point.
type DataPoint struct {
	Timestamp int64
	Value     float64
}

// QueryOptions configures a time-range query.
type QueryOptions struct {
	Start int64 // Start timestamp (inclusive), 0 means no lower bound
	End   int64 // End timestamp (inclusive), 0 means no upper bound
	Limit int   // Maximum number of points to return, 0 means no limit
}

// Query retrieves data points for a series within a time range.
// Points are returned newest-first (descending timestamp order).
func (d *Database) Query(seriesID SeriesID, opts QueryOptions) ([]DataPoint, error) {
	var points []DataPoint

	prefix := make([]byte, 1+SeriesIDSize)
	DataKeyPrefix(prefix, uint64(seriesID))

	err := d.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.Prefix = prefix

		it := txn.NewIterator(iterOpts)
		defer it.Close()

		seekKey := make([]byte, DataKeySize)
		if opts.End > 0 {
			EncodeDataKey(seekKey, uint64(seriesID), opts.End)
		} else {
			copy(seekKey, prefix)
		}

		for it.Seek(seekKey); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			_, ts := DecodeDataKey(key)

			if opts.Start > 0 && ts < opts.Start {
				break
			}

			if opts.End > 0 && ts > opts.End {
				continue
			}

			var value float64
			err := item.Value(func(val []byte) error {
				value = DecodeDataValue(val)
				return nil
			})
			if err != nil {
				return err
			}

			points = append(points, DataPoint{Timestamp: ts, Value: value})

			if opts.Limit > 0 && len(points) >= opts.Limit {
				break
			}
		}
		return nil
	})

	return points, err
}

// QueryByMetric retrieves data points for all series matching a metric name.
func (d *Database) QueryByMetric(metric string, opts QueryOptions) (map[SeriesID][]DataPoint, error) {
	bm, err := d.index.GetAllSeriesIDs(metric)
	if err != nil {
		return nil, err
	}

	results := make(map[SeriesID][]DataPoint)
	iter := bm.Iterator()

	for iter.HasNext() {
		sid := SeriesID(iter.Next())
		points, err := d.Query(sid, opts)
		if err != nil {
			return nil, err
		}
		if len(points) > 0 {
			results[sid] = points
		}
	}

	return results, nil
}

// Iterator provides streaming access to data points.
type Iterator struct {
	db       *Database
	seriesID SeriesID
	opts     QueryOptions
	txn      *badger.Txn
	it       *badger.Iterator
	prefix   []byte
	started  bool
	done     bool
	current  DataPoint
	err      error
}

// NewIterator creates a streaming iterator for a series.
func (d *Database) NewIterator(seriesID SeriesID, opts QueryOptions) *Iterator {
	prefix := make([]byte, 1+SeriesIDSize)
	DataKeyPrefix(prefix, uint64(seriesID))

	txn := d.db.NewTransaction(false)

	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Prefix = prefix

	return &Iterator{
		db:       d,
		seriesID: seriesID,
		opts:     opts,
		txn:      txn,
		it:       txn.NewIterator(iterOpts),
		prefix:   prefix,
	}
}

// Next advances the iterator and returns true if there's a valid point.
func (iter *Iterator) Next() bool {
	if iter.done || iter.err != nil {
		return false
	}

	if !iter.started {
		iter.started = true
		seekKey := make([]byte, DataKeySize)
		if iter.opts.End > 0 {
			EncodeDataKey(seekKey, uint64(iter.seriesID), iter.opts.End)
		} else {
			copy(seekKey, iter.prefix)
		}
		iter.it.Seek(seekKey)
	} else {
		iter.it.Next()
	}

	for iter.it.Valid() {
		item := iter.it.Item()
		key := item.Key()

		if !bytes.HasPrefix(key, iter.prefix) {
			iter.done = true
			return false
		}

		_, ts := DecodeDataKey(key)

		if iter.opts.Start > 0 && ts < iter.opts.Start {
			iter.done = true
			return false
		}

		if iter.opts.End > 0 && ts > iter.opts.End {
			iter.it.Next()
			continue
		}

		var value float64
		iter.err = item.Value(func(val []byte) error {
			value = DecodeDataValue(val)
			return nil
		})
		if iter.err != nil {
			return false
		}

		iter.current = DataPoint{Timestamp: ts, Value: value}
		return true
	}

	iter.done = true
	return false
}

// Value returns the current data point.
func (iter *Iterator) Value() DataPoint {
	return iter.current
}

// Err returns any error encountered during iteration.
func (iter *Iterator) Err() error {
	return iter.err
}

// Close releases resources held by the iterator.
func (iter *Iterator) Close() {
	iter.it.Close()
	iter.txn.Discard()
}
