package ktsdb

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Write writes a single data point to the database.
// Tags are sorted in-place for consistent series ID computation.
func (d *Database) Write(metric string, value float64, tags map[string]string) error {
	return d.WriteAt(metric, value, tags, time.Now().UnixNano())
}

// WriteAt writes a data point with a specific timestamp (nanoseconds).
func (d *Database) WriteAt(metric string, value float64, tags map[string]string, timestamp int64) error {
	return d.WriteAtWithTagset(metric, value, FromMap(tags), timestamp)
}

// WriteAtWithTagset writes a data point using a pre-sorted Tagset.
// This is faster than WriteAt when the tagset is reused across many writes.
func (d *Database) WriteAtWithTagset(metric string, value float64, tagset Tagset, timestamp int64) error {
	id, created, err := d.series.GetOrCreate(metric, tagset)
	if err != nil {
		return err
	}

	if created {
		if err := d.index.Index(metric, tagset, id); err != nil {
			return err
		}
	}

	keyBuf := d.getDataKeyBuf()
	valueBuf := d.getDataValueBuf()
	defer d.putDataKeyBuf(keyBuf)
	defer d.putDataValueBuf(valueBuf)

	EncodeDataKey(*keyBuf, uint64(id), timestamp)
	EncodeDataValue(*valueBuf, value)

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(*keyBuf, *valueBuf)
	})
}

// BatchWriter accumulates writes and flushes them in batches.
type BatchWriter struct {
	db    *Database
	batch *badger.WriteBatch
}

// NewBatchWriter creates a new batch writer.
// Call Flush() when done, or Cancel() to abort.
func (d *Database) NewBatchWriter() *BatchWriter {
	return &BatchWriter{
		db:    d,
		batch: d.db.NewWriteBatch(),
	}
}

// Write adds a data point to the batch.
func (w *BatchWriter) Write(metric string, value float64, tags map[string]string) error {
	return w.WriteAt(metric, value, tags, time.Now().UnixNano())
}

// WriteAt adds a data point with a specific timestamp to the batch.
// Note: Cannot use pooled buffers here because WriteBatch keeps references.
func (w *BatchWriter) WriteAt(metric string, value float64, tags map[string]string, timestamp int64) error {
	return w.WriteAtWithTagset(metric, value, FromMap(tags), timestamp)
}

// WriteAtWithTagset adds a data point using a pre-sorted Tagset.
func (w *BatchWriter) WriteAtWithTagset(metric string, value float64, tagset Tagset, timestamp int64) error {
	id, created, err := w.db.series.GetOrCreate(metric, tagset)
	if err != nil {
		return err
	}

	if created {
		if err := w.db.index.Index(metric, tagset, id); err != nil {
			return err
		}
	}

	keyBuf := make([]byte, DataKeySize)
	valueBuf := make([]byte, 8)

	EncodeDataKey(keyBuf, uint64(id), timestamp)
	EncodeDataValue(valueBuf, value)

	return w.batch.Set(keyBuf, valueBuf)
}

// WriteRaw writes directly with a known series ID (fastest path).
func (w *BatchWriter) WriteRaw(seriesID SeriesID, value float64, timestamp int64) error {
	keyBuf := make([]byte, DataKeySize)
	valueBuf := make([]byte, 8)

	EncodeDataKey(keyBuf, uint64(seriesID), timestamp)
	EncodeDataValue(valueBuf, value)

	return w.batch.Set(keyBuf, valueBuf)
}

// Flush commits all pending writes to the database.
func (w *BatchWriter) Flush() error {
	return w.batch.Flush()
}

// Cancel aborts the batch without committing.
func (w *BatchWriter) Cancel() {
	w.batch.Cancel()
}
