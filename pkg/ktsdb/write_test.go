package ktsdb

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestWrite(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	tags := map[string]string{"env": "prod", "host": "h1"}

	err = db.WriteAt("cpu.total", 42.5, tags, 1000)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	err = db.WriteAt("cpu.total", 43.5, tags, 2000)
	if err != nil {
		t.Fatalf("second Write failed: %v", err)
	}

	count := 0
	err = db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{PrefixData}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	if count != 2 {
		t.Errorf("data point count = %d, want 2", count)
	}
}

func TestWriteDecodeRoundtrip(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	tags := map[string]string{"env": "prod"}
	timestamp := int64(1703635200000000000)
	value := 99.9

	err = db.WriteAt("metric", value, tags, timestamp)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	tagset := FromMap(tags)
	id, _, _ := db.Series().GetOrCreate("metric", tagset)

	var gotTimestamp int64
	var gotValue float64

	err = db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		keyBuf := make([]byte, DataKeySize)
		DataKeyPrefix(keyBuf, uint64(id))

		for it.Seek(keyBuf); it.ValidForPrefix(keyBuf[:1+SeriesIDSize]); it.Next() {
			item := it.Item()
			key := item.Key()
			_, gotTimestamp = DecodeDataKey(key)

			err := item.Value(func(val []byte) error {
				gotValue = DecodeDataValue(val)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if gotTimestamp != timestamp {
		t.Errorf("timestamp = %d, want %d", gotTimestamp, timestamp)
	}
	if gotValue != value {
		t.Errorf("value = %f, want %f", gotValue, value)
	}
}

func TestBatchWriter(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	batch := db.NewBatchWriter()

	for i := 0; i < 100; i++ {
		err := batch.WriteAt("cpu.total", float64(i), map[string]string{"host": "h1"}, int64(i*1000))
		if err != nil {
			t.Fatalf("batch write %d failed: %v", i, err)
		}
	}

	if err := batch.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	count := 0
	err = db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{PrefixData}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	if count != 100 {
		t.Errorf("data point count = %d, want 100", count)
	}
}

func TestBatchWriterCancel(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	batch := db.NewBatchWriter()

	for i := 0; i < 10; i++ {
		batch.WriteAt("cpu.total", float64(i), map[string]string{"host": "h1"}, int64(i))
	}

	batch.Cancel()

	count := 0
	db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{PrefixData}
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	if count != 0 {
		t.Errorf("cancelled batch should write 0 points, got %d", count)
	}
}
