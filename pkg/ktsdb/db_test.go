package ktsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestOpenClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ktsdb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "testdb")
	db, err := Open(DefaultOptions(dbPath))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	if db.Path() != dbPath {
		t.Errorf("Path() = %q, want %q", db.Path(), dbPath)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("second Close() failed: %v", err)
	}
}

func TestOpenInMemory(t *testing.T) {
	opts := Options{
		Path:     "",
		InMemory: true,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open in-memory database: %v", err)
	}
	defer db.Close()

	if db.Badger() == nil {
		t.Error("Badger() returned nil")
	}
}

func TestBufferPools(t *testing.T) {
	opts := Options{
		Path:     "",
		InMemory: true,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	t.Run("data key buffer", func(t *testing.T) {
		buf := db.getDataKeyBuf()
		if buf == nil {
			t.Fatal("getDataKeyBuf returned nil")
		}
		if len(*buf) != DataKeySize {
			t.Errorf("buffer size = %d, want %d", len(*buf), DataKeySize)
		}
		db.putDataKeyBuf(buf)

		buf2 := db.getDataKeyBuf()
		if buf2 == nil {
			t.Fatal("second getDataKeyBuf returned nil")
		}
		db.putDataKeyBuf(buf2)
	})

	t.Run("data value buffer", func(t *testing.T) {
		buf := db.getDataValueBuf()
		if buf == nil {
			t.Fatal("getDataValueBuf returned nil")
		}
		if len(*buf) != 8 {
			t.Errorf("buffer size = %d, want 8", len(*buf))
		}
		db.putDataValueBuf(buf)
	})
}

func TestReopenDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "ktsdb-reopen-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "testdb")
	db1, err := Open(DefaultOptions(dbPath))
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	testKey := []byte("test-key")
	testValue := []byte("test-value")
	err = db1.Badger().Update(func(txn *badger.Txn) error {
		return txn.Set(testKey, testValue)
	})
	if err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}

	db2, err := Open(DefaultOptions(dbPath))
	if err != nil {
		t.Fatalf("failed to reopen database: %v", err)
	}
	defer db2.Close()

	err = db2.Badger().View(func(txn *badger.Txn) error {
		item, err := txn.Get(testKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if string(val) != string(testValue) {
				t.Errorf("value = %q, want %q", val, testValue)
			}
			return nil
		})
	})
	if err != nil {
		t.Errorf("failed to read test data: %v", err)
	}
}
