package ktsdb

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// Database is the main entry point for ktsdb.
type Database struct {
	db     *badger.DB
	path   string
	closed bool
	mu     sync.RWMutex

	series        *SeriesRegistry
	index         *TagIndex
	dataKeyPool   sync.Pool
	dataValuePool sync.Pool
}

// Options configures a Database instance.
type Options struct {
	// Path is the directory where the database files will be stored.
	Path string

	// InMemory, if true, runs Badger in memory-only mode (no persistence).
	// Useful for testing.
	InMemory bool

	// SyncWrites, if true, syncs writes to disk immediately.
	// Slower but safer. Default is false (async writes).
	SyncWrites bool

	// Logger is used for Badger's internal logging.
	// If nil, logging is disabled.
	Logger badger.Logger
}

func DefaultOptions(path string) Options {
	return Options{
		Path: path,
	}
}

// Open creates or opens a Database at the given path.
func Open(opts Options) (*Database, error) {
	badgerOpts := badger.DefaultOptions(opts.Path)

	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}

	badgerOpts = badgerOpts.WithSyncWrites(opts.SyncWrites)

	badgerOpts = badgerOpts.WithLogger(opts.Logger)

	badgerOpts = badgerOpts.
		WithNumMemtables(4).
		WithValueLogFileSize(256 << 20).
		WithCompression(options.Snappy)

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	d := &Database{
		db:   db,
		path: opts.Path,
		dataKeyPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, DataKeySize)
				return &buf
			},
		},
		dataValuePool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 8)
				return &buf
			},
		},
	}
	d.series = newSeriesRegistry(db)
	d.index = newTagIndex(db)
	return d, nil
}

// Close closes the database, releasing all resources.
func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true
	return d.db.Close()
}

// Path returns the filesystem path of the database.
func (d *Database) Path() string {
	return d.path
}

func (d *Database) getDataKeyBuf() *[]byte {
	return d.dataKeyPool.Get().(*[]byte)
}

func (d *Database) putDataKeyBuf(buf *[]byte) {
	d.dataKeyPool.Put(buf)
}

func (d *Database) getDataValueBuf() *[]byte {
	return d.dataValuePool.Get().(*[]byte)
}

func (d *Database) putDataValueBuf(buf *[]byte) {
	d.dataValuePool.Put(buf)
}

func (d *Database) Badger() *badger.DB {
	return d.db
}

func (d *Database) Series() *SeriesRegistry {
	return d.series
}

func (d *Database) Index() *TagIndex {
	return d.index
}
