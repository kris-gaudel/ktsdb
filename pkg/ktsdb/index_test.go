package ktsdb

import (
	"testing"
)

func TestTagIndex(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	db.WriteAt("cpu.total", 1.0, map[string]string{"env": "prod", "host": "h1"}, 1000)
	db.WriteAt("cpu.total", 2.0, map[string]string{"env": "prod", "host": "h2"}, 2000)
	db.WriteAt("cpu.total", 3.0, map[string]string{"env": "dev", "host": "h3"}, 3000)

	bm, err := db.Index().GetAllSeriesIDs("cpu.total")
	if err != nil {
		t.Fatalf("GetAllSeriesIDs failed: %v", err)
	}
	if bm.GetCardinality() != 3 {
		t.Errorf("expected 3 series, got %d", bm.GetCardinality())
	}

	bm, err = db.Index().GetSeriesIDs("cpu.total", "env", "prod")
	if err != nil {
		t.Fatalf("GetSeriesIDs failed: %v", err)
	}
	if bm.GetCardinality() != 2 {
		t.Errorf("expected 2 series with env:prod, got %d", bm.GetCardinality())
	}

	bm, err = db.Index().GetSeriesIDs("cpu.total", "env", "dev")
	if err != nil {
		t.Fatalf("GetSeriesIDs failed: %v", err)
	}
	if bm.GetCardinality() != 1 {
		t.Errorf("expected 1 series with env:dev, got %d", bm.GetCardinality())
	}
}

func TestTagIndexIntersect(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	db.WriteAt("cpu.total", 1.0, map[string]string{"env": "prod", "service": "api"}, 1000)
	db.WriteAt("cpu.total", 2.0, map[string]string{"env": "prod", "service": "db"}, 2000)
	db.WriteAt("cpu.total", 3.0, map[string]string{"env": "dev", "service": "api"}, 3000)

	envProd, _ := db.Index().GetSeriesIDs("cpu.total", "env", "prod")
	serviceAPI, _ := db.Index().GetSeriesIDs("cpu.total", "service", "api")

	result := Intersect(envProd, serviceAPI)

	if result.GetCardinality() != 1 {
		t.Errorf("expected 1 series (env:prod AND service:api), got %d", result.GetCardinality())
	}
}

func TestTagIndexUnion(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	db.WriteAt("cpu.total", 1.0, map[string]string{"env": "prod"}, 1000)
	db.WriteAt("cpu.total", 2.0, map[string]string{"env": "dev"}, 2000)
	db.WriteAt("cpu.total", 3.0, map[string]string{"env": "staging"}, 3000)

	envProd, _ := db.Index().GetSeriesIDs("cpu.total", "env", "prod")
	envDev, _ := db.Index().GetSeriesIDs("cpu.total", "env", "dev")

	result := Union(envProd, envDev)

	if result.GetCardinality() != 2 {
		t.Errorf("expected 2 series (env:prod OR env:dev), got %d", result.GetCardinality())
	}
}

func TestTagIndexPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	{
		db, _ := Open(DefaultOptions(tmpDir))
		db.WriteAt("cpu.total", 1.0, map[string]string{"env": "prod"}, 1000)
		db.WriteAt("cpu.total", 2.0, map[string]string{"env": "dev"}, 2000)
		db.Close()
	}

	{
		db, _ := Open(DefaultOptions(tmpDir))
		defer db.Close()

		bm, err := db.Index().GetAllSeriesIDs("cpu.total")
		if err != nil {
			t.Fatalf("GetAllSeriesIDs after reopen failed: %v", err)
		}
		if bm.GetCardinality() != 2 {
			t.Errorf("expected 2 series after reopen, got %d", bm.GetCardinality())
		}
	}
}

func BenchmarkTagIndexLookup(b *testing.B) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	for i := 0; i < 1000; i++ {
		db.WriteAt("cpu.total", float64(i), map[string]string{
			"env":  "prod",
			"host": "h1",
		}, int64(i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.Index().GetSeriesIDs("cpu.total", "env", "prod")
	}
}
