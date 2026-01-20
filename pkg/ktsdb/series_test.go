package ktsdb

import (
	"testing"
)

func TestComputeSeriesID(t *testing.T) {
	tags := Tagset{
		{Key: "env", Value: "prod"},
		{Key: "host", Value: "h1"},
	}

	id1 := ComputeSeriesID("cpu.total", tags)
	id2 := ComputeSeriesID("cpu.total", tags)

	if id1 != id2 {
		t.Errorf("same input should produce same ID: %d != %d", id1, id2)
	}

	id3 := ComputeSeriesID("cpu.user", tags)
	if id1 == id3 {
		t.Error("different metrics should produce different IDs")
	}

	tags2 := Tagset{
		{Key: "env", Value: "staging"},
		{Key: "host", Value: "h1"},
	}
	id4 := ComputeSeriesID("cpu.total", tags2)
	if id1 == id4 {
		t.Error("different tags should produce different IDs")
	}
}

func TestComputeSeriesIDTagOrder(t *testing.T) {
	tags1 := Tagset{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
	}
	tags2 := Tagset{
		{Key: "b", Value: "2"},
		{Key: "a", Value: "1"},
	}

	id1 := ComputeSeriesID("metric", tags1)
	id2 := ComputeSeriesID("metric", tags2)

	if id1 == id2 {
		t.Error("unsorted tags produce same ID - caller must sort first!")
	}

	tags2.Sort()
	id3 := ComputeSeriesID("metric", tags2)
	if id1 != id3 {
		t.Error("sorted tags should produce same ID")
	}
}

func TestSeriesRegistry(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	reg := db.Series()
	tags := FromMap(map[string]string{"env": "prod", "host": "h1"})

	id1, created1, err := reg.GetOrCreate("cpu.total", tags)
	if err != nil {
		t.Fatalf("GetOrCreate failed: %v", err)
	}
	if !created1 {
		t.Error("first call should create")
	}

	id2, created2, err := reg.GetOrCreate("cpu.total", tags)
	if err != nil {
		t.Fatalf("second GetOrCreate failed: %v", err)
	}
	if created2 {
		t.Error("second call should not create")
	}
	if id1 != id2 {
		t.Errorf("IDs should match: %d != %d", id1, id2)
	}

	meta, err := reg.Get(id1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if meta.Metric != "cpu.total" {
		t.Errorf("metric = %q, want cpu.total", meta.Metric)
	}
	if len(meta.Tags) != 2 {
		t.Errorf("tags len = %d, want 2", len(meta.Tags))
	}
}

func TestSeriesRegistryExists(t *testing.T) {
	db, err := Open(Options{InMemory: true})
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	reg := db.Series()
	tags := FromMap(map[string]string{"env": "prod"})

	if reg.Exists(12345) {
		t.Error("non-existent series should not exist")
	}

	id, _, _ := reg.GetOrCreate("metric", tags)

	if !reg.Exists(id) {
		t.Error("created series should exist")
	}
}

func TestSeriesRegistryPersistence(t *testing.T) {
	tmpDir := t.TempDir()

	tags := FromMap(map[string]string{"env": "prod"})
	var originalID SeriesID

	{
		db, err := Open(DefaultOptions(tmpDir))
		if err != nil {
			t.Fatalf("failed to open db: %v", err)
		}
		originalID, _, _ = db.Series().GetOrCreate("cpu.total", tags)
		db.Close()
	}

	{
		db, err := Open(DefaultOptions(tmpDir))
		if err != nil {
			t.Fatalf("failed to reopen db: %v", err)
		}
		defer db.Close()

		id, created, _ := db.Series().GetOrCreate("cpu.total", tags)
		if created {
			t.Error("should not create on reopen")
		}
		if id != originalID {
			t.Errorf("ID mismatch after reopen: %d != %d", id, originalID)
		}

		meta, err := db.Series().Get(id)
		if err != nil {
			t.Fatalf("Get failed after reopen: %v", err)
		}
		if meta.Metric != "cpu.total" {
			t.Errorf("metric = %q after reopen", meta.Metric)
		}
	}
}

func BenchmarkComputeSeriesID(b *testing.B) {
	tags := Tagset{
		{Key: "env", Value: "prod"},
		{Key: "host", Value: "host-001"},
		{Key: "service", Value: "api-gateway"},
		{Key: "region", Value: "us-east-1"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ComputeSeriesID("cpu.total.user", tags)
	}
}

func BenchmarkSeriesRegistryGetOrCreate(b *testing.B) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	reg := db.Series()
	tags := FromMap(map[string]string{
		"env":     "prod",
		"host":    "h1",
		"service": "api",
	})

	reg.GetOrCreate("cpu.total", tags)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reg.GetOrCreate("cpu.total", tags)
	}
}
