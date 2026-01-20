package ktsdb

import (
	"testing"
)

func TestQuerySeries(t *testing.T) {
	tests := []struct {
		name       string
		writeCount int
		start, end int64
		limit      int
		wantCount  int
		wantFirst  int64
	}{
		{"all points", 5, 0, 0, 0, 5, 5000},
		{"time range", 10, 3000, 7000, 0, 5, 7000},
		{"with limit", 100, 0, 0, 10, 10, 100000},
		{"empty series", 0, 0, 0, 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1"}
			for i := int64(1); i <= int64(tt.writeCount); i++ {
				db.WriteAt("cpu", float64(i), tags, i*1000)
			}

			var seriesID SeriesID
			if tt.writeCount > 0 {
				seriesID, _, _ = db.Series().GetOrCreate("cpu", FromMap(tags))
			}

			points, err := db.Query(seriesID, QueryOptions{
				Start: tt.start,
				End:   tt.end,
				Limit: tt.limit,
			})

			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(points) != tt.wantCount {
				t.Errorf("got %d points, want %d", len(points), tt.wantCount)
			}

			if tt.wantCount > 0 && points[0].Timestamp != tt.wantFirst {
				t.Errorf("first timestamp = %d, want %d", points[0].Timestamp, tt.wantFirst)
			}
		})
	}
}

func TestQueryByMetric(t *testing.T) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	db.WriteAt("cpu", 1.0, map[string]string{"host": "h1"}, 1000)
	db.WriteAt("cpu", 2.0, map[string]string{"host": "h2"}, 2000)
	db.WriteAt("cpu", 3.0, map[string]string{"host": "h3"}, 3000)

	results, err := db.QueryByMetric("cpu", QueryOptions{})
	if err != nil {
		t.Fatalf("QueryByMetric failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 series, got %d", len(results))
	}

	totalPoints := 0
	for _, points := range results {
		totalPoints += len(points)
	}
	if totalPoints != 3 {
		t.Errorf("expected 3 total points, got %d", totalPoints)
	}
}

func TestIterator(t *testing.T) {
	tests := []struct {
		name       string
		writeCount int
		start, end int64
		wantCount  int
	}{
		{"all points", 5, 0, 0, 5},
		{"time range", 10, 2000, 4000, 3},
		{"empty", 0, 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1"}
			for i := int64(1); i <= int64(tt.writeCount); i++ {
				db.WriteAt("cpu", float64(i), tags, i*1000)
			}

			var seriesID SeriesID
			if tt.writeCount > 0 {
				seriesID, _, _ = db.Series().GetOrCreate("cpu", FromMap(tags))
			}

			iter := db.NewIterator(seriesID, QueryOptions{Start: tt.start, End: tt.end})
			defer iter.Close()

			count := 0
			for iter.Next() {
				count++
				_ = iter.Value()
			}

			if iter.Err() != nil {
				t.Fatalf("Iterator error: %v", iter.Err())
			}

			if count != tt.wantCount {
				t.Errorf("got %d points, want %d", count, tt.wantCount)
			}
		})
	}
}

func TestQueryNonExistentSeries(t *testing.T) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	points, err := db.Query(SeriesID(12345), QueryOptions{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(points) != 0 {
		t.Errorf("expected 0 points, got %d", len(points))
	}
}

func BenchmarkQuery(b *testing.B) {
	sizes := []struct {
		name   string
		points int
	}{
		{"100", 100},
		{"1000", 1000},
		{"10000", 10000},
	}

	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1"}
			for i := int64(0); i < int64(size.points); i++ {
				db.WriteAt("cpu", float64(i), tags, i)
			}
			seriesID, _, _ := db.Series().GetOrCreate("cpu", FromMap(tags))

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				db.Query(seriesID, QueryOptions{
					Start: int64(size.points / 4),
					End:   int64(size.points * 3 / 4),
				})
			}
		})
	}
}

func BenchmarkIterator(b *testing.B) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	tags := map[string]string{"host": "h1"}
	for i := int64(0); i < 10000; i++ {
		db.WriteAt("cpu", float64(i), tags, i)
	}
	seriesID, _, _ := db.Series().GetOrCreate("cpu", FromMap(tags))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iter := db.NewIterator(seriesID, QueryOptions{Start: 2500, End: 7500})
		for iter.Next() {
			_ = iter.Value()
		}
		iter.Close()
	}
}
