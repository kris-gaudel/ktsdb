package ktsdb

import (
	"fmt"
	"testing"
)

func TestQuery(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*Database)
		filter     string
		start, end int64
		limit      int
		wantSeries int
		wantPoints int
	}{
		{
			name: "no filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 1.0, map[string]string{"host": "h1"}, 1000)
				db.WriteAt("cpu", 2.0, map[string]string{"host": "h2"}, 2000)
				db.WriteAt("cpu", 3.0, map[string]string{"host": "h3"}, 3000)
			},
			wantSeries: 3,
			wantPoints: 3,
		},
		{
			name: "tag filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 1.0, map[string]string{"env": "prod", "host": "h1"}, 1000)
				db.WriteAt("cpu", 2.0, map[string]string{"env": "prod", "host": "h2"}, 2000)
				db.WriteAt("cpu", 3.0, map[string]string{"env": "dev", "host": "h3"}, 3000)
			},
			filter:     "env:prod",
			wantSeries: 2,
			wantPoints: 2,
		},
		{
			name: "and filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 1.0, map[string]string{"env": "prod", "service": "api"}, 1000)
				db.WriteAt("cpu", 2.0, map[string]string{"env": "prod", "service": "db"}, 2000)
				db.WriteAt("cpu", 3.0, map[string]string{"env": "dev", "service": "api"}, 3000)
			},
			filter:     "env:prod AND service:api",
			wantSeries: 1,
			wantPoints: 1,
		},
		{
			name: "or filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 1.0, map[string]string{"env": "prod"}, 1000)
				db.WriteAt("cpu", 2.0, map[string]string{"env": "dev"}, 2000)
				db.WriteAt("cpu", 3.0, map[string]string{"env": "staging"}, 3000)
			},
			filter:     "env:prod OR env:dev",
			wantSeries: 2,
			wantPoints: 2,
		},
		{
			name: "complex filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 1.0, map[string]string{"env": "prod", "region": "us"}, 1000)
				db.WriteAt("cpu", 2.0, map[string]string{"env": "prod", "region": "eu"}, 2000)
				db.WriteAt("cpu", 3.0, map[string]string{"env": "dev", "region": "us"}, 3000)
				db.WriteAt("cpu", 4.0, map[string]string{"env": "dev", "region": "eu"}, 4000)
			},
			filter:     "(env:prod AND region:us) OR (env:dev AND region:eu)",
			wantSeries: 2,
			wantPoints: 2,
		},
		{
			name: "time range",
			setup: func(db *Database) {
				for i := int64(1); i <= 10; i++ {
					db.WriteAt("cpu", float64(i), map[string]string{"host": "h1"}, i*1000)
				}
			},
			start:      3000,
			end:        7000,
			wantSeries: 1,
			wantPoints: 5,
		},
		{
			name: "with limit",
			setup: func(db *Database) {
				for i := int64(1); i <= 100; i++ {
					db.WriteAt("cpu", float64(i), map[string]string{"host": "h1"}, i*1000)
				}
			},
			limit:      10,
			wantSeries: 1,
			wantPoints: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tt.setup(db)

			q := db.NewQuery("cpu")

			if tt.filter != "" {
				var err error
				q, err = q.Where(tt.filter)
				if err != nil {
					t.Fatalf("where failed: %v", err)
				}
			}

			if tt.start > 0 || tt.end > 0 {
				q.TimeRange(tt.start, tt.end)
			}

			if tt.limit > 0 {
				q.Limit(tt.limit)
			}

			results, err := q.Execute()
			if err != nil {
				t.Fatalf("execute failed: %v", err)
			}

			if len(results) != tt.wantSeries {
				t.Errorf("got %d series, want %d", len(results), tt.wantSeries)
			}

			totalPoints := 0
			for _, points := range results {
				totalPoints += len(points)
			}
			if totalPoints != tt.wantPoints {
				t.Errorf("got %d points, want %d", totalPoints, tt.wantPoints)
			}
		})
	}
}

func TestQueryExecuteRaw(t *testing.T) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	db.WriteAt("cpu", 1.0, map[string]string{"env": "prod"}, 1000)
	db.WriteAt("cpu", 2.0, map[string]string{"env": "prod"}, 2000)
	db.WriteAt("cpu", 3.0, map[string]string{"env": "dev"}, 3000)

	q, _ := db.NewQuery("cpu").Where("env:prod")
	bm, err := q.ExecuteRaw()
	if err != nil {
		t.Fatalf("ExecuteRaw failed: %v", err)
	}

	if bm.GetCardinality() != 1 {
		t.Errorf("expected 1 series ID, got %d", bm.GetCardinality())
	}
}

func BenchmarkQueryExecution(b *testing.B) {
	configs := []struct {
		name   string
		series int
		points int
		filter string
	}{
		{"no_filter_10s", 10, 100, ""},
		{"no_filter_100s", 100, 100, ""},
		{"with_filter_100s", 100, 100, "env:prod"},
		{"complex_filter", 100, 100, "(env:prod OR env:staging) AND region:us"},
	}

	for _, cfg := range configs {
		b.Run(cfg.name, func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			for i := 0; i < cfg.series; i++ {
				env := "prod"
				if i%2 == 0 {
					env = "dev"
				}
				region := "us"
				if i%3 == 0 {
					region = "eu"
				}
				for j := int64(0); j < int64(cfg.points); j++ {
					db.WriteAt("cpu", float64(j), map[string]string{
						"env":    env,
						"region": region,
						"host":   fmt.Sprintf("h%d", i),
					}, j)
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				q := db.NewQuery("cpu")
				if cfg.filter != "" {
					q, _ = q.Where(cfg.filter)
				}
				q.Execute()
			}
		})
	}
}
