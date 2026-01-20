package ktsdb

import (
	"testing"
)

func TestAggregate(t *testing.T) {
	points := []DataPoint{
		{Timestamp: 1000, Value: 10},
		{Timestamp: 1500, Value: 20},
		{Timestamp: 2000, Value: 30},
		{Timestamp: 2500, Value: 40},
	}

	tests := []struct {
		name       string
		fn         AggregateFunc
		bucketSize int64
		wantLen    int
		wantFirst  float64
		wantSecond float64
	}{
		{"avg", AggAvg, 2000, 2, 15, 35},
		{"sum", AggSum, 2000, 2, 30, 70},
		{"min", AggMin, 2000, 2, 10, 30},
		{"max", AggMax, 2000, 2, 20, 40},
		{"count", AggCount, 2000, 2, 2, 2},
		{"single bucket", AggAvg, 5000, 1, 25, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := Aggregate(points, AggregateOptions{
				Func:       tt.fn,
				BucketSize: tt.bucketSize,
			})

			if len(buckets) != tt.wantLen {
				t.Fatalf("got %d buckets, want %d", len(buckets), tt.wantLen)
			}

			if buckets[0].Value != tt.wantFirst {
				t.Errorf("bucket 0: got %f, want %f", buckets[0].Value, tt.wantFirst)
			}

			if tt.wantLen > 1 && buckets[1].Value != tt.wantSecond {
				t.Errorf("bucket 1: got %f, want %f", buckets[1].Value, tt.wantSecond)
			}
		})
	}
}

func TestAggregateEdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		points     []DataPoint
		bucketSize int64
		wantNil    bool
	}{
		{"nil points", nil, 1000, true},
		{"empty points", []DataPoint{}, 1000, true},
		{"zero bucket size", []DataPoint{{Timestamp: 1, Value: 1}}, 0, true},
		{"negative bucket size", []DataPoint{{Timestamp: 1, Value: 1}}, -1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := Aggregate(tt.points, AggregateOptions{
				Func:       AggAvg,
				BucketSize: tt.bucketSize,
			})

			if tt.wantNil && buckets != nil {
				t.Errorf("expected nil, got %v", buckets)
			}
		})
	}
}

func TestAggregateQuery(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(*Database)
		fn          AggregateFunc
		bucketSize  int64
		groupBy     []string
		filter      string
		wantGroups  int
		wantBuckets int
	}{
		{
			name: "no groupby",
			setup: func(db *Database) {
				db.WriteAt("cpu", 10.0, map[string]string{"host": "h1"}, 1000)
				db.WriteAt("cpu", 20.0, map[string]string{"host": "h1"}, 1500)
				db.WriteAt("cpu", 30.0, map[string]string{"host": "h2"}, 2000)
				db.WriteAt("cpu", 40.0, map[string]string{"host": "h2"}, 2500)
			},
			fn:          AggAvg,
			bucketSize:  2000,
			wantGroups:  1,
			wantBuckets: 2,
		},
		{
			name: "with groupby",
			setup: func(db *Database) {
				db.WriteAt("cpu", 10.0, map[string]string{"env": "prod"}, 1000)
				db.WriteAt("cpu", 20.0, map[string]string{"env": "prod"}, 2000)
				db.WriteAt("cpu", 30.0, map[string]string{"env": "dev"}, 1000)
				db.WriteAt("cpu", 40.0, map[string]string{"env": "dev"}, 2000)
			},
			fn:          AggAvg,
			bucketSize:  3000,
			groupBy:     []string{"env"},
			wantGroups:  2,
			wantBuckets: 1,
		},
		{
			name: "with filter",
			setup: func(db *Database) {
				db.WriteAt("cpu", 10.0, map[string]string{"env": "prod"}, 1000)
				db.WriteAt("cpu", 20.0, map[string]string{"env": "prod"}, 2000)
				db.WriteAt("cpu", 100.0, map[string]string{"env": "dev"}, 1000)
			},
			fn:          AggSum,
			bucketSize:  3000,
			filter:      "env:prod",
			wantGroups:  1,
			wantBuckets: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tt.setup(db)

			aq := db.NewAggregateQuery("cpu").BucketSize(tt.bucketSize)

			switch tt.fn {
			case AggAvg:
				aq.Avg()
			case AggSum:
				aq.Sum()
			case AggMin:
				aq.Min()
			case AggMax:
				aq.Max()
			case AggCount:
				aq.Count()
			}

			if len(tt.groupBy) > 0 {
				aq.GroupBy(tt.groupBy...)
			}

			if tt.filter != "" {
				aq.Where(tt.filter)
			}

			results, err := aq.Execute()
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}

			if len(results) != tt.wantGroups {
				t.Errorf("got %d groups, want %d", len(results), tt.wantGroups)
			}

			if len(results) > 0 && len(results[0].Buckets) != tt.wantBuckets {
				t.Errorf("got %d buckets, want %d", len(results[0].Buckets), tt.wantBuckets)
			}
		})
	}
}
