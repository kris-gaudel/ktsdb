package ktsdb

import (
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	tests := []struct {
		name      string
		batchSize int
	}{
		{"single", 1},
		{"batch_100", 100},
		{"batch_1000", 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1", "env": "prod"}

			b.ResetTimer()
			b.ReportAllocs()

			if tt.batchSize == 1 {
				for i := 0; i < b.N; i++ {
					db.WriteAt("cpu.total", float64(i), tags, int64(i))
				}
			} else {
				for i := 0; i < b.N; i++ {
					batch := db.NewBatchWriter()
					for j := 0; j < tt.batchSize; j++ {
						batch.WriteAt("cpu.total", float64(i*tt.batchSize+j), tags, int64(i*tt.batchSize+j))
					}
					batch.Flush()
				}
			}
		})
	}
}

func BenchmarkWriteCardinality(b *testing.B) {
	cardinalities := []int{1, 10, 100, 1000}

	for _, card := range cardinalities {
		b.Run(fmt.Sprintf("series_%d", card), func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				tags := map[string]string{
					"host": fmt.Sprintf("h%d", i%card),
					"env":  "prod",
				}
				db.WriteAt("cpu.total", float64(i), tags, int64(i))
			}
		})
	}
}

func BenchmarkQuerySeries(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("points_%d", size), func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1"}
			for i := int64(0); i < int64(size); i++ {
				db.WriteAt("cpu", float64(i), tags, i)
			}
			seriesID, _, _ := db.Series().GetOrCreate("cpu", FromMap(tags))

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				db.Query(seriesID, QueryOptions{
					Start: int64(size / 4),
					End:   int64(size * 3 / 4),
				})
			}
		})
	}
}

func BenchmarkQueryWithFilter(b *testing.B) {
	filters := []struct {
		name   string
		filter string
	}{
		{"simple", "env:prod"},
		{"and", "env:prod AND host:h50"},
		{"or", "env:prod OR env:dev"},
		{"complex", "(env:prod OR env:staging) AND region:us"},
	}

	for _, f := range filters {
		b.Run(f.name, func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			for i := 0; i < 100; i++ {
				db.WriteAt("cpu", float64(i), map[string]string{
					"env":    []string{"prod", "dev", "staging"}[i%3],
					"region": []string{"us", "eu", "asia"}[i%3],
					"host":   fmt.Sprintf("h%d", i),
				}, int64(i))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				q, _ := db.NewQuery("cpu").Where(f.filter)
				q.Execute()
			}
		})
	}
}

func BenchmarkAggregate(b *testing.B) {
	funcs := []struct {
		name string
		fn   AggregateFunc
	}{
		{"avg", AggAvg},
		{"sum", AggSum},
		{"min", AggMin},
		{"max", AggMax},
		{"count", AggCount},
	}

	for _, f := range funcs {
		b.Run(f.name, func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			tags := map[string]string{"host": "h1"}
			for i := int64(0); i < 10000; i++ {
				db.WriteAt("cpu", float64(i%100), tags, i*1000)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				aq := db.NewAggregateQuery("cpu").BucketSize(60000)
				switch f.fn {
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
				aq.Execute()
			}
		})
	}
}

func BenchmarkSeriesID(b *testing.B) {
	tagCounts := []int{1, 5, 10}

	for _, count := range tagCounts {
		b.Run(fmt.Sprintf("tags_%d", count), func(b *testing.B) {
			tags := make(Tagset, count)
			for i := 0; i < count; i++ {
				tags[i] = Tag{
					Key:   fmt.Sprintf("key%d", i),
					Value: fmt.Sprintf("value%d", i),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				ComputeSeriesID("cpu.total.user", tags)
			}
		})
	}
}

func BenchmarkTagsetFromMap(b *testing.B) {
	sizes := []int{1, 5, 10}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			m := make(map[string]string, size)
			for i := 0; i < size; i++ {
				m[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				FromMap(m)
			}
		})
	}
}

func BenchmarkFilterParse(b *testing.B) {
	exprs := []struct {
		name string
		expr string
	}{
		{"simple", "env:prod"},
		{"and", "env:prod AND host:h1"},
		{"complex", "(env:prod OR env:staging) AND host:h1 AND region:us"},
	}

	for _, e := range exprs {
		b.Run(e.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ParseFilter(e.expr)
			}
		})
	}
}

func BenchmarkIndexLookup(b *testing.B) {
	cardinalities := []int{100, 1000, 10000}

	for _, card := range cardinalities {
		b.Run(fmt.Sprintf("series_%d", card), func(b *testing.B) {
			db, _ := Open(Options{InMemory: true})
			defer db.Close()

			for i := 0; i < card; i++ {
				db.WriteAt("cpu", float64(i), map[string]string{
					"env":  "prod",
					"host": fmt.Sprintf("h%d", i),
				}, int64(i))
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				db.Index().GetSeriesIDs("cpu", "env", "prod")
			}
		})
	}
}

func BenchmarkBitmapOperations(b *testing.B) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	for i := 0; i < 10000; i++ {
		db.WriteAt("cpu", float64(i), map[string]string{
			"env":  []string{"prod", "dev"}[i%2],
			"host": fmt.Sprintf("h%d", i%100),
		}, int64(i))
	}

	bm1, _ := db.Index().GetSeriesIDs("cpu", "env", "prod")
	bm2, _ := db.Index().GetSeriesIDs("cpu", "host", "h50")

	b.Run("intersect", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Intersect(bm1, bm2)
		}
	})

	b.Run("union", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Union(bm1, bm2)
		}
	})
}

func BenchmarkParallel(b *testing.B) {
	b.Run("writes", func(b *testing.B) {
		db, _ := Open(Options{InMemory: true})
		defer db.Close()

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			tags := map[string]string{"host": "h1"}
			for pb.Next() {
				db.WriteAt("cpu", float64(i), tags, int64(i))
				i++
			}
		})
	})

	b.Run("reads", func(b *testing.B) {
		db, _ := Open(Options{InMemory: true})
		defer db.Close()

		tags := map[string]string{"host": "h1"}
		for i := int64(0); i < 10000; i++ {
			db.WriteAt("cpu", float64(i), tags, i)
		}
		seriesID, _, _ := db.Series().GetOrCreate("cpu", FromMap(tags))

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				db.Query(seriesID, QueryOptions{Start: 2500, End: 7500})
			}
		})
	})

	b.Run("mixed", func(b *testing.B) {
		db, _ := Open(Options{InMemory: true})
		defer db.Close()

		tags := map[string]string{"host": "h1"}
		for i := int64(0); i < 1000; i++ {
			db.WriteAt("cpu", float64(i), tags, i)
		}
		seriesID, _, _ := db.Series().GetOrCreate("cpu", FromMap(tags))

		b.ResetTimer()
		b.ReportAllocs()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%10 == 0 {
					db.Query(seriesID, QueryOptions{})
				} else {
					db.WriteAt("cpu", float64(i), tags, int64(1000+i))
				}
				i++
			}
		})
	})
}

func BenchmarkThroughput(b *testing.B) {
	db, _ := Open(Options{InMemory: true})
	defer db.Close()

	tags := map[string]string{"host": "h1", "env": "prod"}
	r := rand.New(rand.NewSource(42))

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(25) // Approximate bytes per data point

	for i := 0; i < b.N; i++ {
		db.WriteAt("cpu.total", r.Float64()*100, tags, int64(i))
	}
}
