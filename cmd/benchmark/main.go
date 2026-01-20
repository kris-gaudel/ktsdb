package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"ktsdb/pkg/ktsdb"
)

func main() {
	path := flag.String("path", ".ktsdb-bench", "database path")
	batchSize := flag.Int("batch", 50000, "batch size")
	totalPoints := flag.Int64("points", 1_000_000_000, "total points to write")
	flag.Parse()

	os.RemoveAll(*path)

	opts := ktsdb.DefaultOptions(*path)
	db, err := ktsdb.Open(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open db: %v\n", err)
		os.Exit(1)
	}

	r := rand.New(rand.NewSource(42))

	hosts := []string{"h-0", "h-1", "h-2", "h-3", "h-4", "h-5", "h-6", "h-7", "h-8", "h-9"}
	pointsPerHost := *totalPoints / int64(len(hosts))

	// Pre-create tagsets and register series (like Talna)
	tagsets := make([]ktsdb.Tagset, len(hosts))
	seriesIDs := make([]ktsdb.SeriesID, len(hosts))

	for i, host := range hosts {
		tagsets[i] = ktsdb.Tagset{
			{Key: "env", Value: "prod"},
			{Key: "host", Value: host},
			{Key: "service", Value: "db"},
		}
		tagsets[i].Sort()
		seriesIDs[i], _, _ = db.Series().GetOrCreate("cpu.total", tagsets[i])
	}

	// Index all series upfront
	for i, ts := range tagsets {
		db.Index().Index("cpu.total", ts, seriesIDs[i])
	}

	var peakMem uint64
	updatePeakMem := func() {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		if m.HeapAlloc > peakMem {
			peakMem = m.HeapAlloc
		}
	}

	start := time.Now()
	var totalWritten int64

	printInterval := pointsPerHost / 10
	if printInterval < 1000 {
		printInterval = 1000
	}

	for hidx, host := range hosts {
		sid := seriesIDs[hidx]
		batch := db.NewBatchWriter()
		batchCount := 0

		for idx := int64(0); idx < pointsPerHost; idx++ {
			timestamp := int64(hidx)*pointsPerHost + idx
			value := r.Float64() * 100

			batch.WriteRaw(sid, value, timestamp)
			batchCount++
			totalWritten++

			if batchCount >= *batchSize {
				batch.Flush()
				batch = db.NewBatchWriter()
				batchCount = 0
			}

			if idx > 0 && idx%printInterval == 0 {
				elapsed := time.Since(start)
				rate := float64(totalWritten) / elapsed.Seconds()
				updatePeakMem()
				fmt.Printf("[%s] %d/%d - %.0f WPS - peak mem: %d MiB\n",
					host, idx, pointsPerHost, rate, peakMem/1024/1024)
			}
		}

		if batchCount > 0 {
			batch.Flush()
		}
	}

	elapsed := time.Since(start)
	rate := float64(totalWritten) / elapsed.Seconds()
	updatePeakMem()

	db.Close()

	// Disk size
	var diskSize int64
	entries, _ := os.ReadDir(*path)
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			diskSize += info.Size()
		}
	}

	fmt.Println()
	fmt.Printf("ingested %d points in %.3fs\n", totalWritten, elapsed.Seconds())
	fmt.Printf("write speed: %.0f writes per second\n", rate)
	fmt.Printf("peak mem: %d MiB\n", peakMem/1024/1024)
	fmt.Printf("disk space: %d bytes (%d MiB, %.4f GiB)\n",
		diskSize,
		diskSize/1024/1024,
		float64(diskSize)/(1024*1024*1024),
	)

	// Reopen benchmark
	reopenStart := time.Now()
	db2, _ := ktsdb.Open(opts)
	fmt.Printf("reopened DB in %dms\n", time.Since(reopenStart).Milliseconds())

	// Query benchmark
	lowerBound := totalWritten - 10000
	if lowerBound < 0 {
		lowerBound = 0
	}

	for i := 0; i < 5; i++ {
		queryStart := time.Now()
		q, _ := db2.NewQuery("cpu.total").Where("host:h-9 OR host:h-8")
		q.TimeRange(lowerBound, 0)
		results, _ := q.Execute()

		count := 0
		for _, pts := range results {
			count += len(pts)
		}
		fmt.Printf("query [%d latest data points] in %dms\n", count, time.Since(queryStart).Milliseconds())
	}

	db2.Close()
	os.RemoveAll(*path)
}
