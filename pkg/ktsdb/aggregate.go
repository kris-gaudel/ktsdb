package ktsdb

import (
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// AggregateFunc defines an aggregation function type.
type AggregateFunc int

const (
	AggAvg AggregateFunc = iota
	AggSum
	AggMin
	AggMax
	AggCount
)

// Bucket represents an aggregated time bucket.
type Bucket struct {
	Timestamp int64
	Value     float64
	Count     int
}

// AggregateOptions configures aggregation behavior.
type AggregateOptions struct {
	Func       AggregateFunc
	BucketSize int64 // Bucket width in nanoseconds
}

// Aggregate applies an aggregation function to data points.
func Aggregate(points []DataPoint, opts AggregateOptions) []Bucket {
	if len(points) == 0 || opts.BucketSize <= 0 {
		return nil
	}

	buckets := make(map[int64]*accumulator)

	for _, p := range points {
		key := (p.Timestamp / opts.BucketSize) * opts.BucketSize
		acc, ok := buckets[key]
		if !ok {
			acc = &accumulator{}
			buckets[key] = acc
		}
		acc.add(p.Value)
	}

	result := make([]Bucket, 0, len(buckets))
	for ts, acc := range buckets {
		result = append(result, Bucket{
			Timestamp: ts,
			Value:     acc.compute(opts.Func),
			Count:     acc.count,
		})
	}

	sortBuckets(result)
	return result
}

type accumulator struct {
	sum   float64
	min   float64
	max   float64
	count int
}

func (a *accumulator) add(v float64) {
	if a.count == 0 {
		a.min = v
		a.max = v
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
	a.sum += v
	a.count++
}

func (a *accumulator) compute(fn AggregateFunc) float64 {
	switch fn {
	case AggAvg:
		if a.count == 0 {
			return 0
		}
		return a.sum / float64(a.count)
	case AggSum:
		return a.sum
	case AggMin:
		return a.min
	case AggMax:
		return a.max
	case AggCount:
		return float64(a.count)
	default:
		return 0
	}
}

func sortBuckets(buckets []Bucket) {
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})
}

// AggregateQuery extends Query with aggregation support.
type AggregateQuery struct {
	*Query
	aggOpts AggregateOptions
	groupBy []string
}

// NewAggregateQuery creates an aggregation query.
func (d *Database) NewAggregateQuery(metric string) *AggregateQuery {
	return &AggregateQuery{
		Query: d.NewQuery(metric),
	}
}

// Where sets the filter expression.
func (aq *AggregateQuery) Where(expr string) (*AggregateQuery, error) {
	_, err := aq.Query.Where(expr)
	if err != nil {
		return nil, err
	}
	return aq, nil
}

// TimeRange sets the time bounds.
func (aq *AggregateQuery) TimeRange(start, end int64) *AggregateQuery {
	aq.Query.TimeRange(start, end)
	return aq
}

// BucketSize sets the aggregation bucket width.
func (aq *AggregateQuery) BucketSize(ns int64) *AggregateQuery {
	aq.aggOpts.BucketSize = ns
	return aq
}

// Avg sets the aggregation function to average.
func (aq *AggregateQuery) Avg() *AggregateQuery {
	aq.aggOpts.Func = AggAvg
	return aq
}

// Sum sets the aggregation function to sum.
func (aq *AggregateQuery) Sum() *AggregateQuery {
	aq.aggOpts.Func = AggSum
	return aq
}

// Min sets the aggregation function to minimum.
func (aq *AggregateQuery) Min() *AggregateQuery {
	aq.aggOpts.Func = AggMin
	return aq
}

// Max sets the aggregation function to maximum.
func (aq *AggregateQuery) Max() *AggregateQuery {
	aq.aggOpts.Func = AggMax
	return aq
}

// Count sets the aggregation function to count.
func (aq *AggregateQuery) Count() *AggregateQuery {
	aq.aggOpts.Func = AggCount
	return aq
}

// GroupBy sets the tag keys to group results by.
func (aq *AggregateQuery) GroupBy(keys ...string) *AggregateQuery {
	aq.groupBy = keys
	return aq
}

// AggregateResult holds results for one group.
type AggregateResult struct {
	Tags    map[string]string
	Buckets []Bucket
}

// Execute runs the aggregation query.
func (aq *AggregateQuery) Execute() ([]AggregateResult, error) {
	seriesIDs, err := aq.Query.resolveFilter()
	if err != nil {
		return nil, err
	}

	if len(aq.groupBy) == 0 {
		return aq.executeNoGroupBy(seriesIDs)
	}
	return aq.executeWithGroupBy(seriesIDs)
}

func (aq *AggregateQuery) executeNoGroupBy(seriesIDs *roaring64.Bitmap) ([]AggregateResult, error) {
	var allPoints []DataPoint
	iter := seriesIDs.Iterator()

	for iter.HasNext() {
		sid := SeriesID(iter.Next())
		points, err := aq.db.Query(sid, aq.options)
		if err != nil {
			return nil, err
		}
		allPoints = append(allPoints, points...)
	}

	buckets := Aggregate(allPoints, aq.aggOpts)
	return []AggregateResult{{Buckets: buckets}}, nil
}

func (aq *AggregateQuery) executeWithGroupBy(seriesIDs *roaring64.Bitmap) ([]AggregateResult, error) {
	groups := make(map[string]*groupAccumulator)
	iter := seriesIDs.Iterator()

	for iter.HasNext() {
		sid := SeriesID(iter.Next())

		meta, err := aq.db.series.Get(sid)
		if err != nil {
			continue
		}

		groupKey := aq.buildGroupKey(meta.Tags)
		group, ok := groups[groupKey]
		if !ok {
			group = &groupAccumulator{
				tags: aq.extractGroupTags(meta.Tags),
			}
			groups[groupKey] = group
		}

		points, err := aq.db.Query(sid, aq.options)
		if err != nil {
			return nil, err
		}
		group.points = append(group.points, points...)
	}

	results := make([]AggregateResult, 0, len(groups))
	for _, group := range groups {
		buckets := Aggregate(group.points, aq.aggOpts)
		results = append(results, AggregateResult{
			Tags:    group.tags,
			Buckets: buckets,
		})
	}

	return results, nil
}

type groupAccumulator struct {
	tags   map[string]string
	points []DataPoint
}

func (aq *AggregateQuery) buildGroupKey(tags Tagset) string {
	key := ""
	for _, k := range aq.groupBy {
		key += k + "=" + tags.Get(k) + ","
	}
	return key
}

func (aq *AggregateQuery) extractGroupTags(tags Tagset) map[string]string {
	result := make(map[string]string)
	for _, k := range aq.groupBy {
		result[k] = tags.Get(k)
	}
	return result
}
