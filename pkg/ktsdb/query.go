package ktsdb

import (
	"github.com/RoaringBitmap/roaring/roaring64"
)

// Query executes a filter expression and returns matching data points.
type Query struct {
	db      *Database
	metric  string
	filter  Filter
	options QueryOptions
}

// NewQuery creates a query builder for a metric.
func (d *Database) NewQuery(metric string) *Query {
	return &Query{
		db:     d,
		metric: metric,
	}
}

// Where sets the filter expression (e.g., "env:prod AND host:h1").
func (q *Query) Where(expr string) (*Query, error) {
	f, err := ParseFilter(expr)
	if err != nil {
		return nil, err
	}
	q.filter = f
	return q, nil
}

// TimeRange sets the time bounds for the query.
func (q *Query) TimeRange(start, end int64) *Query {
	q.options.Start = start
	q.options.End = end
	return q
}

// Limit sets the maximum number of points per series.
func (q *Query) Limit(n int) *Query {
	q.options.Limit = n
	return q
}

// Execute runs the query and returns results grouped by series.
func (q *Query) Execute() (map[SeriesID][]DataPoint, error) {
	seriesIDs, err := q.resolveFilter()
	if err != nil {
		return nil, err
	}

	results := make(map[SeriesID][]DataPoint)
	iter := seriesIDs.Iterator()

	for iter.HasNext() {
		sid := SeriesID(iter.Next())
		points, err := q.db.Query(sid, q.options)
		if err != nil {
			return nil, err
		}
		if len(points) > 0 {
			results[sid] = points
		}
	}

	return results, nil
}

func (q *Query) resolveFilter() (*roaring64.Bitmap, error) {
	if q.filter == nil {
		return q.db.index.GetAllSeriesIDs(q.metric)
	}
	return q.evalFilter(q.filter)
}

func (q *Query) evalFilter(f Filter) (*roaring64.Bitmap, error) {
	switch v := f.(type) {
	case TagFilter:
		return q.db.index.GetSeriesIDs(q.metric, v.Key, v.Value)

	case AndFilter:
		left, err := q.evalFilter(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.evalFilter(v.Right)
		if err != nil {
			return nil, err
		}
		return Intersect(left, right), nil

	case OrFilter:
		left, err := q.evalFilter(v.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.evalFilter(v.Right)
		if err != nil {
			return nil, err
		}
		return Union(left, right), nil

	default:
		return roaring64.New(), nil
	}
}

// ExecuteRaw returns just the matching series IDs without fetching data.
func (q *Query) ExecuteRaw() (*roaring64.Bitmap, error) {
	return q.resolveFilter()
}
