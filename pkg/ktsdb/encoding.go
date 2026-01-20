package ktsdb

import (
	"encoding/binary"
	"math"
)

// Key prefixes for different data types in Badger.
// Using single-byte prefixes keeps keys compact and enables efficient prefix scans.
const (
	PrefixData   byte = 'd' // Data points: d|series_id|negated_ts -> value
	PrefixSeries byte = 's' // Series metadata: s|series_id -> metric + tags
	PrefixIndex  byte = 'i' // Tag index: i|tag:value|series_id -> empty
)

// Key sizes
const (
	SeriesIDSize  = 8                                // uint64
	TimestampSize = 8                                // int64 (nanoseconds)
	DataKeySize   = 1 + SeriesIDSize + TimestampSize // prefix + series_id + timestamp = 17 bytes
	SeriesKeySize = 1 + SeriesIDSize                 // prefix + series_id = 9 bytes
)

// EncodeDataKey encodes a data point key into the provided buffer.
// Format: [prefix][series_id BE][negated_timestamp BE]
//
// The timestamp is negated (bitwise NOT) so that newer timestamps sort first
// when iterating in lexicographic order. This enables efficient "newest first" scans.
//
// buf must be at least DataKeySize (17) bytes.
// Returns the number of bytes written.
func EncodeDataKey(buf []byte, seriesID uint64, timestamp int64) int {
	buf[0] = PrefixData
	binary.BigEndian.PutUint64(buf[1:9], seriesID)
	binary.BigEndian.PutUint64(buf[9:17], uint64(^timestamp))
	return DataKeySize
}

// DecodeDataKey extracts the series ID and timestamp from a data key.
// Returns seriesID, timestamp.
func DecodeDataKey(buf []byte) (uint64, int64) {
	seriesID := binary.BigEndian.Uint64(buf[1:9])
	negatedTS := binary.BigEndian.Uint64(buf[9:17])
	return seriesID, int64(^negatedTS)
}

// EncodeDataValue encodes a float64 value into the provided buffer.
// buf must be at least 8 bytes.
// Returns the number of bytes written.
func EncodeDataValue(buf []byte, value float64) int {
	binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	return 8
}

// DecodeDataValue extracts a float64 value from an encoded buffer.
func DecodeDataValue(buf []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(buf))
}

// EncodeSeriesKey encodes a series metadata key into the provided buffer.
// Format: [prefix][series_id BE]
//
// buf must be at least SeriesKeySize (9) bytes.
// Returns the number of bytes written.
func EncodeSeriesKey(buf []byte, seriesID uint64) int {
	buf[0] = PrefixSeries
	binary.BigEndian.PutUint64(buf[1:9], seriesID)
	return SeriesKeySize
}

// DecodeSeriesKey extracts the series ID from a series metadata key.
func DecodeSeriesKey(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf[1:9])
}

// EncodeIndexKey encodes a tag index key into the provided buffer.
// Format: [prefix][tag:value][series_id BE]
//
// The tag and value are written as "key:value" to enable prefix scans
// for all series matching a specific tag.
//
// buf must be large enough to hold: 1 + len(tag) + 1 + len(value) + 8 bytes.
// Returns the number of bytes written.
func EncodeIndexKey(buf []byte, tag, value string, seriesID uint64) int {
	n := 0
	buf[n] = PrefixIndex
	n++
	n += copy(buf[n:], tag)
	buf[n] = ':'
	n++
	n += copy(buf[n:], value)
	binary.BigEndian.PutUint64(buf[n:], seriesID)
	n += SeriesIDSize
	return n
}

// IndexKeyPrefixSize returns the size of an index key prefix (without series ID).
// This is useful for prefix scans to find all series with a given tag:value.
func IndexKeyPrefixSize(tag, value string) int {
	return 1 + len(tag) + 1 + len(value)
}

// EncodeIndexKeyPrefix encodes just the prefix portion of an index key (no series ID).
// Useful for prefix iteration to find all series matching a tag:value.
// Returns the number of bytes written.
func EncodeIndexKeyPrefix(buf []byte, tag, value string) int {
	n := 0
	buf[n] = PrefixIndex
	n++
	n += copy(buf[n:], tag)
	buf[n] = ':'
	n++
	n += copy(buf[n:], value)
	return n
}

// DataKeyPrefix returns the prefix for all data keys of a given series.
// Useful for iterating all data points for a series.
func DataKeyPrefix(buf []byte, seriesID uint64) int {
	buf[0] = PrefixData
	binary.BigEndian.PutUint64(buf[1:9], seriesID)
	return 1 + SeriesIDSize
}
