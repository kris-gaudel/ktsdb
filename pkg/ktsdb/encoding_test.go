package ktsdb

import (
	"math"
	"testing"
)

func TestEncodeDecodeDataKey(t *testing.T) {
	tests := []struct {
		name      string
		seriesID  uint64
		timestamp int64
	}{
		{"zero values", 0, 0},
		{"typical values", 12345, 1703635200000000000}, // Dec 27, 2024 in nanos
		{"max series ID", math.MaxUint64, 1000},
		{"negative timestamp", 100, -1000}, // Before epoch
		{"max timestamp", 42, math.MaxInt64},
	}

	buf := make([]byte, DataKeySize)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := EncodeDataKey(buf, tt.seriesID, tt.timestamp)
			if n != DataKeySize {
				t.Errorf("EncodeDataKey returned %d, want %d", n, DataKeySize)
			}

			gotSeriesID, gotTimestamp := DecodeDataKey(buf)
			if gotSeriesID != tt.seriesID {
				t.Errorf("seriesID = %d, want %d", gotSeriesID, tt.seriesID)
			}
			if gotTimestamp != tt.timestamp {
				t.Errorf("timestamp = %d, want %d", gotTimestamp, tt.timestamp)
			}
		})
	}
}

func TestDataKeyOrdering(t *testing.T) {
	// Verify that newer timestamps sort BEFORE older timestamps
	// This is crucial for efficient "newest first" iteration

	buf1 := make([]byte, DataKeySize)
	buf2 := make([]byte, DataKeySize)

	seriesID := uint64(100)
	olderTS := int64(1000)
	newerTS := int64(2000)

	EncodeDataKey(buf1, seriesID, newerTS)
	EncodeDataKey(buf2, seriesID, olderTS)

	// Compare lexicographically - newer should come first (be "smaller")
	for i := 0; i < DataKeySize; i++ {
		if buf1[i] < buf2[i] {
			return // Correct: newer timestamp sorts first
		}
		if buf1[i] > buf2[i] {
			t.Errorf("newer timestamp should sort before older timestamp")
			return
		}
	}
	t.Errorf("keys are equal, but timestamps differ")
}

func TestEncodeDecodeDataValue(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"zero", 0.0},
		{"positive", 42.5},
		{"negative", -123.456},
		{"very small", 1e-100},
		{"very large", 1e100},
		{"infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		// Note: NaN != NaN, so we skip that case
	}

	buf := make([]byte, 8)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := EncodeDataValue(buf, tt.value)
			if n != 8 {
				t.Errorf("EncodeDataValue returned %d, want 8", n)
			}

			got := DecodeDataValue(buf)
			if got != tt.value {
				t.Errorf("value = %v, want %v", got, tt.value)
			}
		})
	}
}

func TestEncodeSeriesKey(t *testing.T) {
	buf := make([]byte, SeriesKeySize)

	seriesID := uint64(0xDEADBEEF12345678)
	n := EncodeSeriesKey(buf, seriesID)

	if n != SeriesKeySize {
		t.Errorf("EncodeSeriesKey returned %d, want %d", n, SeriesKeySize)
	}

	if buf[0] != PrefixSeries {
		t.Errorf("prefix = %c, want %c", buf[0], PrefixSeries)
	}

	got := DecodeSeriesKey(buf)
	if got != seriesID {
		t.Errorf("seriesID = %x, want %x", got, seriesID)
	}
}

func TestEncodeIndexKey(t *testing.T) {
	tag := "env"
	value := "prod"
	seriesID := uint64(42)

	expectedSize := 1 + len(tag) + 1 + len(value) + SeriesIDSize // i + env + : + prod + 8
	buf := make([]byte, expectedSize)

	n := EncodeIndexKey(buf, tag, value, seriesID)
	if n != expectedSize {
		t.Errorf("EncodeIndexKey returned %d, want %d", n, expectedSize)
	}

	if buf[0] != PrefixIndex {
		t.Errorf("prefix = %c, want %c", buf[0], PrefixIndex)
	}

	expectedTagValue := "env:prod"
	gotTagValue := string(buf[1 : 1+len(expectedTagValue)])
	if gotTagValue != expectedTagValue {
		t.Errorf("tag:value = %q, want %q", gotTagValue, expectedTagValue)
	}
}

func TestIndexKeyPrefix(t *testing.T) {
	tag := "service"
	value := "database"

	prefixSize := IndexKeyPrefixSize(tag, value)
	buf := make([]byte, prefixSize)

	n := EncodeIndexKeyPrefix(buf, tag, value)
	if n != prefixSize {
		t.Errorf("EncodeIndexKeyPrefix returned %d, want %d", n, prefixSize)
	}

	// Verify a full index key starts with this prefix
	fullBuf := make([]byte, prefixSize+SeriesIDSize)
	EncodeIndexKey(fullBuf, tag, value, 12345)

	for i := 0; i < prefixSize; i++ {
		if buf[i] != fullBuf[i] {
			t.Errorf("prefix mismatch at byte %d: got %x, want %x", i, buf[i], fullBuf[i])
		}
	}
}

func BenchmarkEncodeDataKey(b *testing.B) {
	buf := make([]byte, DataKeySize)
	seriesID := uint64(12345)
	timestamp := int64(1703635200000000000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		EncodeDataKey(buf, seriesID, timestamp)
	}
}

func BenchmarkDecodeDataKey(b *testing.B) {
	buf := make([]byte, DataKeySize)
	EncodeDataKey(buf, 12345, 1703635200000000000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		DecodeDataKey(buf)
	}
}

func BenchmarkEncodeDataValue(b *testing.B) {
	buf := make([]byte, 8)
	value := 42.5

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		EncodeDataValue(buf, value)
	}
}
