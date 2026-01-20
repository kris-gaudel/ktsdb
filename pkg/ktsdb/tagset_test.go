package ktsdb

import (
	"testing"
)

func TestTagsetSort(t *testing.T) {
	tags := Tagset{
		{Key: "z", Value: "1"},
		{Key: "a", Value: "2"},
		{Key: "m", Value: "3"},
	}
	tags.Sort()

	expected := Tagset{
		{Key: "a", Value: "2"},
		{Key: "m", Value: "3"},
		{Key: "z", Value: "1"},
	}

	if !tags.Equal(expected) {
		t.Errorf("got %v, want %v", tags, expected)
	}
}

func TestTagsetSortSameKey(t *testing.T) {
	tags := Tagset{
		{Key: "env", Value: "prod"},
		{Key: "env", Value: "dev"},
	}
	tags.Sort()

	if tags[0].Value != "dev" || tags[1].Value != "prod" {
		t.Errorf("same-key tags not sorted by value: %v", tags)
	}
}

func TestFromMap(t *testing.T) {
	m := map[string]string{
		"env":     "prod",
		"service": "api",
		"host":    "h1",
	}
	tags := FromMap(m)

	if len(tags) != 3 {
		t.Fatalf("len = %d, want 3", len(tags))
	}

	if tags[0].Key != "env" || tags[1].Key != "host" || tags[2].Key != "service" {
		t.Errorf("not sorted: %v", tags)
	}
}

func TestFromMapNil(t *testing.T) {
	tags := FromMap(nil)
	if tags != nil {
		t.Errorf("expected nil, got %v", tags)
	}

	tags = FromMap(map[string]string{})
	if tags != nil {
		t.Errorf("expected nil for empty map, got %v", tags)
	}
}

func TestTagsetGet(t *testing.T) {
	tags := Tagset{
		{Key: "env", Value: "prod"},
		{Key: "host", Value: "h1"},
	}

	if v := tags.Get("env"); v != "prod" {
		t.Errorf("Get(env) = %q, want prod", v)
	}
	if v := tags.Get("missing"); v != "" {
		t.Errorf("Get(missing) = %q, want empty", v)
	}
}

func TestTagsetEqual(t *testing.T) {
	a := Tagset{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	b := Tagset{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	c := Tagset{{Key: "a", Value: "1"}}
	d := Tagset{{Key: "a", Value: "1"}, {Key: "b", Value: "3"}}

	if !a.Equal(b) {
		t.Error("a should equal b")
	}
	if a.Equal(c) {
		t.Error("a should not equal c (different length)")
	}
	if a.Equal(d) {
		t.Error("a should not equal d (different value)")
	}
}

func BenchmarkFromMap(b *testing.B) {
	m := map[string]string{
		"env":     "prod",
		"service": "api",
		"host":    "h1",
		"region":  "us-east-1",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		FromMap(m)
	}
}
