package ktsdb

import "sort"

// Tag represents a key-value label attached to a series.
type Tag struct {
	Key   string
	Value string
}

// Tagset is a sorted slice of tags.
// Tags are sorted by key, then by value for consistent hashing.
type Tagset []Tag

// Sort sorts the tagset in place by key, then value.
func (t Tagset) Sort() {
	sort.Slice(t, func(i, j int) bool {
		if t[i].Key != t[j].Key {
			return t[i].Key < t[j].Key
		}
		return t[i].Value < t[j].Value
	})
}

// FromMap creates a sorted Tagset from a map.
func FromMap(m map[string]string) Tagset {
	if len(m) == 0 {
		return nil
	}
	t := make(Tagset, 0, len(m))
	for k, v := range m {
		t = append(t, Tag{Key: k, Value: v})
	}
	t.Sort()
	return t
}

// Get returns the value for a key, or empty string if not found.
func (t Tagset) Get(key string) string {
	for _, tag := range t {
		if tag.Key == key {
			return tag.Value
		}
	}
	return ""
}

// Equal returns true if two tagsets have the same tags.
func (t Tagset) Equal(other Tagset) bool {
	if len(t) != len(other) {
		return false
	}
	for i := range t {
		if t[i] != other[i] {
			return false
		}
	}
	return true
}
