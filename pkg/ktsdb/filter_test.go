package ktsdb

import (
	"testing"
)

func TestParseFilter(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType string
		wantErr  bool
	}{
		{"simple tag", "env:prod", "TagFilter", false},
		{"and filter", "env:prod AND host:h1", "AndFilter", false},
		{"or filter", "env:prod OR env:dev", "OrFilter", false},
		{"precedence", "env:prod OR env:dev AND host:h1", "OrFilter", false},
		{"parens", "(env:prod OR env:dev) AND host:h1", "AndFilter", false},
		{"chained and", "a:1 AND b:2 AND c:3", "AndFilter", false},
		{"special chars", "service.name:api-gateway", "TagFilter", false},
		{"numeric value", "port:8080", "TagFilter", false},
		{"empty", "", "", false},
		{"whitespace", "   ", "", false},
		{"lowercase and", "a:1 and b:2", "AndFilter", false},
		{"mixed case", "a:1 And b:2", "AndFilter", false},
		{"missing colon", "env", "", true},
		{"missing value", "env:", "", true},
		{"missing key", ":prod", "", true},
		{"missing operand", "AND", "", true},
		{"incomplete", "env:prod AND", "", true},
		{"unclosed paren", "(env:prod", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := ParseFilter(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q", tt.input)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.wantType == "" {
				if f != nil {
					t.Errorf("expected nil, got %T", f)
				}
				return
			}

			gotType := ""
			switch f.(type) {
			case TagFilter:
				gotType = "TagFilter"
			case AndFilter:
				gotType = "AndFilter"
			case OrFilter:
				gotType = "OrFilter"
			}

			if gotType != tt.wantType {
				t.Errorf("got %s, want %s", gotType, tt.wantType)
			}
		})
	}
}

func TestParseFilterTagValues(t *testing.T) {
	tests := []struct {
		input   string
		wantKey string
		wantVal string
	}{
		{"env:prod", "env", "prod"},
		{"service.name:api-gateway", "service.name", "api-gateway"},
		{"port:8080", "port", "8080"},
		{"version:v1.2.3", "version", "v1.2.3"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			f, err := ParseFilter(tt.input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			tag, ok := f.(TagFilter)
			if !ok {
				t.Fatalf("expected TagFilter, got %T", f)
			}

			if tag.Key != tt.wantKey || tag.Value != tt.wantVal {
				t.Errorf("got %s:%s, want %s:%s", tag.Key, tag.Value, tt.wantKey, tt.wantVal)
			}
		})
	}
}

func TestParseFilterPrecedence(t *testing.T) {
	// AND binds tighter than OR: a OR b AND c = a OR (b AND c)
	f, _ := ParseFilter("env:prod OR env:dev AND host:h1")

	or, ok := f.(OrFilter)
	if !ok {
		t.Fatalf("expected OrFilter at root, got %T", f)
	}

	if _, ok := or.Left.(TagFilter); !ok {
		t.Errorf("expected TagFilter on left, got %T", or.Left)
	}
	if _, ok := or.Right.(AndFilter); !ok {
		t.Errorf("expected AndFilter on right, got %T", or.Right)
	}
}

func TestParseFilterAssociativity(t *testing.T) {
	// Left-associative: a AND b AND c = (a AND b) AND c
	f, _ := ParseFilter("a:1 AND b:2 AND c:3")

	and1 := f.(AndFilter)
	and2, ok := and1.Left.(AndFilter)
	if !ok {
		t.Fatalf("expected nested AndFilter, got %T", and1.Left)
	}

	tag := and2.Left.(TagFilter)
	if tag.Key != "a" {
		t.Errorf("expected first tag key 'a', got %s", tag.Key)
	}
}

func BenchmarkParseFilter(b *testing.B) {
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
