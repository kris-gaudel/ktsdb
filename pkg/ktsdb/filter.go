package ktsdb

import (
	"fmt"
	"strings"
	"unicode"
)

// Filter represents a parsed filter expression.
type Filter interface {
	filter()
}

// TagFilter matches series with a specific tag value.
type TagFilter struct {
	Key   string
	Value string
}

func (TagFilter) filter() {}

// AndFilter combines filters with logical AND.
type AndFilter struct {
	Left  Filter
	Right Filter
}

func (AndFilter) filter() {}

// OrFilter combines filters with logical OR.
type OrFilter struct {
	Left  Filter
	Right Filter
}

func (OrFilter) filter() {}

// Token types for the lexer.
type tokenType int

const (
	tokenEOF tokenType = iota
	tokenIdent
	tokenColon
	tokenAnd
	tokenOr
	tokenLParen
	tokenRParen
)

type token struct {
	typ tokenType
	val string
}

// Lexer tokenizes a filter string.
type lexer struct {
	input string
	pos   int
}

func newLexer(input string) *lexer {
	return &lexer{input: input}
}

func (l *lexer) next() token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return token{typ: tokenEOF}
	}

	ch := l.input[l.pos]

	switch ch {
	case ':':
		l.pos++
		return token{typ: tokenColon, val: ":"}
	case '(':
		l.pos++
		return token{typ: tokenLParen, val: "("}
	case ')':
		l.pos++
		return token{typ: tokenRParen, val: ")"}
	}

	if isIdentStart(ch) {
		return l.scanIdent()
	}

	l.pos++
	return token{typ: tokenEOF}
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *lexer) scanIdent() token {
	start := l.pos
	for l.pos < len(l.input) && isIdentChar(l.input[l.pos]) {
		l.pos++
	}
	val := l.input[start:l.pos]

	upper := strings.ToUpper(val)
	switch upper {
	case "AND":
		return token{typ: tokenAnd, val: val}
	case "OR":
		return token{typ: tokenOr, val: val}
	}

	return token{typ: tokenIdent, val: val}
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_' || (ch >= '0' && ch <= '9')
}

func isIdentChar(ch byte) bool {
	return isIdentStart(ch) || (ch >= '0' && ch <= '9') || ch == '-' || ch == '.'
}

// Parser builds a Filter AST from tokens.
type parser struct {
	lex *lexer
	cur token
}

func newParser(input string) *parser {
	p := &parser{lex: newLexer(input)}
	p.cur = p.lex.next()
	return p
}

func (p *parser) advance() {
	p.cur = p.lex.next()
}

// ParseFilter parses a filter expression string.
// Grammar:
//
//	expr   = term (OR term)*
//	term   = factor (AND factor)*
//	factor = tag | '(' expr ')'
//	tag    = ident ':' ident
func ParseFilter(input string) (Filter, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	p := newParser(input)
	return p.parseExpr()
}

func (p *parser) parseExpr() (Filter, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == tokenOr {
		p.advance()
		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}
		left = OrFilter{Left: left, Right: right}
	}

	return left, nil
}

func (p *parser) parseTerm() (Filter, error) {
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.cur.typ == tokenAnd {
		p.advance()
		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		left = AndFilter{Left: left, Right: right}
	}

	return left, nil
}

func (p *parser) parseFactor() (Filter, error) {
	if p.cur.typ == tokenLParen {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.cur.typ != tokenRParen {
			return nil, fmt.Errorf("expected ')', got %q", p.cur.val)
		}
		p.advance()
		return expr, nil
	}

	return p.parseTag()
}

func (p *parser) parseTag() (Filter, error) {
	if p.cur.typ != tokenIdent {
		return nil, fmt.Errorf("expected tag key, got %q", p.cur.val)
	}
	key := p.cur.val
	p.advance()

	if p.cur.typ != tokenColon {
		return nil, fmt.Errorf("expected ':', got %q", p.cur.val)
	}
	p.advance()

	if p.cur.typ != tokenIdent {
		return nil, fmt.Errorf("expected tag value, got %q", p.cur.val)
	}
	value := p.cur.val
	p.advance()

	return TagFilter{Key: key, Value: value}, nil
}
