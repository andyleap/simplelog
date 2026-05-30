package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/andyleap/simplelog/internal/model"
)

// Parse compiles a query expression into an AST. All errors (syntax, bad
// regex, malformed field path) are surfaced here so that evaluation never
// errors. An empty expression matches everything.
func Parse(expr string) (Node, error) {
	if strings.TrimSpace(expr) == "" {
		return matchAll{}, nil
	}
	p := &parser{lex: &lexer{src: expr}}
	if err := p.advance(); err != nil {
		return nil, err
	}
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.kind != tEOF {
		return nil, fmt.Errorf("query: unexpected token at pos %d", p.cur.pos)
	}
	return n, nil
}

// matchAll matches every record (empty query).
type matchAll struct{}

func (matchAll) Match(r *model.Record) bool { return true }

type parser struct {
	lex *lexer
	cur token
}

func (p *parser) advance() error {
	t, err := p.lex.next()
	if err != nil {
		return err
	}
	p.cur = t
	return nil
}

func (p *parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tOr {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrNode{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.cur.kind == tAnd {
		if err := p.advance(); err != nil {
			return nil, err
		}
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &AndNode{L: left, R: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Node, error) {
	if p.cur.kind == tNot {
		if err := p.advance(); err != nil {
			return nil, err
		}
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotNode{X: x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Node, error) {
	if p.cur.kind == tLParen {
		if err := p.advance(); err != nil {
			return nil, err
		}
		n, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.cur.kind != tRParen {
			return nil, fmt.Errorf("query: expected ')' at pos %d", p.cur.pos)
		}
		if err := p.advance(); err != nil {
			return nil, err
		}
		return n, nil
	}

	if p.cur.kind != tIdent {
		return nil, fmt.Errorf("query: expected field or '(' at pos %d", p.cur.pos)
	}
	field, err := p.parseField()
	if err != nil {
		return nil, err
	}

	if p.cur.kind != tOp {
		// Bare field => existence test.
		return &ExistsNode{Field: field}, nil
	}
	op := p.cur.op
	if err := p.advance(); err != nil {
		return nil, err
	}
	return p.parseComparison(field, op)
}

// parseField parses IDENT ("." IDENT | "[" STRING "]")* and classifies it into
// an envelope, label, annotation, or body reference.
func (p *parser) parseField() (FieldRef, error) {
	var parts []string
	parts = append(parts, p.cur.text)
	if err := p.advance(); err != nil {
		return FieldRef{}, err
	}
	for {
		switch p.cur.kind {
		case tDot:
			if err := p.advance(); err != nil {
				return FieldRef{}, err
			}
			if p.cur.kind != tIdent {
				return FieldRef{}, fmt.Errorf("query: expected identifier after '.' at pos %d", p.cur.pos)
			}
			parts = append(parts, p.cur.text)
			if err := p.advance(); err != nil {
				return FieldRef{}, err
			}
		case tLBrack:
			if err := p.advance(); err != nil {
				return FieldRef{}, err
			}
			if p.cur.kind != tString {
				return FieldRef{}, fmt.Errorf("query: expected string key in '[...]' at pos %d", p.cur.pos)
			}
			parts = append(parts, p.cur.text)
			if err := p.advance(); err != nil {
				return FieldRef{}, err
			}
			if p.cur.kind != tRBrack {
				return FieldRef{}, fmt.Errorf("query: expected ']' at pos %d", p.cur.pos)
			}
			if err := p.advance(); err != nil {
				return FieldRef{}, err
			}
		default:
			return classifyField(parts), nil
		}
	}
}

func classifyField(parts []string) FieldRef {
	head := parts[0]
	switch {
	case strings.HasPrefix(head, "_"):
		return FieldRef{Kind: fEnvelope, Name: head}
	case head == "label" && len(parts) >= 2:
		return FieldRef{Kind: fLabel, Name: strings.Join(parts[1:], ".")}
	case head == "annotation" && len(parts) >= 2:
		return FieldRef{Kind: fAnnotation, Name: strings.Join(parts[1:], ".")}
	case head == "body" && len(parts) >= 2:
		return FieldRef{Kind: fBody, Path: parts[1:]}
	default:
		// Bare path resolves into the body (e.g. level, stdout, json.user.id).
		return FieldRef{Kind: fBody, Path: parts}
	}
}

func (p *parser) parseComparison(field FieldRef, op Op) (Node, error) {
	lit := p.cur
	if err := p.advance(); err != nil {
		return nil, err
	}

	if op == OpRe || op == OpNre {
		if lit.kind != tRegex && lit.kind != tString {
			return nil, fmt.Errorf("query: =~ requires a /regex/ or string at pos %d", lit.pos)
		}
		re, err := regexp.Compile(lit.text)
		if err != nil {
			return nil, fmt.Errorf("query: bad regex at pos %d: %v", lit.pos, err)
		}
		return &CmpNode{Field: field, Op: op, Re: re}, nil
	}

	v, err := literalValue(lit)
	if err != nil {
		return nil, err
	}
	if op == OpContains && v.Kind != kindString {
		// Contains always operates on string rendering; coerce literal to string.
		v = value{Kind: kindString, S: v.str()}
	}
	return &CmpNode{Field: field, Op: op, Lit: v}, nil
}

func literalValue(t token) (value, error) {
	switch t.kind {
	case tString:
		return value{Kind: kindString, S: t.text}, nil
	case tNumber:
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return value{}, fmt.Errorf("query: bad number %q at pos %d", t.text, t.pos)
		}
		return value{Kind: kindNumber, N: f}, nil
	case tBool:
		return value{Kind: kindBool, B: t.text == "true"}, nil
	case tRegex:
		return value{}, fmt.Errorf("query: regex literal only valid with =~/!~ at pos %d", t.pos)
	default:
		return value{}, fmt.Errorf("query: expected a value at pos %d", t.pos)
	}
}
