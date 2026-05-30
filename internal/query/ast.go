// Package query implements simplelog's custom boolean expression language for
// filtering structured logs, plus (in planner.go) the segment-pruning query
// planner and the live-tail hub.
//
// Grammar:
//
//	expr    := orExpr
//	orExpr  := andExpr ("or" andExpr)*
//	andExpr := notExpr ("and" notExpr)*
//	notExpr := "not" notExpr | primary
//	primary := "(" expr ")" | field op value | field      // bare field = exists/non-empty
//	op      := "=" | "!=" | "=~" | "!~" | "<" | "<=" | ">" | ">=" | ":"
//	field   := IDENT ("." IDENT | "[" STRING "]")*
//	value   := STRING | NUMBER | BOOL | REGEX
package query

import (
	"regexp"

	"github.com/andyleap/simplelog/internal/model"
)

// Node is an evaluatable AST node.
type Node interface {
	// Match reports whether the record satisfies this node.
	Match(r *model.Record) bool
}

// AndNode is logical conjunction.
type AndNode struct{ L, R Node }

func (n *AndNode) Match(r *model.Record) bool { return n.L.Match(r) && n.R.Match(r) }

// OrNode is logical disjunction.
type OrNode struct{ L, R Node }

func (n *OrNode) Match(r *model.Record) bool { return n.L.Match(r) || n.R.Match(r) }

// NotNode is logical negation.
type NotNode struct{ X Node }

func (n *NotNode) Match(r *model.Record) bool { return !n.X.Match(r) }

// ExistsNode is a bare field reference: true if the field is present and
// non-empty/non-null.
type ExistsNode struct{ Field FieldRef }

func (n *ExistsNode) Match(r *model.Record) bool {
	v, ok := n.Field.resolve(r)
	if !ok {
		return false
	}
	switch v.Kind {
	case kindString:
		return v.S != ""
	case kindNull:
		return false
	default:
		return true
	}
}

// Op is a comparison operator.
type Op int

const (
	OpEq       Op = iota // =
	OpNe                 // !=
	OpRe                 // =~
	OpNre                // !~
	OpLt                 // <
	OpLe                 // <=
	OpGt                 // >
	OpGe                 // >=
	OpContains           // :
)

// CmpNode compares a field against a literal.
type CmpNode struct {
	Field FieldRef
	Op    Op
	Lit   value          // pre-parsed literal; lexical type drives coercion
	Re    *regexp.Regexp // compiled at parse time for OpRe/OpNre
}

func (n *CmpNode) Match(r *model.Record) bool {
	v, ok := n.Field.resolve(r)

	switch n.Op {
	case OpNe, OpNre:
		// "not equal to a missing field" is true.
		if !ok {
			return true
		}
	default:
		if !ok {
			return false
		}
	}

	switch n.Op {
	case OpEq:
		return cmpEq(v, n.Lit)
	case OpNe:
		return !cmpEq(v, n.Lit)
	case OpRe:
		return n.Re.MatchString(v.str())
	case OpNre:
		return !n.Re.MatchString(v.str())
	case OpContains:
		return containsStr(v.str(), n.Lit.S)
	case OpLt, OpLe, OpGt, OpGe:
		return cmpOrder(v, n.Lit, n.Op)
	}
	return false
}
