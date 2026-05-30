package query

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tString
	tNumber
	tBool
	tRegex
	tAnd
	tOr
	tNot
	tLParen
	tRParen
	tDot
	tLBrack
	tRBrack
	tOp // comparison operator; value held in tok.op
)

type token struct {
	kind tokKind
	text string // literal text (string/number/regex value already unescaped)
	op   Op
	pos  int
}

type lexer struct {
	src string
	pos int
}

func (l *lexer) errf(format string, args ...any) error {
	return fmt.Errorf("query: %s (at pos %d)", fmt.Sprintf(format, args...), l.pos)
}

func (l *lexer) next() (token, error) {
	l.skipSpace()
	if l.pos >= len(l.src) {
		return token{kind: tEOF, pos: l.pos}, nil
	}
	start := l.pos
	c := l.src[l.pos]

	switch c {
	case '(':
		l.pos++
		return token{kind: tLParen, pos: start}, nil
	case ')':
		l.pos++
		return token{kind: tRParen, pos: start}, nil
	case '.':
		l.pos++
		return token{kind: tDot, pos: start}, nil
	case '[':
		l.pos++
		return token{kind: tLBrack, pos: start}, nil
	case ']':
		l.pos++
		return token{kind: tRBrack, pos: start}, nil
	case '"', '\'':
		return l.lexString(c)
	case '/':
		return l.lexRegex()
	case '=':
		if l.peekAt(1) == '~' {
			l.pos += 2
			return token{kind: tOp, op: OpRe, pos: start}, nil
		}
		l.pos++
		return token{kind: tOp, op: OpEq, pos: start}, nil
	case '!':
		if l.peekAt(1) == '=' {
			l.pos += 2
			return token{kind: tOp, op: OpNe, pos: start}, nil
		}
		if l.peekAt(1) == '~' {
			l.pos += 2
			return token{kind: tOp, op: OpNre, pos: start}, nil
		}
		return token{}, l.errf("unexpected '!'")
	case '<':
		if l.peekAt(1) == '=' {
			l.pos += 2
			return token{kind: tOp, op: OpLe, pos: start}, nil
		}
		l.pos++
		return token{kind: tOp, op: OpLt, pos: start}, nil
	case '>':
		if l.peekAt(1) == '=' {
			l.pos += 2
			return token{kind: tOp, op: OpGe, pos: start}, nil
		}
		l.pos++
		return token{kind: tOp, op: OpGt, pos: start}, nil
	case ':':
		l.pos++
		return token{kind: tOp, op: OpContains, pos: start}, nil
	}

	if c == '-' || c == '+' || (c >= '0' && c <= '9') {
		return l.lexNumber()
	}
	if isIdentStart(rune(c)) {
		return l.lexIdent()
	}
	return token{}, l.errf("unexpected character %q", string(c))
}

func (l *lexer) peekAt(n int) byte {
	if l.pos+n < len(l.src) {
		return l.src[l.pos+n]
	}
	return 0
}

func (l *lexer) skipSpace() {
	for l.pos < len(l.src) {
		r, sz := utf8.DecodeRuneInString(l.src[l.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		l.pos += sz
	}
}

func (l *lexer) lexString(quote byte) (token, error) {
	start := l.pos
	l.pos++ // opening quote
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\\' && l.pos+1 < len(l.src) {
			next := l.src[l.pos+1]
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\', '"', '\'':
				sb.WriteByte(next)
			default:
				sb.WriteByte(next)
			}
			l.pos += 2
			continue
		}
		if c == quote {
			l.pos++
			return token{kind: tString, text: sb.String(), pos: start}, nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return token{}, l.errf("unterminated string")
}

func (l *lexer) lexRegex() (token, error) {
	start := l.pos
	l.pos++ // opening slash
	var sb strings.Builder
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\\' && l.pos+1 < len(l.src) {
			sb.WriteByte(c)
			sb.WriteByte(l.src[l.pos+1])
			l.pos += 2
			continue
		}
		if c == '/' {
			l.pos++
			return token{kind: tRegex, text: sb.String(), pos: start}, nil
		}
		sb.WriteByte(c)
		l.pos++
	}
	return token{}, l.errf("unterminated regex")
}

func (l *lexer) lexNumber() (token, error) {
	start := l.pos
	if l.src[l.pos] == '-' || l.src[l.pos] == '+' {
		l.pos++
	}
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if (c >= '0' && c <= '9') || c == '.' || c == 'e' || c == 'E' || c == '-' || c == '+' {
			l.pos++
			continue
		}
		break
	}
	return token{kind: tNumber, text: l.src[start:l.pos], pos: start}, nil
}

func (l *lexer) lexIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.src) {
		r, sz := utf8.DecodeRuneInString(l.src[l.pos:])
		if !isIdentPart(r) {
			break
		}
		l.pos += sz
	}
	text := l.src[start:l.pos]
	switch strings.ToLower(text) {
	case "and":
		return token{kind: tAnd, pos: start}, nil
	case "or":
		return token{kind: tOr, pos: start}, nil
	case "not":
		return token{kind: tNot, pos: start}, nil
	case "true":
		return token{kind: tBool, text: "true", pos: start}, nil
	case "false":
		return token{kind: tBool, text: "false", pos: start}, nil
	}
	return token{kind: tIdent, text: text, pos: start}, nil
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

// isIdentPart keeps bare identifiers conservative (letters, digits, _). Keys
// containing other characters (e.g. app.kubernetes.io/name) are written with
// the bracket form: label["app.kubernetes.io/name"].
func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
