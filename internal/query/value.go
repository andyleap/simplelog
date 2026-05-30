package query

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/andyleap/simplelog/internal/model"
)

type valueKind int

const (
	kindNull valueKind = iota
	kindString
	kindNumber
	kindBool
)

// value is a resolved field value or a parsed literal.
type value struct {
	Kind valueKind
	S    string
	N    float64
	B    bool
}

// str renders the value as a string for regex/contains operations.
func (v value) str() string {
	switch v.Kind {
	case kindString:
		return v.S
	case kindNumber:
		return strconv.FormatFloat(v.N, 'f', -1, 64)
	case kindBool:
		return strconv.FormatBool(v.B)
	default:
		return ""
	}
}

// numeric attempts to coerce the value to a float64.
func (v value) numeric() (float64, bool) {
	switch v.Kind {
	case kindNumber:
		return v.N, true
	case kindString:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.S), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// fieldKind selects which part of the record a FieldRef addresses.
type fieldKind int

const (
	fEnvelope fieldKind = iota
	fLabel
	fAnnotation
	fBody
)

// FieldRef is a resolved field path.
type FieldRef struct {
	Kind fieldKind
	Name string   // envelope field name (with _), or label/annotation key
	Path []string // body path (for fBody)
}

// resolve returns the field's value and whether it is present.
func (f FieldRef) resolve(r *model.Record) (value, bool) {
	switch f.Kind {
	case fEnvelope:
		return resolveEnvelope(r, f.Name)
	case fLabel:
		s, ok := r.Labels[f.Name]
		return value{Kind: kindString, S: s}, ok
	case fAnnotation:
		s, ok := r.Annotations[f.Name]
		return value{Kind: kindString, S: s}, ok
	case fBody:
		return resolveBody(r, f.Path)
	}
	return value{}, false
}

func resolveEnvelope(r *model.Record, name string) (value, bool) {
	switch name {
	case "_namespace":
		return value{Kind: kindString, S: r.Namespace}, r.Namespace != ""
	case "_pod":
		return value{Kind: kindString, S: r.Pod}, r.Pod != ""
	case "_container":
		return value{Kind: kindString, S: r.Container}, r.Container != ""
	case "_node":
		return value{Kind: kindString, S: r.Node}, r.Node != ""
	case "_image":
		return value{Kind: kindString, S: r.Image}, r.Image != ""
	case "_stream":
		return value{Kind: kindString, S: r.Stream}, r.Stream != ""
	case "_message":
		return value{Kind: kindString, S: r.Message}, true
	case "_ts":
		// Expose as RFC3339Nano string so string compare and regex work; numeric
		// comparison coerces via unix-nanos fallthrough below.
		return value{Kind: kindNumber, N: float64(r.Timestamp.UnixNano())}, !r.Timestamp.IsZero()
	}
	return value{}, false
}

// resolveBody walks path into the record body and decodes the leaf JSON value.
func resolveBody(r *model.Record, path []string) (value, bool) {
	if len(path) == 0 || r.Body == nil {
		return value{}, false
	}
	raw, ok := r.Body[path[0]]
	if !ok {
		return value{}, false
	}
	for _, key := range path[1:] {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return value{}, false
		}
		raw, ok = obj[key]
		if !ok {
			return value{}, false
		}
	}
	return decodeJSON(raw)
}

// decodeJSON turns a raw JSON value into a query value. Objects and arrays are
// rendered as their raw JSON string (so regex/contains still work on them).
func decodeJSON(raw json.RawMessage) (value, bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return value{Kind: kindNull}, true
	}
	switch trimmed[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return value{}, false
		}
		return value{Kind: kindString, S: s}, true
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(raw, &b); err == nil {
			return value{Kind: kindBool, B: b}, true
		}
		return value{Kind: kindString, S: trimmed}, true
	case '{', '[':
		return value{Kind: kindString, S: trimmed}, true
	default:
		var n float64
		if err := json.Unmarshal(raw, &n); err == nil {
			return value{Kind: kindNumber, N: n}, true
		}
		return value{Kind: kindString, S: trimmed}, true
	}
}

// cmpEq compares for equality. The literal's lexical type drives coercion: a
// numeric literal compares numerically, a string literal compares as strings.
func cmpEq(v, lit value) bool {
	switch lit.Kind {
	case kindNumber:
		fv, ok := v.numeric()
		return ok && fv == lit.N
	case kindBool:
		switch v.Kind {
		case kindBool:
			return v.B == lit.B
		case kindString:
			return v.S == strconv.FormatBool(lit.B)
		}
		return false
	default: // string literal
		return v.str() == lit.S
	}
}

// cmpOrder evaluates <, <=, >, >=. Numeric literals force numeric comparison;
// string literals compare lexicographically.
func cmpOrder(v, lit value, op Op) bool {
	if lit.Kind == kindNumber {
		fv, ok := v.numeric()
		if !ok {
			return false
		}
		return orderFloat(fv, lit.N, op)
	}
	return orderString(v.str(), lit.S, op)
}

func orderFloat(a, b float64, op Op) bool {
	switch op {
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	}
	return false
}

func orderString(a, b string, op Op) bool {
	switch op {
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	case OpGe:
		return a >= b
	}
	return false
}

func containsStr(haystack, needle string) bool { return strings.Contains(haystack, needle) }
