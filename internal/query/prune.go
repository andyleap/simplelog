package query

// predicate is a required equality (field == value) extracted from a query for
// segment pruning.
type predicate struct {
	field string
	value string
}

// prunablePredicates returns equalities on envelope fields that MUST hold for
// the expression to match. These are sound to use for segment pruning: a
// segment whose source summary cannot contain the value can be skipped.
func prunablePredicates(n Node) []predicate {
	return required(n)
}

// required returns the set of equality predicates that must be true for n to
// match. It is conservative: AND unions, OR intersects, NOT and everything
// non-equality contribute nothing.
func required(n Node) []predicate {
	switch t := n.(type) {
	case *AndNode:
		return union(required(t.L), required(t.R))
	case *OrNode:
		return intersect(required(t.L), required(t.R))
	case *CmpNode:
		if t.Op == OpEq && t.Field.Kind == fEnvelope && t.Lit.Kind == kindString {
			return []predicate{{field: t.Field.Name, value: t.Lit.S}}
		}
		return nil
	default:
		return nil
	}
}

func union(a, b []predicate) []predicate {
	out := append([]predicate(nil), a...)
	for _, p := range b {
		if !hasPred(out, p) {
			out = append(out, p)
		}
	}
	return out
}

func intersect(a, b []predicate) []predicate {
	var out []predicate
	for _, p := range a {
		if hasPred(b, p) {
			out = append(out, p)
		}
	}
	return out
}

func hasPred(set []predicate, p predicate) bool {
	for _, q := range set {
		if q == p {
			return true
		}
	}
	return false
}
