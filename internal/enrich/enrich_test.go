package enrich

import "testing"

func TestShouldIngest(t *testing.T) {
	e := &Enricher{
		excludeAnno: "simplelog.io/ingest",
		byUID: map[string]podMeta{
			"opted-out": {annotations: map[string]string{"simplelog.io/ingest": "false"}},
			"opted-in":  {annotations: map[string]string{"simplelog.io/ingest": "true"}},
			"unrelated": {annotations: map[string]string{"other": "x"}},
		},
	}
	cases := map[string]bool{
		"opted-out": false, // annotation = "false" => excluded
		"opted-in":  true,
		"unrelated": true,
		"unknown":   true, // not in cache yet => default ingest
	}
	for uid, want := range cases {
		if got := e.ShouldIngest(uid); got != want {
			t.Errorf("ShouldIngest(%q) = %v, want %v", uid, got, want)
		}
	}

	// Empty annotation key disables filtering entirely.
	none := &Enricher{byUID: e.byUID}
	if !none.ShouldIngest("opted-out") {
		t.Error("with no exclude annotation configured, all pods should ingest")
	}
}
