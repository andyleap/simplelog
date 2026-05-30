package manager

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	"github.com/andyleap/simplelog/internal/store"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	st, err := store.Open(store.Config{ManagerID: "m", BaseDir: t.TempDir()}, store.NewMemBlobs())
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(st, SpoolConfig{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func seedAndSeal(t *testing.T, m *Manager) {
	t.Helper()
	base := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 30; i++ {
		level := "info"
		if i%3 == 0 {
			level = "error"
		}
		lvl, _ := json.Marshal(level)
		m.Spool.Accept(&model.Record{
			Source:    model.Source{Node: "n1", Path: "/x", Inode: 1},
			Offset:    int64(i + 1),
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			Namespace: "prod", Pod: "web", Container: "app", Node: "n1",
			Stream:  model.StreamStdout,
			Message: "msg",
			Labels:  map[string]string{"app": "web"},
			Body:    map[string]json.RawMessage{"level": lvl},
		})
	}
	if _, err := m.Spool.Seal(); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPEndpoints(t *testing.T) {
	m := newTestManager(t)
	seedAndSeal(t, m)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	// Embedded UI served at /.
	body := getString(t, srv.URL+"/")
	if !strings.Contains(body, "simplelog") {
		t.Fatalf("index.html not served: %.80q", body)
	}

	// /v1/fields aggregates from the catalog.
	var fields struct {
		Envelope, Fields, Namespaces []string
	}
	getJSON(t, srv.URL+"/v1/fields", &fields)
	if !contains(fields.Fields, "level") {
		t.Fatalf("fields missing level: %+v", fields)
	}
	if !contains(fields.Namespaces, "prod") {
		t.Fatalf("namespaces missing prod: %+v", fields)
	}

	// /v1/query NDJSON, filtered.
	lines := getLines(t, srv.URL+`/v1/query?expr=`+urlExpr(`level="error"`))
	if len(lines) != 10 { // every 3rd of 30
		t.Fatalf("expected 10 error records, got %d", len(lines))
	}
	var rec model.Record
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("bad NDJSON: %v", err)
	}

	// /v1/histogram returns bucket counts summing to the match total.
	var hist struct {
		Counts []int64
		Total  int64
	}
	getJSON(t, srv.URL+`/v1/histogram?expr=`+urlExpr(`level="error"`)+`&start=`+
		urlExpr("2026-05-30T11:00:00Z")+`&end=`+urlExpr("2026-05-30T14:00:00Z")+`&buckets=10`, &hist)
	if hist.Total != 10 {
		t.Fatalf("histogram total = %d, want 10", hist.Total)
	}
	var sum int64
	for _, c := range hist.Counts {
		sum += c
	}
	if sum != 10 {
		t.Fatalf("histogram counts sum = %d, want 10", sum)
	}
}

func urlExpr(s string) string {
	return strings.NewReplacer(`"`, "%22", " ", "%20", ":", "%3A", "+", "%2B").Replace(s)
}

func getString(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func getLines(t *testing.T, url string) []string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: %d %s", url, resp.StatusCode, b)
	}
	var out []string
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			out = append(out, sc.Text())
		}
	}
	return out
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
