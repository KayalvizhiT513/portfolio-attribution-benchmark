package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIRejectsInvalidRequestsBeforeCalculation(t *testing.T) {
	handler := attributionHandler(config{threads: 3, output: t.TempDir()})
	for _, body := range []string{
		`{}`, `null`, `{"start_date":"2023-02-30","end_date":"2023-03-01"}`,
		`{"start_date":"2023-01-02","end_date":"2023-01-01"}`,
		`{"start_date":"2023-01-01","end_date":"2023-01-02","threads":999}`,
		`{"start_date":"2023-01-01","end_date":"2023-01-02"} {}`,
		strings.Repeat("x", 5000),
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("POST", "/attribution", strings.NewReader(body)))
		if w.Code != 400 {
			t.Fatalf("%s: status %d", body[:min(len(body), 100)], w.Code)
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	var health struct {
		Active, Completed, Failed int64
		Workers                   int `json:"workers_per_request"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if health.Active != 0 || health.Completed != 0 || health.Failed != 0 || health.Workers != 3 {
		t.Fatalf("invalid requests changed calculation counters: %+v", health)
	}
}
