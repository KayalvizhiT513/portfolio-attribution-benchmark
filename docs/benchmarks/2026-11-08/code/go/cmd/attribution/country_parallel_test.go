package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParallelCountryRanges(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	write("security_master.csv", "InstrumentId,EffectiveDate,Currency,Country\nA,2023-02-01,GBP,GB\nA,2023-01-01,USD,US\nB,2023-01-01,USD,US\n")
	var index, portfolio strings.Builder
	header := strings.Join(inputHeader, ",") + "\r\n"
	index.WriteString(header)
	portfolio.WriteString(header)
	dates := []string{"2023-01-30", "2023-01-31", "2023-02-01", "2023-02-02", "2023-02-03"}
	for _, date := range dates {
		index.WriteString("INDEX," + date + ",A,0.5,0.1,0.1\r\nINDEX," + date + ",B,0.5,0.02,0.02\r\n")
		portfolio.WriteString("PF," + date + ",A,0.5,0.1,0.1\r\nPF," + date + ",B,0.5,0.04,0.04\r\n")
	}
	ip := write("index.csv", strings.TrimSuffix(index.String(), "\r\n"))
	pp := write("portfolio_PF.csv", strings.TrimSuffix(portfolio.String(), "\r\n"))
	c := config{data: dir, output: dir, scenario: "country", period: "day", threads: 3}
	m, err := loadMaster(c)
	if err != nil {
		t.Fatal(err)
	}
	mt, _, _, days, used, err := countryParallel(c, &m, []string{pp}, ip)
	if err != nil || !used || days != 5 || mt.rows != 20 || mt.written != 10 {
		t.Fatalf("used=%v days=%d rows=%d written=%d error=%v", used, days, mt.rows, mt.written, err)
	}
	f, err := os.Open(outputPath(c, pp) + ".partial")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i, date := range dates {
		if rows[1+i*2][2] != date || rows[2+i*2][2] != date {
			t.Fatal("unordered output", rows)
		}
	}
	c.maxDays = 2
	mt, _, _, days, used, err = countryParallel(c, &m, []string{pp}, ip)
	if err != nil || !used || days != 2 || mt.rows != 8 {
		t.Fatalf("maxDays: days=%d rows=%d error=%v", days, mt.rows, err)
	}
	quoted := write("quoted.csv", strings.ReplaceAll(index.String(), "INDEX,", "\"INDEX\nquoted\","))
	if _, ok, err := countryRanges(quoted, 0); err != nil || ok {
		t.Fatalf("quoted CSV should use streaming: %v %v", ok, err)
	}
}
