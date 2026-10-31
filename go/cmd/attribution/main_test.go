package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"math"
	"strings"
	"testing"
)

func TestFormula(t *testing.T) {
	e := effect(stat{w: 0.6, r: [2]float64{0.06, 0.06}}, stat{w: 0.4, r: [2]float64{0.02, 0.02}}, 0, 0.03)
	for i, v := range []float64{0.004, 0.02, 0.01} {
		if math.Abs(e[i]-v) > 1e-12 {
			t.Fatal(e)
		}
	}
}
func TestDates(t *testing.T) {
	a, _ := bucket("2023-01-02", "week")
	b, _ := bucket("2023-01-08", "week")
	c, _ := bucket("2023-01-01", "week")
	if a != b || a == c {
		t.Fatal(a, b, c)
	}
	if _, e := parseDate("2023-02-29"); e == nil {
		t.Fatal("accepted invalid date")
	}
	q, _ := bucket("2023-03-31", "quarter")
	if q != "2023-Q1" {
		t.Fatal(q)
	}
}
func TestLinked(t *testing.T) {
	a, pg, bg := 0.0, 1.0, 1.0
	for _, d := range [][2]float64{{0.1, 0.05}, {-0.1, -0.03}, {0, 0}} {
		a = a*(1+d[1]) + (d[0]-d[1])*pg
		pg *= 1 + d[0]
		bg *= 1 + d[1]
	}
	if math.Abs(a-(pg-bg)) > 1e-12 {
		t.Fatal(a, pg-bg)
	}
}

func TestCSVWriterEscapes(t *testing.T) {
	var b bytes.Buffer
	bw := bufio.NewWriter(&b)
	w := rowWriter{w: bw}
	fields := []string{"normal", "with,comma", "with\"quote", "with\nnewline", ""}
	if err := w.record(fields, []float64{0.12345}); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	row, err := csv.NewReader(strings.NewReader(b.String())).Read()
	if err != nil {
		t.Fatal(err)
	}
	for i, field := range fields {
		if row[i] != field {
			t.Fatal(row)
		}
	}
	if row[5] != "0.12345" {
		t.Fatal(row)
	}
}
