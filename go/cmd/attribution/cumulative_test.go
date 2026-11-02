package main

import (
	"math"
	"strconv"
	"testing"
)

func TestZeroDenominators(t *testing.T) {
	obs := []totalObservation{{p: [2]float64{.01, .01}, b: [2]float64{.01, .01}}, {p: [2]float64{.01, .01}, b: [2]float64{.01, .01}}}
	got := cumulativeValues(obs, 0)
	for i, want := range []string{"0", "0", "0", "", ""} {
		if got[i] != want {
			t.Fatalf("%v", got)
		}
	}
	for _, v := range cumulativeValues(obs[:1], 0) {
		if v != "" {
			t.Fatal("single observation should be undefined")
		}
	}
}
func TestIdenticalSeries(t *testing.T) {
	obs := []totalObservation{}
	for _, v := range []float64{-.01, 0, .01} {
		obs = append(obs, totalObservation{p: [2]float64{v, v}, b: [2]float64{v, v}})
	}
	got := cumulativeValues(obs, 0)
	sd, _ := strconv.ParseFloat(got[0], 64)
	if math.Abs(sd-.01) > 1e-15 || got[2] != "0" || got[3] != "" || got[4] != "1" {
		t.Fatal(got)
	}
}

func TestCumulativeEmptyUSWindows(t *testing.T) {
	rows, err := cumulativeRows(nil, "PF", 4)
	if err != nil || len(rows) != 36 {
		t.Fatalf("rows=%v error=%v", rows, err)
	}
	for _, row := range rows {
		if row[4] != "0" || row[5] != "" {
			t.Fatal(row)
		}
		for _, v := range row[6:] {
			if v != "" {
				t.Fatal(row)
			}
		}
	}
}
