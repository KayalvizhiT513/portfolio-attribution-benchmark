package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestEffectiveMaster(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "security_master.csv")
	write := func(text string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	header := "InstrumentId,EffectiveDate,Currency,Country\n"
	write(header + "A,2023-02-01,GBP,GB\nB,2023-01-01,USD,US\nA,2023-01-01,USD,US\n")
	m, err := loadMaster(config{data: dir, scenario: "country"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ date, country string }{
		{"2023-01-01", "US"}, {"2023-01-31", "US"}, {"2023-02-01", "GB"}, {"2024-01-01", "GB"},
	} {
		k, err := m.countryOn(m.ids["A"], tc.date)
		if err != nil || m.countryNames[k] != tc.country {
			t.Fatalf("%s: country %d, error %v", tc.date, k, err)
		}
	}
	if _, err := m.countryOn(m.ids["A"], "2022-12-31"); err == nil {
		t.Fatal("accepted holdings before first master date")
	}
	// Both streams must filter and normalize using the classification for that day.
	for _, entity := range []string{"INDEX", "PF"} {
		holdings := filepath.Join(dir, entity+".csv")
		text := "Entity,EffectiveDate,InstrumentId,Weight,BaseReturn,LocalReturn\n"
		for _, date := range []string{"2023-01-31", "2023-02-01"} {
			text += entity + "," + date + ",A,0.5,0.1,0.1\n" + entity + "," + date + ",B,0.5,0.02,0.02\n"
		}
		if err := os.WriteFile(holdings, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		stream, err := newStream(holdings)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []float64{0.06, 0.02} {
			day, err := stream.next(&m, config{scenario: "country"}, &metrics{})
			if err != nil {
				t.Fatal(err)
			}
			got := stat{}
			for _, security := range day.stats {
				got.w += security.w
				got.r[0] += security.r[0]
			}
			if got.w != 1 || math.Abs(got.r[0]-want) > 1e-12 {
				t.Fatalf("got %+v, want return %g", got, want)
			}
		}
		stream.r.Close()
	}
	for _, rows := range []string{
		"A,2023-01-01,USD,US\nA,2023-01-01,GBP,GB\n",
		"A,2023-02-29,USD,US\n", "A,,USD,US\n",
	} {
		write(header + rows)
		if _, err := loadMaster(config{data: dir}); err == nil {
			t.Fatalf("accepted invalid master: %s", rows)
		}
	}
	write("InstrumentId,Currency,Country\nA,USD,US\n")
	m, err = loadMaster(config{data: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.countryOn(m.ids["A"], "1900-01-01"); err != nil {
		t.Fatal(err)
	}
	write("InstrumentId,Currency,Country\nA,USD,US\nA,GBP,GB\n")
	if _, err := loadMaster(config{data: dir}); err == nil {
		t.Fatal("accepted duplicate legacy ID")
	}
}
