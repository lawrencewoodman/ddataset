package dcache

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/dcsv"
	"github.com/lawrencewoodman/ddataset/internal/testhelpers"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
)

func TestNew(t *testing.T) {
	cases := []struct {
		filename     string
		separator    rune
		fieldNames   []string
		maxCacheRows int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			0},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			1},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			5},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			1000},
	}

	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds := New(ds, c.maxCacheRows)

		for i := 0; i < 10; i++ {
			if err := testhelpers.CheckDatasetsEqual(ds, cds); err != nil {
				t.Fatalf("checkDatasetsEqual err: %s", err)
			}
		}
	}
}

func TestOpen(t *testing.T) {
	cases := []struct {
		filename     string
		separator    rune
		fieldNames   []string
		maxCacheRows int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			0},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			1},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			5},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			1000},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds := New(ds, c.maxCacheRows)
		dsConn, err := ds.Open()
		if err != nil {
			t.Fatalf("ds.Open() err: %s", err)
		}
		cdsConn, err := cds.Open()
		if err != nil {
			t.Fatalf("cds.Open() err: %s", err)
		}

		if err := testhelpers.CheckDatasetConnsEqual(dsConn, cdsConn); err != nil {
			t.Errorf("checkDatasetConnsEqual err: %s", err)
		}
	}
}

func TestOpen_multiple_conns(t *testing.T) {
	cases := []struct {
		filename     string
		separator    rune
		fieldNames   []string
		maxCacheRows int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			0},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			1},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			5},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			100},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			1000},
	}
	var err error
	const numConns = 10

	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds := New(ds, c.maxCacheRows)
		cdsConns := make([]ddataset.Conn, numConns)
		for i := range cdsConns {
			cdsConns[i], err = cds.Open()
			if err != nil {
				t.Fatalf("cds.Open() err: %s", err)
			}
		}

		for _, c := range cdsConns {
			cdsConnRef, err := cds.Open()
			if err != nil {
				t.Fatalf("cds.Open() err: %s", err)
			}
			if err := testhelpers.CheckDatasetConnsEqual(cdsConnRef, c); err != nil {
				t.Fatalf("checkDatasetsEqual err: %s", err)
			}
		}
	}
}

func TestOpen_errors(t *testing.T) {
	filename := "missing.csv"
	fieldNames := []string{"age", "occupation"}
	maxCacheRows := 100
	wantErr := &os.PathError{"open", "missing.csv", syscall.ENOENT}
	ds := dcsv.New(filename, false, ';', fieldNames)
	rds := New(ds, maxCacheRows)
	_, err := rds.Open()
	if err := testhelpers.CheckPathErrorMatch(err, wantErr); err != nil {
		t.Errorf("Open() - filename: %s - problem with error: %s",
			filename, err)
	}
}

func TestFields(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	fieldNames := []string{
		"age", "job", "marital", "education", "default", "balance",
		"housing", "loan", "contact", "day", "month", "duration", "campaign",
		"pdays", "previous", "poutcome", "y",
	}
	maxCacheRows := 100
	ds := dcsv.New(filename, false, ';', fieldNames)
	rds := New(ds, maxCacheRows)

	got := rds.Fields()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("Fields() - got: %s, want: %s", got, fieldNames)
	}
}

func TestErr(t *testing.T) {
	cases := []struct {
		filename     string
		separator    rune
		fieldNames   []string
		maxCacheRows int
		wantErr      error
	}{
		{filepath.Join("fixtures", "invalid_numfields_at_102.csv"), ',',
			[]string{"band", "score", "team", "points", "rating"},
			105,
			&csv.ParseError{102, 0, errors.New("wrong number of fields in line")}},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome"},
			4,
			errors.New("wrong number of field names for dataset")},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			20, nil},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, false, c.separator, c.fieldNames)
		rds := New(ds, c.maxCacheRows)
		conn, err := rds.Open()
		if err != nil {
			t.Fatalf("Open() - filename: %s, err: %s", c.filename, err)
		}
		for conn.Next() {
			conn.Read()
		}
		if c.wantErr == nil {
			if conn.Err() != nil {
				t.Errorf("Read() - filename: %s, wantErr: %s, got error: %s",
					c.filename, c.wantErr, conn.Err())
			}
		} else {
			if conn.Err() == nil || conn.Err().Error() != c.wantErr.Error() {
				t.Errorf("Read() - filename: %s, wantErr: %s, got error: %s",
					c.filename, c.wantErr, conn.Err())
			}
		}
	}
}

func TestNext(t *testing.T) {
	cases := []struct {
		filename       string
		separator      rune
		hasHeader      bool
		fieldNames     []string
		maxCacheRows   int
		wantNumRecords int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 0, 9},
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 5, 9},
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 9, 9},
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 100, 9},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		cds := New(ds, c.maxCacheRows)
		conn, err := cds.Open()
		if err != nil {
			t.Fatalf("Open() - filename: %s, err: %s", c.filename, err)
		}
		numRecords := 0
		for conn.Next() {
			numRecords++
		}
		if conn.Next() {
			t.Errorf("conn.Next() - Return true, despite having finished")
		}
		if numRecords != c.wantNumRecords {
			t.Errorf("conn.Next() - filename: %s, wantNumRecords: %d, gotNumRecords: %d",
				c.filename, c.wantNumRecords, numRecords)
		}
	}
}

func TestOpenNextRead_multiple_conns(t *testing.T) {
	filename := filepath.Join("fixtures", "debt.csv")
	hasHeader := true
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	cacheRecords := 100
	ds := dcsv.New(filename, hasHeader, ',', fieldNames)
	cds := New(ds, cacheRecords)
	numSums := 10
	sumBalances := make([]int64, numSums)

	for i := 0; i < numSums; i++ {
		sumBalances[i] = testhelpers.SumBalance(cds)
	}

	sumBalance := sumBalances[0]
	for i, s := range sumBalances {
		if s != sumBalance {
			t.Error("sumBalances are not all equal: ", i)
			return
		}
	}
}

func TestOpenNextRead_goroutines(t *testing.T) {
	cases := []struct {
		cacheRecords int
	}{
		{0}, {100}, {1000}, {10000}, {100000},
	}
	var numGoroutines int
	filename := filepath.Join("fixtures", "debt.csv")
	hasHeader := true
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := dcsv.New(filename, hasHeader, ',', fieldNames)
	if testing.Short() {
		numGoroutines = 10
	} else {
		numGoroutines = 500
	}
	for _, c := range cases {
		cds := New(ds, c.cacheRecords)
		sumBalances := make(chan int64, numGoroutines)
		wg := sync.WaitGroup{}
		wg.Add(numGoroutines)

		sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
			defer wg.Done()
			sum <- testhelpers.SumBalance(ds)
		}

		for i := 0; i < numGoroutines; i++ {
			go sumBalanceGR(cds, sumBalances)
		}

		go func() {
			wg.Wait()
			close(sumBalances)
		}()

		sumBalance := <-sumBalances
		for sum := range sumBalances {
			if sumBalance != sum {
				t.Errorf("sumBalances are not all equal for cacheRecords: %d",
					c.cacheRecords)
				return
			}
		}
	}
}

/*************************
 *  Benchmarks
 *************************/

func BenchmarkOpenNextRead(b *testing.B) {
	benchmarks := []struct {
		cacheRecords int
	}{
		{0}, {100}, {1000}, {10000}, {100000},
	}
	filename := filepath.Join("fixtures", "debt.csv")
	hasHeader := true
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := dcsv.New(filename, hasHeader, ',', fieldNames)

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("cacherecords-%d", bm.cacheRecords), func(b *testing.B) {
			cds := New(ds, bm.cacheRecords)
			sumBalances := make([]int64, b.N)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sumBalances[i] = testhelpers.SumBalance(cds)
			}
			b.StopTimer()

			sumBalance := sumBalances[0]
			for _, s := range sumBalances {
				if s != sumBalance {
					b.Fatalf("sumBalances are not all equal")
					return
				}
			}
		})
	}
}

func BenchmarkOpenNextRead_goroutines(b *testing.B) {
	benchmarks := []struct {
		cacheRecords int
	}{
		{0}, {100}, {1000}, {10000}, {100000},
	}
	filename := filepath.Join("fixtures", "debt.csv")
	hasHeader := true
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := dcsv.New(filename, hasHeader, ',', fieldNames)
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("cacherecords-%d", bm.cacheRecords), func(b *testing.B) {
			cds := New(ds, bm.cacheRecords)
			sumBalances := make(chan int64, b.N)
			wg := sync.WaitGroup{}
			wg.Add(b.N)

			sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
				defer wg.Done()
				sum <- testhelpers.SumBalance(ds)
			}

			for i := 0; i < b.N; i++ {
				go sumBalanceGR(cds, sumBalances)
			}

			go func() {
				wg.Wait()
				close(sumBalances)
			}()

			b.ResetTimer()
			sumBalance := <-sumBalances
			for sum := range sumBalances {
				if sumBalance != sum {
					b.Fatal("sumBalances are not all equal")
					return
				}
			}
		})
	}
}

func BenchmarkNext(b *testing.B) {
	benchmarks := []struct {
		cacheRecords int
	}{
		{0}, {100}, {1000}, {10000}, {100000},
	}
	filename := filepath.Join("fixtures", "debt.csv")
	separator := ','
	hasHeader := true
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := dcsv.New(filename, hasHeader, separator, fieldNames)

	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("cacherecords-%d", bm.cacheRecords), func(b *testing.B) {
			cds := New(ds, bm.cacheRecords)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				conn, err := cds.Open()
				if err != nil {
					b.Errorf("Open() - filename: %s, err: %s", filename, err)
				}
				b.StartTimer()
				for conn.Next() {
				}
			}
		})
	}
}
