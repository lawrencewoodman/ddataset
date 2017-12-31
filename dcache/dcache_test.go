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
		cds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("New: %s", err)
		}

		for i := 0; i < 10; i++ {
			if err := testhelpers.CheckDatasetsEqual(ds, cds); err != nil {
				t.Fatalf("checkDatasetsEqual err: %s", err)
			}
		}
	}
}

func TestNew_errors(t *testing.T) {
	cases := []struct {
		filename     string
		separator    rune
		fieldNames   []string
		maxCacheRows int
		wantErr      error
	}{
		{filename: filepath.Join("fixtures", "invalid_numfields_at_102.csv"),
			separator:    ',',
			fieldNames:   []string{"band", "score", "team", "points", "rating"},
			maxCacheRows: 105,
			wantErr: &csv.ParseError{
				Line:   102,
				Column: 0,
				Err:    csv.ErrFieldCount,
			}},
		{filename: "missing.csv",
			separator:    ',',
			fieldNames:   []string{"age", "occupation"},
			maxCacheRows: 100,
			wantErr:      &os.PathError{"open", "missing.csv", syscall.ENOENT},
		},

		{filename: filepath.Join("fixtures", "bank.csv"),
			separator: ';',
			fieldNames: []string{"age", "job", "marital", "education", "default",
				"balance", "housing", "loan", "contact", "day", "month", "duration",
				"campaign", "pdays", "previous", "poutcome"},
			maxCacheRows: 4,
			wantErr:      errors.New("wrong number of field names for dataset"),
		},
	}

	for _, c := range cases {
		ds := dcsv.New(c.filename, false, c.separator, c.fieldNames)
		_, err := New(ds, c.maxCacheRows)
		if err == nil || err.Error() != c.wantErr.Error() {
			t.Errorf("New - filename: %s, got: %s, want: %s", c.filename, err, c.wantErr)
		}
	}
}

func TestRelease_error(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	fieldNames := []string{
		"age", "job", "marital", "education", "default", "balance",
		"housing", "loan", "contact", "day", "month", "duration", "campaign",
		"pdays", "previous", "poutcome", "y",
	}
	maxCacheRows := 100
	ds := dcsv.New(filename, true, ';', fieldNames)
	rds, err := New(ds, maxCacheRows)
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	if err := rds.Release(); err != nil {
		t.Errorf("Release: %s", err)
	}

	if err := rds.Release(); err != ddataset.ErrReleased {
		t.Errorf("Release - got: %s, want: %s", err, ddataset.ErrReleased)
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
	for i, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		dsConn, err := ds.Open()
		if err != nil {
			t.Fatalf("ds.Open() err: %s", err)
		}
		cdsConn, err := cds.Open()
		if err != nil {
			t.Fatalf("cds.Open() err: %s", err)
		}

		if err := testhelpers.CheckDatasetConnsEqual(dsConn, cdsConn); err != nil {
			t.Errorf("(%d) checkDatasetConnsEqual err: %s", i, err)
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
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
			10000},
	}
	const numConns = 10

	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("New: %s", err)
		}
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

func TestOpen_error_released(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	separator := ';'
	fieldNames := []string{"age", "job", "marital", "education", "default",
		"balance", "housing", "loan", "contact", "day", "month", "duration",
		"campaign", "pdays", "previous", "poutcome", "y"}
	maxCacheRows := 100
	ds := dcsv.New(filename, true, separator, fieldNames)
	cds, err := New(ds, maxCacheRows)
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	cds.Release()
	if _, err := cds.Open(); err != ddataset.ErrReleased {
		t.Fatalf("cds.Open() err: %s", err)
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
	rds, err := New(ds, maxCacheRows)
	if err != nil {
		t.Fatalf("New: %s", err)
	}

	got := rds.Fields()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("Fields() - got: %s, want: %s", got, fieldNames)
	}
}

func TestNumRecords(t *testing.T) {
	cases := []struct {
		filename     string
		hasHeader    bool
		separator    rune
		fieldNames   []string
		maxCacheRows int
		want         int64
	}{
		{filename: filepath.Join("fixtures", "bank.csv"),
			hasHeader: true,
			separator: ';',
			fieldNames: []string{
				"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y",
			},
			maxCacheRows: 12,
			want:         9,
		},
		{filename: filepath.Join("fixtures", "bank.csv"),
			hasHeader: true,
			separator: ';',
			fieldNames: []string{
				"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y",
			},
			maxCacheRows: 9,
			want:         9,
		},
		{filename: filepath.Join("fixtures", "bank.csv"),
			hasHeader: true,
			separator: ';',
			fieldNames: []string{
				"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y",
			},
			maxCacheRows: 8,
			want:         9,
		},
		{filename: filepath.Join("fixtures", "bank.csv"),
			hasHeader: true,
			separator: ';',
			fieldNames: []string{
				"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y",
			},
			maxCacheRows: 0,
			want:         9,
		},
	}
	for i, c := range cases {
		ds := dcsv.New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		cds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("(%d) New: %s", i, err)
		}
		got := cds.NumRecords()
		if got != c.want {
			t.Errorf("(%d) Records - got: %d, want: %d", i, got, c.want)
		}
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
			100,
			&csv.ParseError{
				Line:   102,
				Column: 0,
				Err:    csv.ErrFieldCount,
			}},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			20, nil},
	}
	for i, c := range cases {
		ds := dcsv.New(c.filename, false, c.separator, c.fieldNames)
		rds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("(%d) New: %s", i, err)
		}
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
		cds, err := New(ds, c.maxCacheRows)
		if err != nil {
			t.Fatalf("New: %s", err)
		}
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
	cds, err := New(ds, cacheRecords)
	if err != nil {
		t.Fatalf("New: %s", err)
	}
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
		cds, err := New(ds, c.cacheRecords)
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		sumBalances := make(chan int64, numGoroutines)
		wg := sync.WaitGroup{}
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				sumBalances <- testhelpers.SumBalance(cds)
			}()

		}

		go func() {
			wg.Wait()
			close(sumBalances)
		}()

		sumBalance := <-sumBalances
		i := 0
		for sum := range sumBalances {
			if sumBalance != sum {
				t.Errorf("sumBalances are not all equal for cacheRecords: %d",
					c.cacheRecords)
				return
			}
			i++
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
			cds, err := New(ds, bm.cacheRecords)
			if err != nil {
				b.Fatalf("New: %s", err)
			}
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
			cds, err := New(ds, bm.cacheRecords)
			if err != nil {
				b.Fatalf("New: %s", err)
			}
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
			cds, err := New(ds, bm.cacheRecords)
			if err != nil {
				b.Fatalf("New: %s", err)
			}
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
