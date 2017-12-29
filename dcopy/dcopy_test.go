package dcopy

import (
	"encoding/csv"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/dcsv"
	"github.com/lawrencewoodman/ddataset/internal/testhelpers"
)

func TestNew(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
		},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
		},
	}

	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds, err := New(ds, "")
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		defer func() {
			if err := cds.Release(); err != nil {
				t.Error("Release: ", err)
			}
		}()

		for i := 0; i < 10; i++ {
			if err := testhelpers.CheckDatasetsEqual(ds, cds); err != nil {
				t.Fatalf("checkDatasetsEqual err: %s", err)
			}
		}
	}
}

func TestNew_tmpdir_specified(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	separator := ';'
	fields := []string{"age", "job", "marital", "education", "default", "balance",
		"housing", "loan", "contact", "day", "month", "duration", "campaign",
		"pdays", "previous", "poutcome", "y"}

	tmpDir, err := ioutil.TempDir("", "TestNew_tmpdirs")
	if err != nil {
		t.Fatalf("TempDir: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	ds := dcsv.New(filename, true, separator, fields)
	cds, err := New(ds, tmpDir)
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	defer func() {
		if err := cds.Release(); err != nil {
			t.Error("Release: ", err)
		}
	}()
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir: %s", err)
	}
	if len(files) == 1 &&
		files[0].IsDir() &&
		strings.HasPrefix(files[0].Name(), "dcopy") {
		dcopyFiles, err := ioutil.ReadDir(filepath.Join(tmpDir, files[0].Name()))
		if err != nil {
			t.Fatalf("ReadDir: %s", err)
		}
		if len(dcopyFiles) == 1 && dcopyFiles[0].Name() == "copy.csv" {
			return
		}
	}
	t.Errorf("New - can't find \"copy.csv\" in \"%s\"", tmpDir)
}

func TestNew_errors(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
		wantErr    error
	}{
		{filename: filepath.Join("fixtures", "invalid_numfields_at_102.csv"),
			separator:  ',',
			fieldNames: []string{"band", "score", "team", "points", "rating"},
			wantErr: &csv.ParseError{
				Line:   102,
				Column: 0,
				Err:    csv.ErrFieldCount,
			},
		},
		{filename: "missing.csv",
			separator:  ';',
			fieldNames: []string{"band", "score", "team", "points", "rating"},
			wantErr:    &os.PathError{"open", "missing.csv", syscall.ENOENT},
		},
		{filename: filepath.Join("fixtures", "bank.csv"),
			separator: ';',
			fieldNames: []string{"age", "job", "marital", "education", "default",
				"balance", "housing", "loan", "contact", "day", "month", "duration",
				"campaign", "pdays", "previous", "poutcome"},
			wantErr: errors.New("wrong number of field names for dataset"),
		},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, false, c.separator, c.fieldNames)
		if _, err := New(ds, ""); err == nil || err.Error() != c.wantErr.Error() {
			t.Fatalf("New: err: %s, want: %s", err, c.wantErr)
		}
	}
}

func TestOpen(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
		},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
		},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds, err := New(ds, "")
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		defer func() {
			if err := cds.Release(); err != nil {
				t.Error("Release: ", err)
			}
		}()
		dsConn, err := ds.Open()
		if err != nil {
			t.Fatalf("ds.Open() err: %s", err)
		}
		defer dsConn.Close()
		cdsConn, err := cds.Open()
		if err != nil {
			t.Fatalf("cds.Open() err: %s", err)
		}
		defer cdsConn.Close()

		if err := testhelpers.CheckDatasetConnsEqual(dsConn, cdsConn); err != nil {
			t.Errorf("checkDatasetConnsEqual err: %s", err)
		}
	}
}

func TestOpen_multiple_conns(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
		},
		{filepath.Join("fixtures", "debt.csv"), ',',
			[]string{"name", "balance", "numCards", "martialStatus",
				"tertiaryEducated", "success"},
		},
	}
	const numConns = 10

	for _, c := range cases {
		ds := dcsv.New(c.filename, true, c.separator, c.fieldNames)
		cds, err := New(ds, "")
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		defer func() {
			if err := cds.Release(); err != nil {
				t.Error("Release: ", err)
			}
		}()
		cdsConns := make([]ddataset.Conn, numConns)
		for i := range cdsConns {
			cdsConns[i], err = cds.Open()
			if err != nil {
				t.Fatalf("cds.Open() err: %s", err)
			}
			defer cdsConns[i].Close()
		}

		for _, c := range cdsConns {
			cdsConnRef, err := cds.Open()
			if err != nil {
				t.Fatalf("cds.Open() err: %s", err)
			}
			defer cdsConnRef.Close()
			if err := testhelpers.CheckDatasetConnsEqual(cdsConnRef, c); err != nil {
				t.Fatalf("checkDatasetsEqual err: %s", err)
			}
		}
	}
}

func TestOpen_error(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	separator := ';'
	fieldNames := []string{"age", "job", "marital", "education", "default",
		"balance", "housing", "loan", "contact", "day", "month", "duration",
		"campaign", "pdays", "previous", "poutcome", "y"}
	ds := dcsv.New(filename, true, separator, fieldNames)
	cds, err := New(ds, "")
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
	ds := dcsv.New(filename, true, ';', fieldNames)
	cds, err := New(ds, "")
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	defer func() {
		if err := cds.Release(); err != nil {
			t.Error("Release: ", err)
		}
	}()

	got := cds.Fields()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("Fields() - got: %s, want: %s", got, fieldNames)
	}
}

func TestRelease_error(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	fieldNames := []string{
		"age", "job", "marital", "education", "default", "balance",
		"housing", "loan", "contact", "day", "month", "duration", "campaign",
		"pdays", "previous", "poutcome", "y",
	}
	ds := dcsv.New(filename, true, ';', fieldNames)
	rds, err := New(ds, "")
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

func TestNext(t *testing.T) {
	cases := []struct {
		filename       string
		separator      rune
		hasHeader      bool
		fieldNames     []string
		wantNumRecords int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 9},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		cds, err := New(ds, "")
		if err != nil {
			t.Fatalf("New: %s", err)
		}
		defer func() {
			if err := cds.Release(); err != nil {
				t.Error("Release: ", err)
			}
		}()
		conn, err := cds.Open()
		if err != nil {
			t.Fatalf("Open() - filename: %s, err: %s", c.filename, err)
		}
		defer conn.Close()
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
	ds := dcsv.New(filename, hasHeader, ',', fieldNames)
	cds, err := New(ds, "")
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	defer func() {
		if err := cds.Release(); err != nil {
			t.Error("Release: ", err)
		}
	}()
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
	cds, err := New(ds, "")
	if err != nil {
		t.Fatalf("New: %s", err)
	}
	defer func() {
		if err := cds.Release(); err != nil {
			t.Error("Release: ", err)
		}
	}()

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
			t.Errorf("sumBalances are not all equal (%d != %d)", sumBalance, sum)
		}
	}
}

/*************************
 *  Benchmarks
 *************************/

func BenchmarkNew(b *testing.B) {
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

	cds, err := New(ds, "")
	if err != nil {
		b.Fatalf("New: %s", err)
	}
	defer cds.Release()
}

func BenchmarkOpenNextRead(b *testing.B) {
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

	cds, err := New(ds, "")
	if err != nil {
		b.Fatalf("New: %s", err)
	}
	defer cds.Release()
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
}

func BenchmarkOpenNextRead_goroutines(b *testing.B) {
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
	cds, err := New(ds, "")
	if err != nil {
		b.Fatalf("New: %s", err)
	}
	defer cds.Release()
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
}

func BenchmarkNext(b *testing.B) {
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

	cds, err := New(ds, "")
	if err != nil {
		b.Fatalf("New: %s", err)
	}
	defer cds.Release()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		conn, err := cds.Open()
		if err != nil {
			b.Errorf("Open() - filename: %s, err: %s", filename, err)
		}
		defer conn.Close()
		b.StartTimer()
		for conn.Next() {
		}
	}
}
