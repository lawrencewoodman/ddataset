package dcsv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/dlit"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
)

func TestNew(t *testing.T) {
	cases := []struct {
		filename   string
		fieldNames []string
	}{
		{filepath.Join("fixtures", "bank.csv"),
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}},
	}
	for _, c := range cases {
		ds := New(c.filename, true, ';', c.fieldNames)
		if _, ok := ds.(*DCSV); !ok {
			t.Errorf("New(filename: %s...) want DCSV type, got type: %T", ds)
		}
	}
}

func TestOpen(t *testing.T) {
	cases := []struct {
		filename   string
		fieldNames []string
	}{
		{filepath.Join("fixtures", "bank.csv"),
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}},
	}
	for _, c := range cases {
		ds := New(c.filename, true, ';', c.fieldNames)
		if _, err := ds.Open(); err != nil {
			t.Errorf("Open() err: %s", err)
		}
	}
}

func TestOpen_errors(t *testing.T) {
	filename := "missing.csv"
	fieldNames := []string{"age", "occupation"}
	wantErr := &os.PathError{"open", "missing.csv", syscall.ENOENT}
	ds := New(filename, true, ';', fieldNames)
	_, err := ds.Open()
	if err := checkPathErrorMatch(err, wantErr); err != nil {
		t.Errorf("Open() - filename: %s - problem with error: %s",
			filename, err)
	}
}

func TestGetFieldNames(t *testing.T) {
	filename := filepath.Join("fixtures", "bank.csv")
	fieldNames := []string{
		"age", "job", "marital", "education", "default", "balance",
		"housing", "loan", "contact", "day", "month", "duration", "campaign",
		"pdays", "previous", "poutcome", "y",
	}
	ds := New(filename, true, ';', fieldNames)
	got := ds.GetFieldNames()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("GetFieldNames() - got: %s, want: %s", got, fieldNames)
	}
}

func TestRead(t *testing.T) {
	cases := []struct {
		filename        string
		hasHeader       bool
		fieldNames      []string
		wantNumColumns  int
		wantNumRows     int
		wantThirdRecord ddataset.Record
	}{
		{filepath.Join("fixtures", "bank.csv"), false,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			17, 10,
			ddataset.Record{
				"age":       dlit.MustNew(32),
				"job":       dlit.MustNew("entrepreneur"),
				"marital":   dlit.MustNew("married"),
				"education": dlit.MustNew("secondary"),
				"default":   dlit.MustNew("no"),
				"balance":   dlit.MustNew(2),
				"housing":   dlit.MustNew("yes"),
				"loan":      dlit.MustNew("yes"),
				"contact":   dlit.MustNew("unknown"),
				"day":       dlit.MustNew(5),
				"month":     dlit.MustNew("may"),
				"duration":  dlit.MustNew(76),
				"campaign":  dlit.MustNew(1),
				"pdays":     dlit.MustNew(-1),
				"previous":  dlit.MustNew(0),
				"poutcome":  dlit.MustNew("unknown"),
				"y":         dlit.MustNew("no")}},
		{filepath.Join("fixtures", "bank.csv"), true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			17, 9,
			ddataset.Record{
				"age":       dlit.MustNew(74),
				"job":       dlit.MustNew("blue-collar"),
				"marital":   dlit.MustNew("married"),
				"education": dlit.MustNew("unknown"),
				"default":   dlit.MustNew("no"),
				"balance":   dlit.MustNew(1506),
				"housing":   dlit.MustNew("yes"),
				"loan":      dlit.MustNew("no"),
				"contact":   dlit.MustNew("unknown"),
				"day":       dlit.MustNew(5),
				"month":     dlit.MustNew("may"),
				"duration":  dlit.MustNew(92),
				"campaign":  dlit.MustNew(1),
				"pdays":     dlit.MustNew(-1),
				"previous":  dlit.MustNew(0),
				"poutcome":  dlit.MustNew("unknown"),
				"y":         dlit.MustNew("no")}},
	}
	for _, c := range cases {
		ds := New(c.filename, c.hasHeader, ';', c.fieldNames)
		conn, err := ds.Open()
		if err != nil {
			t.Errorf("Open() - filename: %s, err: %s", c.filename, err)
		}
		gotNumRows := 0
		for conn.Next() {
			record := conn.Read()

			gotNumColumns := len(record)
			if gotNumColumns != c.wantNumColumns {
				t.Errorf("Read() - filename: %s, gotNumColumns: %d, want: %d",
					c.filename, gotNumColumns, c.wantNumColumns)
			}
			if gotNumRows == 2 && !matchRecords(record, c.wantThirdRecord) {
				t.Errorf("Read() - filename: %s, got: %s, want: %s",
					c.filename, record, c.wantThirdRecord)
			}
			gotNumRows++
		}
		if err := conn.Err(); err != nil {
			t.Errorf("Read() - filename: %s, err: %s", c.filename, err)
		}
		if gotNumRows != c.wantNumRows {
			t.Errorf("Read() - filename: %s, gotNumRows: %d, want: %d",
				c.filename, gotNumRows, c.wantNumRows)
		}
	}
}

func TestErr(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
		wantErr    error
	}{
		{filepath.Join("fixtures", "invalid_numfields_at_102.csv"), ',',
			[]string{"band", "score", "team", "points", "rating"},
			&csv.ParseError{102, 0, errors.New("wrong number of fields in line")}},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome"},
			ddataset.ErrWrongNumFields},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, nil},
	}
	for _, c := range cases {
		ds := New(c.filename, false, c.separator, c.fieldNames)
		conn, err := ds.Open()
		if err != nil {
			t.Errorf("Open() - filename: %s, err: %s", c.filename, err)
		}
		for conn.Next() {
			conn.Read()
		}
		if c.wantErr == nil {
			if conn.Err() != nil {
				t.Errorf("Err() - filename: %s, wantErr: %s, got error: %s",
					c.filename, c.wantErr, conn.Err())
			}
		} else {
			if conn.Err() == nil ||
				conn.Err().Error() != c.wantErr.Error() {
				t.Errorf("Err() - filename: %s, wantErr: %s, got error: %s",
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
		wantNumRecords int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 9},
	}
	for _, c := range cases {
		ds := New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		conn, err := ds.Open()
		if err != nil {
			t.Errorf("Open() - filename: %s, err: %s", c.filename, err)
		}
		numRecords := 0
		for conn.Next() {
			numRecords++
		}
		if conn.Next() {
			t.Errorf("conn.Next() - Return true, despite having finished")
		}
		if err := conn.Err(); err != nil {
			t.Errorf("conn.Err() - filename: %s, err: %s", c.filename, err)
		}
		if numRecords != c.wantNumRecords {
			t.Errorf("conn.Next() - filename: %s, wantNumRecords: %d, gotNumRecords: %d",
				c.filename, c.wantNumRecords, numRecords)
		}
	}
}

func TestNext_errors(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
		stopRow    int
		wantErr    error
	}{
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 2,
			ddataset.ErrConnClosed},
		{filepath.Join("fixtures", "bank.csv"), ';',
			[]string{"age", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome"}, 0,
			ddataset.ErrWrongNumFields},
		{filepath.Join("fixtures", "invalid_numfields_at_102.csv"), ',',
			[]string{"band", "score", "team", "points", "rating"}, 101,
			&csv.ParseError{102, 0, errors.New("wrong number of fields in line")}},
	}
	for _, c := range cases {
		ds := New(c.filename, false, c.separator, c.fieldNames)
		conn, err := ds.Open()
		if err != nil {
			t.Errorf("Open() - filename: %s, err: %s", c.filename, err)
		}
		i := 0
		for conn.Next() {
			if i == c.stopRow {
				if err := conn.Close(); err != nil {
					t.Errorf("conn.Close() - Err: %d", err)
				}
				break
			}
			i++
		}
		if i != c.stopRow {
			t.Errorf("conn.Next() - Not stopped at row: %d", c.stopRow)
		}
		if conn.Next() {
			t.Errorf("conn.Next() - Return true, despite connection being closed")
		}
		if !errorMatch(conn.Err(), c.wantErr) {
			t.Errorf("conn.Err() - err: %s, want err: %s", conn.Err(), c.wantErr)
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
	ds := New(filename, hasHeader, ',', fieldNames)
	if testing.Short() {
		numGoroutines = 10
	} else {
		numGoroutines = 1000
	}
	sumBalances := make(chan int64, numGoroutines)
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
		defer wg.Done()
		sum <- sumBalance(ds)
	}

	for i := 0; i < numGoroutines; i++ {
		go sumBalanceGR(ds, sumBalances)
	}

	go func() {
		wg.Wait()
		close(sumBalances)
	}()

	sumBalance := <-sumBalances
	for sum := range sumBalances {
		if sumBalance != sum {
			t.Error("sumBalances are not all equal")
			return
		}
	}
}

/*************************
 *  Benchmarks
 *************************/

func sumBalance(ds ddataset.Dataset) int64 {
	conn, err := ds.Open()
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	sum := int64(0)
	for conn.Next() {
		l := conn.Read()["balance"]
		v, ok := l.Int()
		if !ok {
			panic(fmt.Sprintf("balance can't be read as an int: %s", l))
		}
		sum += v
	}
	return sum
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
	ds := New(filename, hasHeader, ',', fieldNames)
	sumBalances := make([]int64, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sumBalances[i] = sumBalance(ds)
	}

	sumBalance := sumBalances[0]
	for _, s := range sumBalances {
		if s != sumBalance {
			b.Error("sumBalances are not all equal")
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
	ds := New(filename, hasHeader, ',', fieldNames)
	sumBalances := make(chan int64, b.N)
	wg := sync.WaitGroup{}
	wg.Add(b.N)

	sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
		defer wg.Done()
		sum <- sumBalance(ds)
	}

	for i := 0; i < b.N; i++ {
		go sumBalanceGR(ds, sumBalances)
	}

	go func() {
		wg.Wait()
		close(sumBalances)
	}()

	b.ResetTimer()
	sumBalance := <-sumBalances
	for sum := range sumBalances {
		if sumBalance != sum {
			b.Error("sumBalances are not all equal")
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
	ds := New(filename, hasHeader, separator, fieldNames)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		conn, err := ds.Open()
		if err != nil {
			b.Errorf("Open() - filename: %s, err: %s", filename, err)
		}
		b.StartTimer()
		for conn.Next() {
		}
	}
}

/*************************
 *   Helper functions
 *************************/

func matchRecords(r1 ddataset.Record, r2 ddataset.Record) bool {
	if len(r1) != len(r2) {
		return false
	}
	for fieldName, value := range r1 {
		if value.String() != r2[fieldName].String() {
			return false
		}
	}
	return true
}

func errorMatch(e1 error, e2 error) bool {
	if e1 == nil && e2 == nil {
		return true
	}
	if e1 == nil || e2 == nil {
		return false
	}
	if e1.Error() == e2.Error() {
		return true
	}
	return false
}

func checkPathErrorMatch(
	checkErr error,
	wantErr *os.PathError,
) error {
	perr, ok := checkErr.(*os.PathError)
	if !ok {
		return errors.New("error isn't a os.PathError")
	}
	if perr.Op != wantErr.Op {
		return fmt.Errorf("wanted perr.Op: %s, got: %s", perr.Op, wantErr.Op)
	}
	if filepath.Clean(perr.Path) != filepath.Clean(wantErr.Path) {
		return fmt.Errorf("wanted perr.Path: %s, got: %s", perr.Path, wantErr.Path)
	}
	if perr.Err != wantErr.Err {
		return fmt.Errorf("wanted perr.Err: %s, got: %s", perr.Err, wantErr.Err)
	}
	return nil
}
