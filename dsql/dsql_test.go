// TODO: Test NULL handling of null values in database
// TODO: Test Next, Err for errors - using a mock database
// TODO: Test Open most fully for errors - using a mock database
// TODO: Generate an error from Next() by creating a database then closing it
//       after one run through for next() loop then run next() again

package dsql

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/internal"
	"github.com/lawrencewoodman/ddataset/internal/testhelpers"
	"github.com/lawrencewoodman/dlit"
)

func TestNew(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
	if _, ok := ds.(*DSQL); !ok {
		t.Errorf("New(...) want DSQL type, got type: %T", ds)
	}
}

func TestOpen(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
	conn, err := ds.Open()
	if err != nil {
		t.Errorf("Open() err: %s", err)
	}
	conn.Close()
}

func TestOpen_errors(t *testing.T) {
	cases := []struct {
		filename   string
		tableName  string
		fieldNames []string
		wantErr    error
	}{
		{filename: filepath.Join("fixtures", "missing.db"),
			tableName:  "userinfo",
			fieldNames: []string{"uid", "username", "dept", "started"},
			wantErr: fmt.Errorf("database doesn't exist: %s",
				filepath.Join("fixtures", "missing.db")),
		},
		{filename: filepath.Join("fixtures", "users.db"),
			tableName:  "missing",
			fieldNames: []string{"uid", "username", "dept", "started"},
			wantErr:    errors.New("table name doesn't exist: missing"),
		},
		{filename: filepath.Join("fixtures", "users.db"),
			tableName:  "userinfo",
			fieldNames: []string{"username", "dept", "started"},
			wantErr: errors.New(
				"number of field names doesn't match number of columns in table",
			),
		},
	}
	for _, c := range cases {
		ds := New(
			internal.NewSqlite3Handler(c.filename, c.tableName, 64),
			c.fieldNames,
		)
		if _, err := ds.Open(); err.Error() != c.wantErr.Error() {
			t.Errorf("Open() filename: %s, wantErr: %s, got err: %s",
				c.filename, c.wantErr, err)
		}
	}
}

func TestOpen_error_released(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(
		internal.NewSqlite3Handler(filename, tableName, 64),
		fieldNames,
	)
	ds.Release()
	if _, err := ds.Open(); err != ddataset.ErrReleased {
		t.Fatalf("ds.Open() err: %s", err)
	}
}

func TestRelease_error(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(
		internal.NewSqlite3Handler(filename, tableName, 64),
		fieldNames,
	)
	if err := ds.Release(); err != nil {
		t.Errorf("Release: %s", err)
	}

	if err := ds.Release(); err != ddataset.ErrReleased {
		t.Errorf("Release - got: %s, want: %s", err, ddataset.ErrReleased)
	}
}

func TestFields(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(
		internal.NewSqlite3Handler(filename, tableName, 64),
		fieldNames,
	)
	got := ds.Fields()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("Fields() - got: %s, want: %s", got, fieldNames)
	}
}

func TestNumRecords(t *testing.T) {
	cases := []struct {
		filename   string
		tableName  string
		fieldNames []string
		want       int64
	}{
		{filename: filepath.Join("fixtures", "users.db"),
			tableName:  "userinfo",
			fieldNames: []string{"uid", "username", "dept", "started"},
			want:       5,
		},
		{filename: filepath.Join("fixtures", "debt.db"),
			tableName: "people",
			fieldNames: []string{
				"name",
				"balance",
				"numCards",
				"martialStatus",
				"tertiaryEducated",
				"success",
			},
			want: 10000,
		},
	}
	for i, c := range cases {
		ds := New(
			internal.NewSqlite3Handler(c.filename, c.tableName, 64),
			c.fieldNames,
		)
		got := ds.NumRecords()
		if got != c.want {
			t.Errorf("(%d) Records - got: %d, want: %d", i, got, c.want)
		}
	}
}

func TestNext(t *testing.T) {
	wantNumRecords := 5
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(
		internal.NewSqlite3Handler(filename, tableName, 64),
		fieldNames,
	)
	conn, err := ds.Open()
	if err != nil {
		t.Errorf("Open() - filename: %s, err: %s", filename, err)
	}
	defer conn.Close()
	numRecords := 0
	for conn.Next() {
		numRecords++
	}
	if conn.Next() {
		t.Errorf("conn.Next() - Return true, despite having finished")
	}
	if numRecords != wantNumRecords {
		t.Errorf("conn.Next() - wantNumRecords: %d, gotNumRecords: %d",
			wantNumRecords, numRecords)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("conn.Err() - err: %s", err)
	}
}

func TestRead(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "name", "dpt", "startDate"}
	wantRecords := []ddataset.Record{
		ddataset.Record{
			"uid":       dlit.MustNew(1),
			"name":      dlit.MustNew("Fred Wilkins"),
			"dpt":       dlit.MustNew("Logistics"),
			"startDate": dlit.MustNew("2013-10-05 10:00:00"),
		},
		ddataset.Record{
			"uid":       dlit.MustNew(2),
			"name":      dlit.MustNew("Bob Field"),
			"dpt":       dlit.MustNew("Logistics"),
			"startDate": dlit.MustNew("2013-05-05 10:00:00"),
		},
		ddataset.Record{
			"uid":       dlit.MustNew(3),
			"name":      dlit.MustNew("Ned James"),
			"dpt":       dlit.MustNew("Shipping"),
			"startDate": dlit.MustNew("2012-05-05 10:00:00"),
		},
		ddataset.Record{
			"uid":       dlit.MustNew(4),
			"name":      dlit.MustNew("Mary Terence"),
			"dpt":       dlit.MustNew("Shipping"),
			"startDate": dlit.MustNew("2011-05-05 10:00:00"),
		},
		ddataset.Record{
			"uid":       dlit.MustNew(5),
			"name":      dlit.MustNew("George Eliot"),
			"dpt":       dlit.MustNew(""),
			"startDate": dlit.MustNew("2010-05-05 10:00:00"),
		},
	}

	ds := New(
		internal.NewSqlite3Handler(filename, tableName, 64),
		fieldNames,
	)
	conn, err := ds.Open()
	if err != nil {
		t.Errorf("Open() - filename: %s, err: %s", filename, err)
		return
	}
	defer conn.Close()

	for _, wantRecord := range wantRecords {
		if !conn.Next() {
			t.Errorf("Next() - return false early")
			return
		}
		record := conn.Read()
		if !testhelpers.MatchRecords(record, wantRecord) {
			t.Errorf("Read() got: %s, want: %s", record, wantRecord)
		}
		if err := conn.Err(); err != nil {
			t.Errorf("Err() err: %s", err)
		}
	}
}

func TestOpenNextRead_goroutines(t *testing.T) {
	var numGoroutines int
	filename := filepath.Join("fixtures", "debt.db")
	tableName := "people"
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
	if testing.Short() {
		numGoroutines = 10
	} else {
		numGoroutines = 500
	}
	sumBalances := make(chan int64, numGoroutines)
	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
		defer wg.Done()
		sum <- testhelpers.SumBalance(ds)
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

func BenchmarkOpenNextRead(b *testing.B) {
	filename := filepath.Join("fixtures", "debt.db")
	tableName := "people"
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
	sumBalances := make([]int64, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sumBalances[i] = testhelpers.SumBalance(ds)
	}
	b.StopTimer()

	sumBalance := sumBalances[0]
	for _, s := range sumBalances {
		if s != sumBalance {
			b.Error("sumBalances are not all equal")
			return
		}
	}

}

func BenchmarkOpenNextRead_goroutines(b *testing.B) {
	filename := filepath.Join("fixtures", "debt.db")
	tableName := "people"
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
	sumBalances := make(chan int64, b.N)
	wg := sync.WaitGroup{}
	wg.Add(b.N)

	sumBalanceGR := func(ds ddataset.Dataset, sum chan int64) {
		defer wg.Done()
		sum <- testhelpers.SumBalance(ds)
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
	filename := filepath.Join("fixtures", "debt.db")
	tableName := "people"
	fieldNames := []string{
		"name",
		"balance",
		"numCards",
		"martialStatus",
		"tertiaryEducated",
		"success",
	}
	ds := New(internal.NewSqlite3Handler(filename, tableName, 64), fieldNames)
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
