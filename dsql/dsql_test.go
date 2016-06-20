// TODO: Test NULL handling of null values in database
// TODO: Test Next, Err for errors - using a mock database
// TODO: Test Open most fully for errors - using a mock database
// TODO: Generate an error from Next() by creating a database then closing it
//       after one run through for next() loop then run next() again
package dsql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/dlit"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(newDBHandler(filename, tableName), fieldNames)
	if _, ok := ds.(*DSQL); !ok {
		t.Errorf("New(...) want DSQL type, got type: %T", ds)
	}
}

func TestOpen(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(newDBHandler(filename, tableName), fieldNames)
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
		ds := New(newDBHandler(c.filename, c.tableName), c.fieldNames)
		if _, err := ds.Open(); err.Error() != c.wantErr.Error() {
			t.Errorf("Open() filename: %s, wantErr: %s, got err: %s",
				c.filename, c.wantErr, err)
		}
	}
}

func TestGetFieldNames(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(newDBHandler(filename, tableName), fieldNames)
	got := ds.GetFieldNames()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("GetFieldNames() - got: %s, want: %s", got, fieldNames)
	}
}

func TestNext(t *testing.T) {
	wantNumRecords := 4
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(newDBHandler(filename, tableName), fieldNames)
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
	}

	ds := New(newDBHandler(filename, tableName), fieldNames)
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
		if !matchRecords(record, wantRecord) {
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
	ds := New(newDBHandler(filename, tableName), fieldNames)
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
	ds := New(newDBHandler(filename, tableName), fieldNames)
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
	ds := New(newDBHandler(filename, tableName), fieldNames)
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
	ds := New(newDBHandler(filename, tableName), fieldNames)
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

type dbHandler struct {
	filename  string
	tableName string
	db        *sql.DB
}

func newDBHandler(filename, tableName string) *dbHandler {
	return &dbHandler{
		filename:  filename,
		tableName: tableName,
		db:        nil,
	}
}

func (d *dbHandler) Open() error {
	if !fileExists(d.filename) {
		return fmt.Errorf("database doesn't exist: %s", d.filename)
	}
	db, err := sql.Open("sqlite3", d.filename)
	d.db = db
	return err
}

func (d *dbHandler) Close() error {
	return d.db.Close()
}

func (d *dbHandler) Rows() (*sql.Rows, error) {
	if err := d.checkTableExists(d.tableName); err != nil {
		d.Close()
		return nil, err
	}
	rows, err := d.db.Query(fmt.Sprintf("SELECT * FROM \"%s\"", d.tableName))
	if err != nil {
		d.Close()
	}
	return rows, err
}

// Returns error if table doesn't exist in database
func (d *dbHandler) checkTableExists(tableName string) error {
	var rowTableName string
	var rows *sql.Rows
	var err error
	tableNames := make([]string, 0)

	rows, err = d.db.Query("select name from sqlite_master where type='table'")
	if err != nil {
		return err
	}

	for rows.Next() {
		if err := rows.Scan(&rowTableName); err != nil {
			return err
		}
		tableNames = append(tableNames, rowTableName)
	}

	if !inStringsSlice(tableName, tableNames) {
		return fmt.Errorf("table name doesn't exist: %s", tableName)
	}
	return nil
}

func fileExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().IsRegular()
}
