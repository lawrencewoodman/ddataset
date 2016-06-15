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
	"github.com/lawrencewoodman/dlit"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vlifesystems/rulehunter/dataset"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(makeDBInit(filename, tableName), fieldNames)
	if _, ok := ds.(*DSQL); !ok {
		t.Errorf("New(...) want DSQL type, got type: %T", ds)
	}
}

func TestOpen(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(makeDBInit(filename, tableName), fieldNames)
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
		ds := New(makeDBInit(c.filename, c.tableName), c.fieldNames)
		if _, err := ds.Open(); err.Error() != c.wantErr.Error() {
			t.Errorf("Open() filename: %s, wantErr: %s, got err: %s",
				c.filename, c.wantErr, err)
		}
	}
}

func TestCheckTableExists(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		t.Errorf("sql.Open(\"sqlite3\", \"%s\") - err: %s", filename, err)
	}
	if err := CheckTableExists("sqlite3", db, tableName); err != nil {
		t.Errorf("CheckTableExists() - filename: %s, tableName: %s, err: %s",
			filename, tableName, err)
	}
}
func TestCheckTableExists_errors(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "nothere"
	wantErr := errors.New("table name doesn't exist: nothere")
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		t.Errorf("sql.Open(\"sqlite3\", \"%s\") - err: %s", filename, err)
	}
	err = CheckTableExists("sqlite3", db, tableName)
	if err.Error() != wantErr.Error() {
		t.Errorf("CheckTableExists() - filename: %s, tableName: %s, wantErr: %s, gotErr: %s",
			filename, tableName, wantErr, err)
	}
}

func TestGetFieldNames(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "username", "dept", "started"}
	ds := New(makeDBInit(filename, tableName), fieldNames)
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
	ds := New(makeDBInit(filename, tableName), fieldNames)
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
}

func TestRead(t *testing.T) {
	filename := filepath.Join("fixtures", "users.db")
	tableName := "userinfo"
	fieldNames := []string{"uid", "name", "dpt", "startDate"}
	wantRecords := []dataset.Record{
		dataset.Record{
			"uid":       dlit.MustNew(1),
			"name":      dlit.MustNew("Fred Wilkins"),
			"dpt":       dlit.MustNew("Logistics"),
			"startDate": dlit.MustNew("2013-10-05 10:00:00"),
		},
		dataset.Record{
			"uid":       dlit.MustNew(2),
			"name":      dlit.MustNew("Bob Field"),
			"dpt":       dlit.MustNew("Logistics"),
			"startDate": dlit.MustNew("2013-05-05 10:00:00"),
		},
		dataset.Record{
			"uid":       dlit.MustNew(3),
			"name":      dlit.MustNew("Ned James"),
			"dpt":       dlit.MustNew("Shipping"),
			"startDate": dlit.MustNew("2012-05-05 10:00:00"),
		},
		dataset.Record{
			"uid":       dlit.MustNew(4),
			"name":      dlit.MustNew("Mary Terence"),
			"dpt":       dlit.MustNew("Shipping"),
			"startDate": dlit.MustNew("2011-05-05 10:00:00"),
		},
	}

	ds := New(makeDBInit(filename, tableName), fieldNames)
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

/*************************
 *  Benchmarks
 *************************/
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
	ds := New(makeDBInit(filename, tableName), fieldNames)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		conn, err := ds.Open()
		if err != nil {
			b.Errorf("Open() - filename: %s, err: %s", filename, err)
			return
		}
		//defer conn.Close()
		b.StartTimer()
		for conn.Next() {
		}
		b.StopTimer()
		conn.Close()
	}
}

/*************************
 *   Helper functions
 *************************/

func matchRecords(r1 dataset.Record, r2 dataset.Record) bool {
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

func dbOpenWithRows(
	filename string,
	tableName string,
) (*sql.DB, *sql.Rows, error) {
	if !fileExists(filename) {
		return nil, nil, fmt.Errorf("database doesn't exist: %s", filename)
	}
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, nil, err
	}
	if err := CheckTableExists("sqlite3", db, tableName); err != nil {
		db.Close()
		return nil, nil, err
	}
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM \"%s\"", tableName))
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, rows, nil
}

// Open an sqlite3 database for testing
func makeDBInit(filename string, tableName string) DBInitFunc {
	return func() (*sql.DB, *sql.Rows, error) {
		db, rows, err := dbOpenWithRows(filename, tableName)
		return db, rows, err
	}
}

func fileExists(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().IsRegular()
}
