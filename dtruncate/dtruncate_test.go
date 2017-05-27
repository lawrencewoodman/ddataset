package dtruncate

import (
	"encoding/csv"
	"errors"
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/dcsv"
	"github.com/lawrencewoodman/ddataset/internal/testhelpers"
	"github.com/lawrencewoodman/dlit"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

func TestOpen(t *testing.T) {
	cases := []struct {
		filename   string
		fieldNames []string
		numRecords int
	}{
		{filepath.Join("fixtures", "bank.csv"),
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"},
			10},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, false, ';', c.fieldNames)
		rds := New(ds, c.numRecords)
		if _, err := rds.Open(); err != nil {
			t.Fatalf("Open() err: %s", err)
		}
	}
}

func TestOpen_errors(t *testing.T) {
	filename := "missing.csv"
	fieldNames := []string{"age", "occupation"}
	numRecords := 10
	wantErr := &os.PathError{"open", "missing.csv", syscall.ENOENT}
	ds := dcsv.New(filename, false, ';', fieldNames)
	rds := New(ds, numRecords)
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
	numRecords := 3
	ds := dcsv.New(filename, false, ';', fieldNames)
	rds := New(ds, numRecords)

	got := rds.Fields()
	if !reflect.DeepEqual(got, fieldNames) {
		t.Errorf("Fields() - got: %s, want: %s", got, fieldNames)
	}
}

func TestRead(t *testing.T) {
	cases := []struct {
		filename        string
		hasHeader       bool
		fieldNames      []string
		wantNumColumns  int
		wantNumRecords  int
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
		ds := dcsv.New(c.filename, c.hasHeader, ';', c.fieldNames)
		rds := New(ds, c.wantNumRecords)
		conn, err := rds.Open()
		if err != nil {
			t.Fatalf("Open() - filename: %s, err: %s", c.filename, err)
		}
		gotNumRecords := 0
		for conn.Next() {
			gotNumRecords++
			record := conn.Read()

			gotNumColumns := len(record)
			if gotNumColumns != c.wantNumColumns {
				t.Errorf("Read() - filename: %s, gotNumColumns: %d, want: %d",
					c.filename, gotNumColumns, c.wantNumColumns)
			}
			if gotNumRecords == 3 &&
				!testhelpers.MatchRecords(record, c.wantThirdRecord) {
				t.Errorf("Read() - filename: %s, got: %s, want: %s",
					c.filename, record, c.wantThirdRecord)
			}
		}
		if err := conn.Err(); err != nil {
			t.Errorf("Read() - filename: %s, err: %s", c.filename, err)
		}
		if gotNumRecords != c.wantNumRecords {
			t.Errorf("Read() - filename: %s, gotNumRecords: %d, want: %d",
				c.filename, gotNumRecords, c.wantNumRecords)
		}
	}
}

func TestErr(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		fieldNames []string
		numRecords int
		wantErr    error
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
		rds := New(ds, c.numRecords)
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
		wantNumRecords int
	}{
		{filepath.Join("fixtures", "bank.csv"), ';', true,
			[]string{"age", "job", "marital", "education", "default", "balance",
				"housing", "loan", "contact", "day", "month", "duration", "campaign",
				"pdays", "previous", "poutcome", "y"}, 4},
		{filepath.Join("fixtures", "invalid_numfields_at_102.csv"),
			',', false,
			[]string{"band", "score", "team", "points", "rating"}, 50},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		rds := New(ds, c.wantNumRecords)
		conn, err := rds.Open()
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

func TestNext_errors(t *testing.T) {
	cases := []struct {
		filename   string
		separator  rune
		hasHeader  bool
		fieldNames []string
		stopRow    int
		numRecords int
		wantErr    error
	}{
		{filename: filepath.Join("fixtures", "bank.csv"),
			separator: ';',
			hasHeader: true,
			fieldNames: []string{"age", "job", "marital", "education", "default",
				"balance", "housing", "loan", "contact", "day", "month", "duration",
				"campaign", "pdays", "previous", "poutcome", "y"},
			stopRow:    2,
			numRecords: 4,
			wantErr:    errors.New("connection has been closed")},
	}
	for _, c := range cases {
		ds := dcsv.New(c.filename, c.hasHeader, c.separator, c.fieldNames)
		rds := New(ds, c.numRecords)
		conn, err := rds.Open()
		if err != nil {
			t.Fatalf("Open() - filename: %s, err: %s", c.filename, err)
		}
		recordNum := 0
		for conn.Next() {
			if recordNum == c.stopRow {
				if err := conn.Close(); err != nil {
					t.Errorf("conn.Close() - Err: %d", err)
				}
				break
			}
			recordNum++
		}
		if recordNum != c.stopRow {
			t.Errorf("conn.Next() - Not stopped at row: %d", c.stopRow)
		}
		if conn.Next() {
			t.Errorf("conn.Next() - Return true, despite reducedDataset being closed")
		}
		if conn.Err() == nil || conn.Err().Error() != c.wantErr.Error() {
			t.Errorf("conn.Err() - err: %s, want err: %s", conn.Err(), c.wantErr)
		}
	}
}
