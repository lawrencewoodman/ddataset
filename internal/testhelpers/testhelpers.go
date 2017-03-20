// Package testhelpers contains routines to help test ddataset
package testhelpers

import (
	"errors"
	"fmt"
	"github.com/lawrencewoodman/ddataset"
	"os"
	"path/filepath"
)

func CheckDatasetsEqual(d1, d2 ddataset.Dataset) error {
	c1, err := d1.Open()
	if err != nil {
		panic(err)
	}
	defer c1.Close()
	c2, err := d2.Open()
	if err != nil {
		panic(err)
	}
	defer c2.Close()
	return CheckDatasetConnsEqual(c1, c2)
}

func CheckDatasetConnsEqual(c1, c2 ddataset.Conn) error {
	for {
		c1Next := c1.Next()
		c2Next := c2.Next()
		if c1Next != c2Next {
			return errors.New("datasets don't finish at same point")
		}
		if !c1Next {
			break
		}

		c1Record := c1.Read()
		c2Record := c2.Read()
		if !MatchRecords(c1Record, c2Record) {
			return errors.New("datasets don't match")
		}
	}
	if c1.Err() != c2.Err() {
		return errors.New("datasets final error doesn't match")
	}
	return nil
}
func MatchRecords(r1 ddataset.Record, r2 ddataset.Record) bool {
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

func SumBalance(ds ddataset.Dataset) int64 {
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

func ErrorMatch(e1 error, e2 error) bool {
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

func CheckPathErrorMatch(checkErr error, wantErr *os.PathError) error {
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
