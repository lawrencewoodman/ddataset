/*
 * A Go package to copy a Dataset
 *
 * Copyright (C) 2017 Lawrence Woodman <lwoodman@vlifesystems.com>
 *
 * Licensed under an MIT licence.  Please see LICENCE.md for details.
 */

// Package dcopy copies a Dataset so that you can work consistently on
// the same Dataset.  This is important where a database is likely to be
// updated while you are working on it.  The copy of the database is stored
// in an sqlite3 database located in a temporary directory.
package dcopy

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/dsql"
	"github.com/lawrencewoodman/ddataset/internal"
	_ "github.com/mattn/go-sqlite3"
)

// DCopy represents a copy of a Dataset
type DCopy struct {
	dataset ddataset.Dataset
	tmpDir  string
}

// DCopyConn represents a connection to a DCopy Dataset
type DCopyConn struct {
	conn ddataset.Conn
	err  error
}

// New creates a new DCopy Dataset which will be a copy of the Dataset
// supplied at the time it is run.
func New(dataset ddataset.Dataset) (*DCopy, error) {
	const numRecordsPerTX = 500
	if len(dataset.Fields()) < 1 {
		return nil, fmt.Errorf("Dataset must have at least one field to copy")
	}
	tmpDir, err := ioutil.TempDir("", "dcopy")
	if err != nil {
		return nil, err
	}
	copyDBFilename := filepath.Join(tmpDir, "copy.db")
	copyDB, err := sql.Open("sqlite3", copyDBFilename)
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	sqlPragmaStmt := "PRAGMA SYNCHRONOUS = OFF;\n" +
		"PRAGMA JOURNAL_MODE = OFF;"
	if _, err := copyDB.Exec(sqlPragmaStmt); err != nil {
		return nil, err
	}

	// field names are quoted to prevent clashes with sqlite keywords
	sqlCreateStmt :=
		fmt.Sprintf("CREATE TABLE dataset ('%s' TEXT", dataset.Fields()[0])

	// TODO: Restrict field names to names that are valid in sqlite3 statement
	for _, fieldName := range dataset.Fields()[1:] {
		sqlCreateStmt += fmt.Sprintf(", '%s' TEXT", fieldName)
	}
	sqlCreateStmt += ");"
	if _, err = copyDB.Exec(sqlCreateStmt); err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}

	conn, err := dataset.Open()
	if err != nil {
		os.RemoveAll(tmpDir)
		return nil, err
	}
	defer conn.Close()

	for {
		records, err := getRecords(conn, numRecordsPerTX)
		if err != nil {
			os.RemoveAll(tmpDir)
			return nil, err
		}
		if len(records) == 0 {
			break
		}
		tx, err := copyDB.Begin()
		if err != nil {
			return nil, err
		}

		sqlInsertStmt := "INSERT INTO dataset VALUES(?"
		for i := 0; i < len(dataset.Fields())-1; i++ {
			sqlInsertStmt += ", ?"
		}
		sqlInsertStmt += ")"

		stmt, err := tx.Prepare(sqlInsertStmt)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()

		for _, record := range records {
			sqlValues := make([]interface{}, len(dataset.Fields()))
			for i, f := range dataset.Fields() {
				sqlValues[i] = record[f].String()
			}
			if _, err := stmt.Exec(sqlValues...); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &DCopy{
		dataset: dsql.New(
			internal.NewSqlite3Handler(copyDBFilename, "dataset", ""),
			dataset.Fields(),
		),
		tmpDir: tmpDir,
	}, nil
}

// Open creates a connection to the Dataset
func (c *DCopy) Open() (ddataset.Conn, error) {
	conn, err := c.dataset.Open()
	if err != nil {
		return nil, err
	}
	return &DCopyConn{
		conn: conn,
		err:  nil,
	}, nil
}

// Fields returns the field names used by the Dataset
func (c *DCopy) Fields() []string {
	return c.dataset.Fields()
}

// Delete deletes the copy of the database
func (c *DCopy) Delete() {
	os.RemoveAll(c.tmpDir)
}

// Next returns whether there is a Record to be Read
func (cc *DCopyConn) Next() bool {
	return cc.conn.Next()
}

// Err returns any errors from the connection
func (cc *DCopyConn) Err() error {
	return cc.conn.Err()
}

// Read returns the current Record
func (cc *DCopyConn) Read() ddataset.Record {
	return cc.conn.Read()
}

// Close closes the connection and deletes the copy
func (cc *DCopyConn) Close() error {
	return cc.conn.Close()
}

func getRecords(conn ddataset.Conn, num int) ([]ddataset.Record, error) {
	n := 0
	records := []ddataset.Record{}
	for n < num && conn.Next() {
		record := conn.Read().Clone()
		n++
		records = append(records, record)
	}
	return records, conn.Err()
}
