/*
 * A Go package to handles access to a an Sql database as a Dataset
 *
 * Copyright (C) 2016 Lawrence Woodman <lwoodman@vlifesystems.com>
 *
 * Licensed under an MIT licence.  Please see LICENCE.md for details.
 */

// Package dsql handles access to an SQL database as a Dataset
package dsql

import (
	"database/sql"
	"fmt"
	"github.com/lawrencewoodman/dlit"
	"github.com/vlifesystems/rulehunter/dataset"
)

type DSQL struct {
	dbInitFunc DBInitFunc
	fieldNames []string
}

type DSQLConn struct {
	dataset       *DSQL
	db            *sql.DB
	rows          *sql.Rows
	row           []sql.NullString
	rowPtrs       []interface{}
	currentRecord dataset.Record
	err           error
}

// The function to open the database and return a pointer to it
// and the rows to use for the Dataset
type DBInitFunc func() (*sql.DB, *sql.Rows, error)

func New(
	dbInitFunc DBInitFunc,
	fieldNames []string,
) dataset.Dataset {
	return &DSQL{
		dbInitFunc: dbInitFunc,
		fieldNames: fieldNames,
	}
}

func (s *DSQL) Open() (dataset.Conn, error) {
	db, rows, err := s.dbInitFunc()
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		db.Close()
		return nil, err
	}
	numColumns := len(columns)
	if err := checkTableValid(s.fieldNames, numColumns); err != nil {
		db.Close()
		return nil, err
	}
	row := make([]sql.NullString, numColumns)
	rowPtrs := make([]interface{}, numColumns)
	for i, _ := range s.fieldNames {
		rowPtrs[i] = &row[i]
	}

	return &DSQLConn{
		dataset:       s,
		db:            db,
		rows:          rows,
		row:           row,
		rowPtrs:       rowPtrs,
		currentRecord: make(dataset.Record, numColumns),
		err:           nil,
	}, nil
}

func (s *DSQL) GetFieldNames() []string {
	return s.fieldNames
}

func (sc *DSQLConn) Next() bool {
	if sc.err != nil {
		return false
	}
	if sc.rows.Next() {
		if err := sc.rows.Scan(sc.rowPtrs...); err != nil {
			sc.Close()
			sc.err = err
			return false
		}
		if err := sc.makeRowCurrentRecord(); err != nil {
			sc.Close()
			sc.err = err
			return false
		}
		return true
	}
	if err := sc.rows.Err(); err != nil {
		sc.Close()
		sc.err = err
		return false
	}
	return false
}

func (sc *DSQLConn) Err() error {
	return sc.err
}

func (sc *DSQLConn) Read() dataset.Record {
	return sc.currentRecord
}

func (sc *DSQLConn) Close() error {
	return sc.db.Close()
}

// Returns error if table doesn't exist in database
func CheckTableExists(driverName string, db *sql.DB, tableName string) error {
	var rowTableName string
	var rows *sql.Rows
	var err error
	tableNames := make([]string, 0)

	if driverName == "sqlite3" {
		rows, err = db.Query("select name from sqlite_master where type='table'")
	} else {
		rows, err = db.Query("show tables")
	}
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

func (sc *DSQLConn) makeRowCurrentRecord() error {
	var l *dlit.Literal
	var err error
	for i, v := range sc.row {
		if v.Valid {
			l = dlit.NewString(v.String)
		} else {
			l, err = dlit.New(nil)
			if err != nil {
				sc.Close()
				return err
			}
		}
		sc.currentRecord[sc.dataset.fieldNames[i]] = l
	}
	return nil
}

func checkTableValid(fieldNames []string, numColumns int) error {
	if len(fieldNames) < numColumns {
		return fmt.Errorf(
			"number of field names doesn't match number of columns in table",
		)
	}
	return nil
}

func inStringsSlice(needle string, haystack []string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
