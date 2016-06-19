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
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/dlit"
	"sync"
)

type DSQL struct {
	dbHandler  DBHandler
	openConn   int
	fieldNames []string
	sync.Mutex
}

type DSQLConn struct {
	dataset       *DSQL
	rows          *sql.Rows
	row           []sql.NullString
	rowPtrs       []interface{}
	currentRecord ddataset.Record
	err           error
}

// Interface to handle basic access to the Sql database
type DBHandler interface {
	Open() error
	Rows() (*sql.Rows, error)
	Close() error
}

func New(dbHandler DBHandler, fieldNames []string) ddataset.Dataset {
	return &DSQL{
		dbHandler:  dbHandler,
		openConn:   0,
		fieldNames: fieldNames,
	}
}

func (s *DSQL) Open() (ddataset.Conn, error) {
	s.Lock()
	if s.openConn == 0 {
		if err := s.dbHandler.Open(); err != nil {
			return nil, err
		}
	}
	s.openConn++
	s.Unlock()
	rows, err := s.dbHandler.Rows()
	if err != nil {
		s.close()
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		s.close()
		return nil, err
	}
	numColumns := len(columns)
	if err := checkTableValid(s.fieldNames, numColumns); err != nil {
		s.close()
		return nil, err
	}
	row := make([]sql.NullString, numColumns)
	rowPtrs := make([]interface{}, numColumns)
	for i, _ := range s.fieldNames {
		rowPtrs[i] = &row[i]
	}

	return &DSQLConn{
		dataset:       s,
		rows:          rows,
		row:           row,
		rowPtrs:       rowPtrs,
		currentRecord: make(ddataset.Record, numColumns),
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

func (sc *DSQLConn) Read() ddataset.Record {
	return sc.currentRecord
}

func (sc *DSQLConn) Close() error {
	return sc.dataset.close()
}

func (s *DSQL) close() error {
	s.Lock()
	defer s.Unlock()
	if s.openConn >= 1 {
		s.openConn--
		if s.openConn == 0 {
			return s.dbHandler.Close()
		}
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
