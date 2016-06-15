/*
 * A Go package to describe a dynamic Dataset interface
 *
 * Copyright (C) 2016 Lawrence Woodman <lwoodman@vlifesystems.com>
 *
 * Licensed under an MIT licence.  Please see LICENCE.md for details.
 */

// Package ddataset describes a dynamic Dataset interface
package ddataset

import (
	"errors"
	"github.com/lawrencewoodman/dlit"
)

var ErrConnClosed = errors.New("connection has been closed")
var ErrWrongNumFields = errors.New("wrong number of field names for dataset")

type Dataset interface {
	Open() (Conn, error)
	GetFieldNames() []string
}

type Conn interface {
	Next() bool
	Err() error
	Read() Record
	Close() error
}

type Record map[string]*dlit.Literal
