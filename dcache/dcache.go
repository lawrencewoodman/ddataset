/*
 * A Go package to cache access to a Dataset
 *
 * Copyright (C) 2017 Lawrence Woodman <lwoodman@vlifesystems.com>
 *
 * Licensed under an MIT licence.  Please see LICENCE.md for details.
 */

// Package dcache caches a Dataset
package dcache

import (
	"github.com/lawrencewoodman/ddataset"
	"github.com/lawrencewoodman/ddataset/internal"
)

// DCache represents a cached Dataset
type DCache struct {
	dataset      ddataset.Dataset
	cache        []ddataset.Record
	maxCacheRows int64
	allCached    bool
	cachedRows   int64
	isReleased   bool
}

// DCacheConn represents a connection to a DCache Dataset
type DCacheConn struct {
	dataset   *DCache
	conn      ddataset.Conn
	recordNum int64
	err       error
}

// New creates a new DCache Dataset which will store up to maxCacheRows
// of another Dataset in memory.  There is only a speed increase if
// maxCacheRows is at least as big as the number of rows in the Dataset
// you want to cache.  However, if it is less the access time is about
// the same.
func New(
	dataset ddataset.Dataset,
	maxCacheRows int64,
) (ddataset.Dataset, error) {
	conn, err := dataset.Open()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cache := make([]ddataset.Record, maxCacheRows)
	cachedRows := int64(0)
	for cachedRows < maxCacheRows && conn.Next() {
		cache[cachedRows] = conn.Read().Clone()
		cachedRows++
	}
	moreRecords := conn.Next()

	if err := conn.Err(); err != nil {
		return nil, err
	}

	allCached := false
	if cachedRows <= maxCacheRows && !moreRecords {
		allCached = true
	}

	return &DCache{
		dataset:      dataset,
		cache:        cache,
		cachedRows:   cachedRows,
		maxCacheRows: maxCacheRows,
		allCached:    allCached,
		isReleased:   false,
	}, nil
}

// Open creates a connection to the Dataset
func (c *DCache) Open() (ddataset.Conn, error) {
	if c.isReleased {
		return nil, ddataset.ErrReleased
	}
	if c.allCached {
		return &DCacheConn{
			dataset:   c,
			conn:      nil,
			recordNum: -1,
			err:       nil,
		}, nil
	}
	conn, err := c.dataset.Open()
	if err != nil {
		return nil, err
	}
	return &DCacheConn{
		dataset:   c,
		conn:      conn,
		recordNum: -1,
		err:       nil,
	}, nil
}

// Fields returns the field names used by the Dataset
func (c *DCache) Fields() []string {
	if c.isReleased {
		return []string{}
	}
	return c.dataset.Fields()
}

// NumRecords returns the number of records in the Dataset.  If there is
// a problem getting the number of records it returns -1.  NOTE: The returned
// value can change if the underlying Dataset changes.
func (d *DCache) NumRecords() int64 {
	if d.allCached {
		return d.cachedRows
	}
	return internal.CountNumRecords(d)
}

// Release releases any resources associated with the Dataset d,
// rendering it unusable in the future.
func (d *DCache) Release() error {
	if !d.isReleased {
		d.cache = nil
		d.allCached = false
		d.maxCacheRows = int64(0)
		d.isReleased = true
		return nil
	}
	return ddataset.ErrReleased
}

// Next returns whether there is a Record to be Read
func (cc *DCacheConn) Next() bool {
	if cc.dataset.allCached {
		if (cc.recordNum + 1) < cc.dataset.cachedRows {
			cc.recordNum++
			return true
		}
		return false
	}
	if cc.conn.Err() != nil {
		return false
	}

	isRecord := cc.conn.Next()
	if isRecord {
		cc.recordNum++
	}

	return isRecord
}

// Err returns any errors from the connection
func (cc *DCacheConn) Err() error {
	if cc.dataset.allCached {
		return nil
	}
	return cc.conn.Err()
}

// Read returns the current Record
func (cc *DCacheConn) Read() ddataset.Record {
	if cc.recordNum < cc.dataset.cachedRows {
		return cc.dataset.cache[cc.recordNum]
	}

	return cc.conn.Read()
}

// Close closes the connection
func (cc *DCacheConn) Close() error {
	if cc.dataset.allCached {
		return nil
	}
	return cc.conn.Close()
}
