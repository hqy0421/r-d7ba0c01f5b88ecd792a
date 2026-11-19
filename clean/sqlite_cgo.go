// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//go:build cgo && linux
// +build cgo,linux

package clean

/*
#cgo LDFLAGS: -l:libsqlite3.so.0

#include <stdlib.h>

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef void (*sqlite3_destructor_type)(void*);

extern int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags, const char *zVfs);
extern int sqlite3_close_v2(sqlite3 *db);
extern const char *sqlite3_errmsg(sqlite3 *db);
extern int sqlite3_extended_errcode(sqlite3 *db);
extern int sqlite3_busy_timeout(sqlite3 *db, int ms);
extern int sqlite3_exec(sqlite3 *db, const char *sql, void *cb, void *arg, char **errmsg);
extern void sqlite3_free(void *p);
extern int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, const char **pzTail);
extern int sqlite3_step(sqlite3_stmt *stmt);
extern int sqlite3_finalize(sqlite3_stmt *stmt);
extern int sqlite3_column_type(sqlite3_stmt *stmt, int i);
extern long long sqlite3_column_int64(sqlite3_stmt *stmt, int i);
extern const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int i);
extern const void *sqlite3_column_blob(sqlite3_stmt *stmt, int i);
extern int sqlite3_column_bytes(sqlite3_stmt *stmt, int i);
extern int sqlite3_bind_null(sqlite3_stmt *stmt, int i);
extern int sqlite3_bind_int64(sqlite3_stmt *stmt, int i, long long v);
extern int sqlite3_bind_double(sqlite3_stmt *stmt, int i, double v);
extern int sqlite3_bind_text(sqlite3_stmt *stmt, int i, const char *p, int n, sqlite3_destructor_type d);
extern int sqlite3_bind_blob(sqlite3_stmt *stmt, int i, const void *p, int n, sqlite3_destructor_type d);
extern const char *sqlite3_libversion(void);

#define SQLITE_TRANSIENT ((sqlite3_destructor_type)-1)

static int goc_bind_text(sqlite3_stmt *stmt, int i, const char *p, int n) {
	return sqlite3_bind_text(stmt, i, p, n, SQLITE_TRANSIENT);
}

static int goc_bind_blob(sqlite3_stmt *stmt, int i, const void *p, int n) {
	return sqlite3_bind_blob(stmt, i, p, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"math"
	"unsafe"
)

const (
	sqliteOK         = 0
	sqliteRow        = 100
	sqliteDone       = 101
	sqliteOpenRW     = 0x00000002
	sqliteOpenCreate = 0x00000004
	sqliteOpenFull   = 0x00010000
)

// Available reports whether this build can talk to SQLite. The cleaning store
// needs cgo plus the system SQLite library.
func Available() bool { return true }

// sqliteError describes a failing SQLite call.
type sqliteError struct {
	Code int
	Msg  string
}

func (e *sqliteError) Error() string {
	return fmt.Sprintf("clean: sqlite error %d: %s", e.Code, e.Msg)
}

var _ sqlDriver = (*sqliteConn)(nil)

// sqliteConn is a minimal, single-connection binding to the system SQLite
// library. The store serialises all access with its own mutex, so one
// connection is enough.
type sqliteConn struct {
	db *C.sqlite3
}

func (c *sqliteConn) lastError(prefix string) error {
	if c == nil || c.db == nil {
		return &sqliteError{Code: -1, Msg: prefix + ": database is closed"}
	}
	return &sqliteError{
		Code: int(C.sqlite3_extended_errcode(c.db)),
		Msg:  prefix + ": " + C.GoString(C.sqlite3_errmsg(c.db)),
	}
}

// openSQLite opens (or creates) a database file and applies the pragmas that
// make a batch commit durable and atomic.
func openSQLite(path string, busyTimeoutMS int) (*sqliteConn, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var db *C.sqlite3
	rc := C.sqlite3_open_v2(cpath, &db, C.int(sqliteOpenRW|sqliteOpenCreate|sqliteOpenFull), nil) //nolint:gocritic // cgo call with a C string argument
	if rc != sqliteOK {
		msg := "unable to open database"
		if db != nil {
			msg = C.GoString(C.sqlite3_errmsg(db))
			C.sqlite3_close_v2(db)
		}
		return nil, &sqliteError{Code: int(rc), Msg: msg}
	}
	conn := &sqliteConn{db: db}
	if busyTimeoutMS > 0 {
		if rc := C.sqlite3_busy_timeout(db, C.int(busyTimeoutMS)); rc != sqliteOK {
			err := conn.lastError("cannot set busy timeout")
			_ = conn.close()
			return nil, err
		}
	}
	for _, pragma := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA temp_store = MEMORY",
	} {
		if err := conn.execSimple(pragma); err != nil {
			_ = conn.close()
			return nil, err
		}
	}
	return conn, nil
}

func (c *sqliteConn) close() error {
	if c == nil || c.db == nil {
		return nil
	}
	rc := C.sqlite3_close_v2(c.db)
	c.db = nil
	if rc != sqliteOK {
		return &sqliteError{Code: int(rc), Msg: "cannot close database"}
	}
	return nil
}

// execSimple runs one or more statements without arguments and discards any
// rows they produce. It is used for DDL and pragmas.
func (c *sqliteConn) execSimple(sql string) error {
	if c == nil || c.db == nil {
		return &sqliteError{Code: -1, Msg: "database is closed"}
	}
	csql := C.CString(sql)
	defer C.free(unsafe.Pointer(csql))

	var errmsg *C.char
	rc := C.sqlite3_exec(c.db, csql, nil, nil, &errmsg) //nolint:gocritic // cgo call with a C string argument
	if rc != sqliteOK {
		msg := C.GoString(C.sqlite3_errmsg(c.db))
		if errmsg != nil {
			msg = C.GoString(errmsg)
			C.sqlite3_free(unsafe.Pointer(errmsg))
		}
		return &sqliteError{Code: int(rc), Msg: msg}
	}
	return nil
}

func (c *sqliteConn) prepare(query string) (*sqliteStmt, error) {
	if c == nil || c.db == nil {
		return nil, &sqliteError{Code: -1, Msg: "database is closed"}
	}
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var stmt *C.sqlite3_stmt
	rc := C.sqlite3_prepare_v2(c.db, cquery, C.int(-1), &stmt, nil) //nolint:gocritic // cgo call with a C string argument
	if rc != sqliteOK {
		return nil, c.lastError("cannot prepare statement")
	}
	return &sqliteStmt{conn: c, stmt: stmt}, nil
}

// exec runs a statement that carries arguments and does not return rows.
func (c *sqliteConn) exec(query string, args ...interface{}) error {
	stmt, err := c.prepare(query)
	if err != nil {
		return err
	}
	defer stmt.close()
	if err := stmt.bind(args); err != nil {
		return err
	}
	return stmt.step()
}

// query prepares a statement and returns a forward-only iterator.
func (c *sqliteConn) query(query string, args ...interface{}) (rowIter, error) {
	stmt, err := c.prepare(query)
	if err != nil {
		return nil, err
	}
	if err := stmt.bind(args); err != nil {
		stmt.close()
		return nil, err
	}
	return &sqliteRows{stmt: stmt}, nil
}

type sqliteStmt struct {
	conn *sqliteConn
	stmt *C.sqlite3_stmt
}

func (s *sqliteStmt) close() {
	if s == nil || s.stmt == nil {
		return
	}
	C.sqlite3_finalize(s.stmt)
	s.stmt = nil
}

// emptyBlob keeps a non-nil pointer so that a zero-length []byte binds as an
// empty BLOB rather than SQL NULL.
var emptyBlob = [1]byte{}

func (s *sqliteStmt) bind(args []interface{}) error {
	for i, arg := range args {
		idx := C.int(i + 1)
		var rc C.int
		switch v := arg.(type) {
		case nil:
			rc = C.sqlite3_bind_null(s.stmt, idx)
		case bool:
			n := int64(0)
			if v {
				n = 1
			}
			rc = C.sqlite3_bind_int64(s.stmt, idx, C.longlong(n))
		case int:
			rc = C.sqlite3_bind_int64(s.stmt, idx, C.longlong(v))
		case int32:
			rc = C.sqlite3_bind_int64(s.stmt, idx, C.longlong(v))
		case int64:
			rc = C.sqlite3_bind_int64(s.stmt, idx, C.longlong(v))
		case float32:
			rc = C.sqlite3_bind_double(s.stmt, idx, C.double(v))
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return fmt.Errorf("clean: cannot bind non-finite float64 %v", v)
			}
			rc = C.sqlite3_bind_double(s.stmt, idx, C.double(v))
		case string:
			cs := C.CString(v)
			rc = C.goc_bind_text(s.stmt, idx, cs, C.int(len(v)))
			C.free(unsafe.Pointer(cs))
		case []byte:
			var p unsafe.Pointer
			if len(v) == 0 {
				p = unsafe.Pointer(&emptyBlob[0])
			} else {
				p = C.CBytes(v)
			}
			rc = C.goc_bind_blob(s.stmt, idx, p, C.int(len(v)))
			if len(v) != 0 {
				C.free(p)
			}
		default:
			return fmt.Errorf("clean: cannot bind value of type %T", arg)
		}
		if rc != sqliteOK {
			return s.conn.lastError("cannot bind parameter")
		}
	}
	return nil
}

// step executes a statement that must not produce rows.
func (s *sqliteStmt) step() error {
	switch rc := C.sqlite3_step(s.stmt); rc {
	case sqliteDone:
		return nil
	case sqliteRow:
		return errors.New("clean: query unexpectedly produced a row")
	default:
		return s.conn.lastError("cannot execute statement")
	}
}

type sqliteRows struct {
	stmt    *sqliteStmt
	started bool
	done    bool
	err     error
}

func (r *sqliteRows) Next() bool {
	if r.done || r.err != nil {
		return false
	}
	rc := C.sqlite3_step(r.stmt.stmt)
	switch rc {
	case sqliteRow:
		r.started = true
		return true
	case sqliteDone:
		r.done = true
		return false
	default:
		r.err = r.stmt.conn.lastError("cannot read row")
		return false
	}
}

func (r *sqliteRows) Err() error { return r.err }

func (r *sqliteRows) Close() error {
	if r.stmt != nil {
		r.stmt.close()
		r.stmt = nil
	}
	return nil
}

func (r *sqliteRows) Int64(i int) int64 {
	return int64(C.sqlite3_column_int64(r.stmt.stmt, C.int(i)))
}

func (r *sqliteRows) Text(i int) string {
	p := C.sqlite3_column_text(r.stmt.stmt, C.int(i))
	if p == nil {
		return ""
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), C.sqlite3_column_bytes(r.stmt.stmt, C.int(i)))
}

func (r *sqliteRows) Blob(i int) []byte {
	p := C.sqlite3_column_blob(r.stmt.stmt, C.int(i))
	if p == nil {
		return nil
	}
	return C.GoBytes(p, C.sqlite3_column_bytes(r.stmt.stmt, C.int(i)))
}
