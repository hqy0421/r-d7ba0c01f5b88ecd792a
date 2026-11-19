// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package clean

// sqlDriver is the minimal database surface the store needs. The cgo build
// implements it over the system SQLite library; other builds have no
// implementation, which keeps this package compiling everywhere.
type sqlDriver interface {
	exec(query string, args ...interface{}) error
	execSimple(sql string) error
	query(query string, args ...interface{}) (rowIter, error)
	close() error
}

// rowIter is a forward-only result set.
type rowIter interface {
	Next() bool
	Err() error
	Close() error
	Int64(i int) int64
	Text(i int) string
	Blob(i int) []byte
}
