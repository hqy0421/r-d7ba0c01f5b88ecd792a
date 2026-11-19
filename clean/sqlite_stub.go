// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

//go:build !cgo || !linux
// +build !cgo !linux

package clean

// Available reports whether this build can talk to SQLite.
func Available() bool { return false }

// openSQLite always fails in this build.
func openSQLite(string, int) (sqlDriver, error) { return nil, ErrSQLiteUnavailable }
