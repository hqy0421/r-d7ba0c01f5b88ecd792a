// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package clean

// schemaVersion is the fixed on-disk layout version. A database written by a
// different layout is rejected rather than migrated implicitly.
const schemaVersion = 1

// createStatements is the complete, fixed schema. It is intentionally not
// parameterised by callers: the cleaning contract depends on these exact
// tables and constraints.
var createStatements = []string{
	`CREATE TABLE IF NOT EXISTS meta (
		key   TEXT PRIMARY KEY NOT NULL,
		value TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS raw (
		seq         INTEGER PRIMARY KEY AUTOINCREMENT,
		batch_id    TEXT NOT NULL,
		row_index   INTEGER NOT NULL,
		source_rev  INTEGER NOT NULL,
		op          TEXT NOT NULL CHECK (op IN ('I','U','D')),
		schema_id   TEXT NOT NULL,
		encoding    TEXT NOT NULL DEFAULT '',
		pk          TEXT NOT NULL,
		payload     BLOB NOT NULL,
		payload_sha TEXT NOT NULL,
		received_at INTEGER NOT NULL,
		UNIQUE (batch_id, row_index)
	)`,
	`CREATE TABLE IF NOT EXISTS source (
		pk         TEXT PRIMARY KEY NOT NULL,
		source_rev INTEGER NOT NULL,
		op         TEXT NOT NULL CHECK (op IN ('I','U','D')),
		deleted    INTEGER NOT NULL DEFAULT 0,
		schema_id  TEXT NOT NULL,
		value      BLOB NOT NULL,
		value_sha  TEXT NOT NULL,
		raw_seq    INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		FOREIGN KEY (raw_seq) REFERENCES raw(seq)
	)`,
	`CREATE TABLE IF NOT EXISTS target (
		pk         TEXT PRIMARY KEY NOT NULL,
		source_rev INTEGER NOT NULL,
		rule_epoch INTEGER NOT NULL,
		value      BLOB NOT NULL,
		work_order TEXT NOT NULL DEFAULT '',
		updated_at INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS quarantine (
		pk         TEXT PRIMARY KEY NOT NULL,
		source_rev INTEGER NOT NULL,
		rule_epoch INTEGER NOT NULL,
		op         TEXT NOT NULL CHECK (op IN ('I','U','D')),
		value      BLOB NOT NULL,
		reason     TEXT NOT NULL,
		code       TEXT NOT NULL,
		work_order TEXT NOT NULL DEFAULT '',
		updated_at INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS repair (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		pk          TEXT NOT NULL,
		source_rev  INTEGER NOT NULL,
		rule_epoch  INTEGER NOT NULL,
		work_order  TEXT NOT NULL,
		kind        TEXT NOT NULL CHECK (kind IN ('value','overrides')),
		payload     BLOB NOT NULL,
		payload_sha TEXT NOT NULL,
		created_at  INTEGER NOT NULL,
		UNIQUE (pk, work_order)
	)`,
	`CREATE INDEX IF NOT EXISTS repair_lookup ON repair(pk, source_rev, rule_epoch, id)`,
	`CREATE TABLE IF NOT EXISTS receipt (
		batch_id      TEXT PRIMARY KEY NOT NULL,
		content_sha   TEXT NOT NULL,
		source_rev    INTEGER NOT NULL,
		row_count     INTEGER NOT NULL,
		valid_count   INTEGER NOT NULL,
		invalid_count INTEGER NOT NULL,
		deleted_count INTEGER NOT NULL,
		stale_repairs INTEGER NOT NULL DEFAULT 0,
		version       INTEGER NOT NULL,
		applied_at    INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS checkpoint (
		id              INTEGER PRIMARY KEY CHECK (id = 1),
		last_batch_id   TEXT NOT NULL,
		last_source_rev INTEGER NOT NULL,
		last_version    INTEGER NOT NULL,
		updated_at      INTEGER NOT NULL
	)`,
}

// meta keys.
const (
	metaSchemaVersion = "schema_version"
	metaRuleEpoch     = "rule_epoch"
	metaVersion       = "version"
	metaRulesID       = "rules_id"
	metaRulesVersion  = "rules_version"
	metaAppliedRev    = "applied_source_rev"
)
