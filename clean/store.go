// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package clean

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	goavro "github.com/linkedin/goavro/v2"
)

// Op is the change operation carried by a source row.
type Op string

// The three insert/update/delete operations understood by the cleaning store.
const (
	OpInsert Op = "I"
	OpUpdate Op = "U"
	OpDelete Op = "D"
)

func (o Op) valid() bool { return o == OpInsert || o == OpUpdate || o == OpDelete }

// Encoding names the wire representation of a row payload.
type Encoding string

// Supported payload encodings. The zero value means binary Avro.
const (
	EncodingBinary Encoding = ""
	EncodingJSON   Encoding = "json"
)

// Row is one versioned change entry. The payload is a complete Avro datum for
// the schema registered under SchemaID; OpDelete rows carry no payload.
type Row struct {
	Op         Op
	SchemaID   string
	PrimaryKey string
	Payload    []byte
	Encoding   Encoding
}

// Batch is a run of rows that share one source revision. The ID identifies the
// batch for idempotent retries and must be stable across them.
type Batch struct {
	ID             string
	SourceRevision int64
	Rows           []Row
}

// RowView is the immutable argument handed to a row callback. A callback is
// pure: it decides validity of Value and must not touch the store.
type RowView struct {
	// Op is the operation that produced the row (I or U).
	Op Op
	// PrimaryKey is the affected business key.
	PrimaryKey string
	// SchemaID names the schema the source value was decoded with.
	SchemaID string
	// SourceRevision is the revision of the source row.
	SourceRevision int64
	// RuleEpoch is the rule installation epoch the decision runs under.
	RuleEpoch int64
	// Value is the decoded native value to judge: the corrected record for a
	// manual repair candidate, otherwise the source record.
	Value interface{}
	// Candidate is 0 for the source value and 1..n for repair candidates in
	// the order they were recorded.
	Candidate int
	// WorkOrder is empty for the source candidate and names the work order of
	// a repair candidate.
	WorkOrder string
}

// Decision is the pure verdict a row callback returns.
type Decision struct {
	// Valid sends the candidate value to the clean target table.
	Valid bool
	// Reason explains an invalid verdict; it is stored with the original
	// value in quarantine.
	Reason string
	// Code is a short, stable machine-readable classification of an invalid
	// verdict.
	Code string
}

// RowCallback judges one candidate value. It must be deterministic and free of
// side effects: the store may call it during a batch, a repair or a full
// revalidation, and uses "first valid candidate wins".
type RowCallback func(RowView) Decision

// Rules identifies one installed generation of cleaning rules. ID and Version
// together form the persisted rule identity; Callback is the installed pure row
// callback. Callback is only required for InstallRules and UseRules, not for
// opening a store.
type Rules struct {
	ID       string
	Version  string
	Callback RowCallback
}

func (r Rules) identity() string {
	return r.ID + "@" + r.Version
}

// Repair is a manual correction tied to one work order. It only applies to the
// exact source revision it was authored against and to the rule epoch current
// when it is recorded; anything later makes it stale.
//
// Either Value (a complete replacement datum) or Overrides (record fields
// merged over the source record) must be set, not both.
type Repair struct {
	PrimaryKey     string
	SourceRevision int64
	WorkOrder      string
	SchemaID       string
	Value          interface{}
	Overrides      map[string]interface{}
}

// RepairResult reports how a manual repair was absorbed.
type RepairResult struct {
	PrimaryKey     string
	WorkOrder      string
	SourceRevision int64
	RuleEpoch      int64
	// Applied reports that this repair's candidate is the winning value and
	// was written to the target table.
	Applied bool
	// Valid reports whether the row is clean after the repair.
	Valid bool
	// Winner is the work order of the winning repair, empty when the source
	// value itself won.
	Winner string
	// Reason and Code describe why the row remains invalid, if it does.
	Reason string
	Code   string
	// Idempotent reports that this call replayed an already recorded repair.
	Idempotent bool
}

// Receipt is the durable record of one applied batch. Returning the stored
// receipt for a retried batch is what makes ApplyBatch idempotent.
type Receipt struct {
	BatchID        string
	SourceRevision int64
	Version        int64
	Rows           int
	Valid          int
	Invalid        int
	Deleted        int
	// StaleRepairs counts repairs skipped because they no longer match the
	// source revision or rule epoch.
	StaleRepairs int
	// Replayed is true when this receipt came from an earlier application of
	// the same batch rather than from applying it now.
	Replayed bool
}

// Checkpoint is the committed position of the batch stream. It advances in the
// same transaction as the data it summarises.
type Checkpoint struct {
	LastBatchID  string
	LastRevision int64
	LastVersion  int64
	UpdatedAt    time.Time
}

// Status is a consistent snapshot of the store's metadata and row counts.
type Status struct {
	SchemaVersion int
	RuleEpoch     int64
	RulesID       string
	RulesVersion  string
	Version       int64
	Checkpoint    Checkpoint
	// Stale reports that committed target or quarantine rows were produced
	// under an older rule epoch than the installed one, or that a recorded
	// repair no longer binds to the current source revision. It is derived
	// from committed state, so it survives Close and Open.
	Stale bool
	// StaleRows counts target plus quarantine rows below the current epoch,
	// plus repairs that no longer bind to the current source revision.
	StaleRows   int
	RawRows     int
	Sources     int
	Targets     int
	Quarantined int
	Repairs     int
}

// TargetRow is a row of the clean target table.
type TargetRow struct {
	PrimaryKey     string
	SourceRevision int64
	RuleEpoch      int64
	Value          interface{}
	WorkOrder      string
	UpdatedAt      time.Time
}

// QuarantinedRow is a rejected row: the original source value plus the reason
// it was not accepted, and any work order whose repair was rejected.
type QuarantinedRow struct {
	PrimaryKey     string
	SourceRevision int64
	RuleEpoch      int64
	Op             Op
	Value          interface{}
	Reason         string
	Code           string
	WorkOrder      string
	UpdatedAt      time.Time
}

// SourceRow is the current source state derived from raw. Deleted is true for
// a row that has been deleted and not resurrected.
type SourceRow struct {
	PrimaryKey     string
	SourceRevision int64
	Op             Op
	Deleted        bool
	Value          interface{}
	SchemaID       string
}

// Report summarises a full revalidation from raw.
type Report struct {
	RuleEpoch int64
	Version   int64
	Sources   int
	Valid     int
	Invalid   int
	Deleted   int
	// StaleRepairs counts repairs whose source revision or rule epoch no
	// longer matches, and which were therefore discarded.
	StaleRepairs int
}

// Sentinel errors. They are stable so callers can classify failures without
// inspecting strings.
var (
	// ErrSQLiteUnavailable is returned by Open when this build cannot link
	// against SQLite (for example, cgo is disabled or the platform is not
	// supported).
	ErrSQLiteUnavailable = errors.New("clean: SQLite support requires cgo on a supported platform")
	// ErrClosed is returned by any operation on a closed store.
	ErrClosed = errors.New("clean: store is closed")
	// ErrNoRules is returned when the store has no bound row callback.
	ErrNoRules = errors.New("clean: no cleaning rules are bound")
	// ErrRulesMismatch is returned when UseRules binds a different rules
	// identity than the one recorded in the database.
	ErrRulesMismatch = errors.New("clean: rules identity does not match the installed rules")
	// ErrOutOfOrder is returned for a batch older than the committed
	// checkpoint.
	ErrOutOfOrder = errors.New("clean: batch source revision is older than the checkpoint")
	// ErrBatchConflict is returned when a batch ID is retried with different
	// content.
	ErrBatchConflict = errors.New("clean: batch id was already applied with different content")
	// ErrUnknownSchema is returned for an unregistered schema ID.
	ErrUnknownSchema = errors.New("clean: unknown schema id")
	// ErrInvalidBatch is returned for a malformed batch or row.
	ErrInvalidBatch = errors.New("clean: invalid batch")
	// ErrStaleRepair is returned when a repair no longer matches the current
	// source revision or rule epoch.
	ErrStaleRepair = errors.New("clean: repair is stale for the current source revision or rule epoch")
	// ErrSourceMissing is returned when a repair names a primary key that has
	// never been seen.
	ErrSourceMissing = errors.New("clean: repair names a missing source row")
	// ErrSourceDeleted is returned when a repair tries to act on a deleted
	// row. Deleted rows are never resurrected by a repair.
	ErrSourceDeleted = errors.New("clean: repair targets a deleted source row")
	// ErrRepairConflict is returned when a work order is reused for a
	// primary key with a different repair payload.
	ErrRepairConflict = errors.New("clean: work order already recorded a different repair")
	// ErrRepairValue is returned for a repair that cannot be applied to the
	// source value shape.
	ErrRepairValue = errors.New("clean: repair value is incompatible with the source value")
	// ErrNoSource is returned by revalidation helpers when raw is empty.
	ErrNoSource = errors.New("clean: no source rows")
)

// CallbackPanicError reports that the row callback panicked. The batch was
// rolled back, so the store kept its previous state.
type CallbackPanicError struct {
	PrimaryKey string
	Candidate  int
	WorkOrder  string
	Panic      interface{}
	Stack      []byte
}

func (e *CallbackPanicError) Error() string {
	return fmt.Sprintf("clean: callback panicked for %q candidate %d: %v", e.PrimaryKey, e.Candidate, e.Panic)
}

// StorageError reports a SQLite failure. The transaction was rolled back, so
// the batch kept its previous state.
type StorageError struct {
	Op  string
	Err error
}

func (e *StorageError) Error() string {
	return "clean: storage failure during " + e.Op + ": " + e.Err.Error()
}

// DecodeError reports a payload the registered Codec could not decode. The
// batch is rejected as a whole; it is not a validation verdict.
type DecodeError struct {
	PrimaryKey string
	SchemaID   string
	Err        error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("clean: cannot decode payload for %q using schema %q: %v", e.PrimaryKey, e.SchemaID, e.Err)
}

// Unwrap exposes the underlying decode error.
func (e *DecodeError) Unwrap() error { return e.Err }

// Options configures Open.
type Options struct {
	// Path is the SQLite database file. It is created when absent.
	Path string
	// Schemas maps schema IDs to Avro schema specifications. Every I/U row
	// must name one of these.
	Schemas map[string]string
	// BusyTimeout is how long SQLite waits for a competing writer. Zero
	// selects a one second default.
	BusyTimeout time.Duration
	// Now overrides the wall clock; it exists for deterministic tests.
	Now func() time.Time
}

// Store is a cleaning store backed by SQLite. It is safe for concurrent use;
// operations are serialised.
type Store struct {
	mu     sync.Mutex
	conn   sqlDriver
	path   string
	now    func() time.Time
	codecs map[string]*goavro.Codec
	rules  *Rules
	closed bool
}

// Open opens or creates the cleaning database at opts.Path and registers the
// supplied Avro schemas. The on-disk schema is fixed: a database written with
// another layout version is rejected, never migrated implicitly.
func Open(opts Options) (*Store, error) {
	if !Available() {
		return nil, ErrSQLiteUnavailable
	}
	if opts.Path == "" {
		return nil, fmt.Errorf("%w: Options.Path is empty", ErrInvalidBatch)
	}
	timeout := opts.BusyTimeout
	if timeout <= 0 {
		timeout = time.Second
	}
	conn, err := openSQLite(opts.Path, int(timeout/time.Millisecond))
	if err != nil {
		return nil, err
	}
	s := &Store{
		conn:   conn,
		path:   opts.Path,
		now:    opts.Now,
		codecs: make(map[string]*goavro.Codec, len(opts.Schemas)),
	}
	if s.now == nil {
		s.now = time.Now
	}
	for id, spec := range opts.Schemas {
		if err := s.registerCodec(id, spec); err != nil {
			_ = conn.close()
			return nil, err
		}
	}
	if err := s.initSchema(); err != nil {
		_ = conn.close()
		return nil, err
	}
	return s, nil
}

// RegisterSchema adds or replaces the Codec used for a schema ID. It does not
// alter existing raw data; rows are always re-decoded from their bytes.
func (s *Store) RegisterSchema(id, specification string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	return s.registerCodec(id, specification)
}

func (s *Store) registerCodec(id, specification string) error {
	if id == "" {
		return fmt.Errorf("%w: empty schema id", ErrInvalidBatch)
	}
	codec, err := goavro.NewCodec(specification)
	if err != nil {
		return fmt.Errorf("clean: cannot compile schema %q: %v", id, err)
	}
	s.codecs[id] = codec
	return nil
}

// Available reports whether this build can talk to SQLite.
func (s *Store) Available() bool { return Available() }

func (s *Store) usable() error {
	if s == nil || s.closed {
		return ErrClosed
	}
	return nil
}

func (s *Store) initSchema() error {
	for _, stmt := range createStatements {
		if err := s.conn.execSimple(stmt); err != nil {
			return &StorageError{Op: "create schema", Err: err}
		}
	}
	if err := s.conn.execSimple("BEGIN IMMEDIATE"); err != nil {
		return &StorageError{Op: "begin schema init", Err: err}
	}
	fail := func(op string, err error) error {
		_ = s.conn.execSimple("ROLLBACK")
		return &StorageError{Op: op, Err: err}
	}
	// Refuse to touch a database written with an unknown layout.
	rows, err := s.conn.query(`SELECT value FROM meta WHERE key = ?`, metaSchemaVersion)
	if err != nil {
		return fail("read schema version", err)
	}
	haveVersion := false
	var stored string
	if rows.Next() {
		haveVersion = true
		stored = rows.Text(0)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fail("read schema version", err)
	}
	rows.Close()
	if haveVersion {
		v, convErr := strconv.Atoi(stored)
		if convErr != nil || v != schemaVersion {
			return fail("check schema version", fmt.Errorf("clean: database schema version %q is not supported (want %d)", stored, schemaVersion))
		}
	}
	now := s.stamp()
	defaults := [][2]string{
		{metaSchemaVersion, strconv.Itoa(schemaVersion)},
		{metaRuleEpoch, "0"},
		{metaVersion, "0"},
		{metaRulesID, ""},
		{metaRulesVersion, ""},
		{metaAppliedRev, "0"},
	}
	for _, kv := range defaults {
		if err := s.conn.exec(`INSERT OR IGNORE INTO meta(key, value) VALUES(?, ?)`, kv[0], kv[1]); err != nil {
			return fail("insert meta", err)
		}
	}
	if err := s.conn.exec(`INSERT OR IGNORE INTO checkpoint(id, last_batch_id, last_source_rev, last_version, updated_at)
		VALUES(1, '', 0, 0, ?)`, now); err != nil {
		return fail("insert checkpoint", err)
	}
	if err := s.conn.execSimple("COMMIT"); err != nil {
		return fail("commit schema init", err)
	}
	return nil
}

func (s *Store) stamp() int64 { return s.now().UTC().UnixNano() }

func stampTime(nanos int64) time.Time { return time.Unix(0, nanos).UTC() }

func (s *Store) readIntMeta(key string) (int64, error) {
	rows, err := s.conn.query(`SELECT value FROM meta WHERE key = ?`, key)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, nil
	}
	return strconv.ParseInt(rows.Text(0), 10, 64)
}

func (s *Store) readMeta(key string) (string, error) {
	rows, err := s.conn.query(`SELECT value FROM meta WHERE key = ?`, key)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", rows.Err()
	}
	return rows.Text(0), nil
}

// Close releases the database connection. It is safe to call more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.conn.close()
}

func sha256Hex(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		var size [8]byte
		for i := 0; i < 8; i++ {
			size[i] = byte(uint64(len(p)) >> (8 * uint(i)))
		}
		h.Write(size[:])
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}
