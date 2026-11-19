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
	"errors"
	"math/big"
	"strings"
)

// emptyDigest is the value_sha recorded for deleted source rows.
const emptyDigest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Target returns one clean row by primary key.
func (s *Store) Target(pk string) (TargetRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return TargetRow{}, false, err
	}
	rows, err := s.conn.query(`SELECT pk, source_rev, rule_epoch, value, work_order, updated_at FROM target WHERE pk = ?`, pk)
	if err != nil {
		return TargetRow{}, false, &StorageError{Op: "read target", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return TargetRow{}, false, &StorageError{Op: "read target", Err: err}
		}
		return TargetRow{}, false, nil
	}
	value, err := decodeStoredValue(rows.Blob(3))
	if err != nil {
		return TargetRow{}, false, &StorageError{Op: "decode target", Err: err}
	}
	return TargetRow{
		PrimaryKey:     rows.Text(0),
		SourceRevision: rows.Int64(1),
		RuleEpoch:      rows.Int64(2),
		Value:          value,
		WorkOrder:      rows.Text(4),
		UpdatedAt:      stampTime(rows.Int64(5)),
	}, true, nil
}

// Targets returns every clean row ordered by primary key.
func (s *Store) Targets() ([]TargetRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	rows, err := s.conn.query(`SELECT pk, source_rev, rule_epoch, value, work_order, updated_at FROM target ORDER BY pk`)
	if err != nil {
		return nil, &StorageError{Op: "read targets", Err: err}
	}
	defer rows.Close()
	var out []TargetRow
	for rows.Next() {
		value, err := decodeStoredValue(rows.Blob(3))
		if err != nil {
			return nil, &StorageError{Op: "decode target", Err: err}
		}
		out = append(out, TargetRow{
			PrimaryKey:     rows.Text(0),
			SourceRevision: rows.Int64(1),
			RuleEpoch:      rows.Int64(2),
			Value:          value,
			WorkOrder:      rows.Text(4),
			UpdatedAt:      stampTime(rows.Int64(5)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, &StorageError{Op: "read targets", Err: err}
	}
	return out, nil
}

// Quarantined returns every rejected row ordered by primary key, each with its
// original value and the reason it was rejected.
func (s *Store) Quarantined() ([]QuarantinedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	rows, err := s.conn.query(`SELECT pk, source_rev, rule_epoch, op, value, reason, code, work_order, updated_at
		FROM quarantine ORDER BY pk`)
	if err != nil {
		return nil, &StorageError{Op: "read quarantine", Err: err}
	}
	defer rows.Close()
	var out []QuarantinedRow
	for rows.Next() {
		value, err := decodeStoredValue(rows.Blob(4))
		if err != nil {
			return nil, &StorageError{Op: "decode quarantine", Err: err}
		}
		out = append(out, QuarantinedRow{
			PrimaryKey:     rows.Text(0),
			SourceRevision: rows.Int64(1),
			RuleEpoch:      rows.Int64(2),
			Op:             Op(rows.Text(3)),
			Value:          value,
			Reason:         rows.Text(5),
			Code:           rows.Text(6),
			WorkOrder:      rows.Text(7),
			UpdatedAt:      stampTime(rows.Int64(8)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, &StorageError{Op: "read quarantine", Err: err}
	}
	return out, nil
}

// Quarantine returns one rejected row by primary key.
func (s *Store) Quarantine(pk string) (QuarantinedRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return QuarantinedRow{}, false, err
	}
	rows, err := s.conn.query(`SELECT pk, source_rev, rule_epoch, op, value, reason, code, work_order, updated_at
		FROM quarantine WHERE pk = ?`, pk)
	if err != nil {
		return QuarantinedRow{}, false, &StorageError{Op: "read quarantine", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return QuarantinedRow{}, false, &StorageError{Op: "read quarantine", Err: err}
		}
		return QuarantinedRow{}, false, nil
	}
	value, err := decodeStoredValue(rows.Blob(4))
	if err != nil {
		return QuarantinedRow{}, false, &StorageError{Op: "decode quarantine", Err: err}
	}
	return QuarantinedRow{
		PrimaryKey:     rows.Text(0),
		SourceRevision: rows.Int64(1),
		RuleEpoch:      rows.Int64(2),
		Op:             Op(rows.Text(3)),
		Value:          value,
		Reason:         rows.Text(5),
		Code:           rows.Text(6),
		WorkOrder:      rows.Text(7),
		UpdatedAt:      stampTime(rows.Int64(8)),
	}, true, nil
}

// Source returns the current source state of one primary key.
func (s *Store) Source(pk string) (SourceRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return SourceRow{}, false, err
	}
	rec, found, err := s.readSource(pk)
	if err != nil || !found {
		return SourceRow{}, found, err
	}
	return SourceRow{
		PrimaryKey:     pk,
		SourceRevision: rec.SourceRevision,
		Op:             rec.Op,
		Deleted:        rec.Deleted,
		Value:          rec.Value,
		SchemaID:       rec.SchemaID,
	}, true, nil
}

// Raw returns a copy of the immutable raw payload for a key, newest first.
func (s *Store) Raw(pk string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	rows, err := s.conn.query(`SELECT payload FROM raw WHERE pk = ? ORDER BY seq DESC`, pk)
	if err != nil {
		return nil, &StorageError{Op: "read raw", Err: err}
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		out = append(out, rows.Blob(0))
	}
	if err := rows.Err(); err != nil {
		return nil, &StorageError{Op: "read raw", Err: err}
	}
	return out, nil
}

// Checkpoint returns the committed batch breakpoint.
func (s *Store) Checkpoint() (Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Checkpoint{}, err
	}
	return s.readCheckpoint()
}

func (s *Store) readCheckpoint() (Checkpoint, error) {
	rows, err := s.conn.query(`SELECT last_batch_id, last_source_rev, last_version, updated_at FROM checkpoint WHERE id = 1`)
	if err != nil {
		return Checkpoint{}, &StorageError{Op: "read checkpoint", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Checkpoint{}, &StorageError{Op: "read checkpoint", Err: err}
		}
		return Checkpoint{}, &StorageError{Op: "read checkpoint", Err: errors.New("checkpoint row missing")}
	}
	return Checkpoint{
		LastBatchID:  rows.Text(0),
		LastRevision: rows.Int64(1),
		LastVersion:  rows.Int64(2),
		UpdatedAt:    stampTime(rows.Int64(3)),
	}, nil
}

// Status returns the store's metadata, staleness and row counts.
func (s *Store) Status() (Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Status{}, err
	}
	epoch, err := s.readIntMeta(metaRuleEpoch)
	if err != nil {
		return Status{}, &StorageError{Op: "read rule epoch", Err: err}
	}
	version, err := s.readIntMeta(metaVersion)
	if err != nil {
		return Status{}, &StorageError{Op: "read version", Err: err}
	}
	rulesID, err := s.readMeta(metaRulesID)
	if err != nil {
		return Status{}, &StorageError{Op: "read rules id", Err: err}
	}
	rulesVersion, err := s.readMeta(metaRulesVersion)
	if err != nil {
		return Status{}, &StorageError{Op: "read rules version", Err: err}
	}
	checkpoint, err := s.readCheckpoint()
	if err != nil {
		return Status{}, err
	}
	status := Status{
		SchemaVersion: schemaVersion,
		RuleEpoch:     epoch,
		RulesID:       rulesID,
		RulesVersion:  rulesVersion,
		Version:       version,
		Checkpoint:    checkpoint,
	}
	counts := []struct {
		name string
		dest *int
	}{
		{"raw", &status.RawRows},
		{"source", &status.Sources},
		{"target", &status.Targets},
		{"quarantine", &status.Quarantined},
		{"repair", &status.Repairs},
	}
	for _, c := range counts {
		n, err := s.countRows(c.name)
		if err != nil {
			return Status{}, err
		}
		*c.dest = n
	}
	stale, err := s.countStale(epoch)
	if err != nil {
		return Status{}, err
	}
	status.StaleRows = stale
	status.Stale = stale > 0
	return status, nil
}

func (s *Store) countRows(table string) (int, error) {
	rows, err := s.conn.query(`SELECT COUNT(*) FROM ` + table)
	if err != nil {
		return 0, &StorageError{Op: "count " + table, Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, &StorageError{Op: "count " + table, Err: rows.Err()}
	}
	return int(rows.Int64(0)), nil
}

// countStale counts committed target and quarantine rows produced under an
// older rule epoch, plus repairs that no longer bind to the current source
// revision. It is computed from committed state, so it is correct right after
// Open and after any half-finished upgrade.
func (s *Store) countStale(epoch int64) (int, error) {
	var total int
	for _, table := range []string{"target", "quarantine"} {
		rows, err := s.conn.query(`SELECT COUNT(*) FROM `+table+` WHERE rule_epoch <> ?`, epoch)
		if err != nil {
			return 0, &StorageError{Op: "count stale " + table, Err: err}
		}
		if rows.Next() {
			total += int(rows.Int64(0))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, &StorageError{Op: "count stale " + table, Err: err}
		}
	}
	rows, err := s.conn.query(`SELECT COUNT(*) FROM repair r WHERE r.rule_epoch <> ? OR NOT EXISTS (
		SELECT 1 FROM source s WHERE s.pk = r.pk AND s.source_rev = r.source_rev AND s.deleted = 0)`, epoch)
	if err != nil {
		return 0, &StorageError{Op: "count stale repairs", Err: err}
	}
	if rows.Next() {
		total += int(rows.Int64(0))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, &StorageError{Op: "count stale repairs", Err: err}
	}
	return total, nil
}

// Stale reports whether the store needs a revalidation because rules were
// re-installed, or because repairs no longer bind to their source version.
func (s *Store) Stale() (bool, error) {
	status, err := s.Status()
	if err != nil {
		return false, err
	}
	return status.Stale, nil
}

// RepairCount returns how many recorded manual repairs are live for a key.
func (s *Store) RepairCount(pk string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	rows, err := s.conn.query(`SELECT COUNT(*) FROM repair WHERE pk = ?`, pk)
	if err != nil {
		return 0, &StorageError{Op: "count repairs", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, &StorageError{Op: "count repairs", Err: rows.Err()}
	}
	return int(rows.Int64(0)), nil
}

// RatFromString is a small helper for tests and callers that build decimal
// repair values. It returns an error for malformed input. The numerator and
// denominator are parsed individually so the value is bounded by int64 decimal
// parsing rather than by an unbounded rational parser.
func RatFromString(s string) (*big.Rat, error) {
	numText, denText := s, "1"
	if i := strings.IndexByte(s, '/'); i >= 0 {
		numText, denText = s[:i], s[i+1:]
	}
	num, ok := new(big.Int).SetString(numText, 10)
	if !ok {
		return nil, errors.New("clean: cannot parse rational numerator " + s)
	}
	den, ok := new(big.Int).SetString(denText, 10)
	if !ok || den.Sign() == 0 {
		return nil, errors.New("clean: cannot parse rational denominator " + s)
	}
	return new(big.Rat).SetFrac(num, den), nil
}
