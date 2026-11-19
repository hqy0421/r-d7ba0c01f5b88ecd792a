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
	"fmt"
)

// InstallRules installs a new generation of cleaning rules and increments the
// persisted rule epoch. Re-installing the same identity is a no-op and keeps
// the epoch, which makes deployment retries safe. Any change of identity
// invalidates every manual repair recorded under an older epoch and marks the
// committed target and quarantine rows stale until Revalidate runs.
func (s *Store) InstallRules(rules Rules) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return 0, err
	}
	if rules.Callback == nil {
		return 0, fmt.Errorf("%w: InstallRules requires a callback", ErrNoRules)
	}
	if rules.ID == "" || rules.Version == "" {
		return 0, fmt.Errorf("%w: rules ID and Version are required", ErrInvalidBatch)
	}
	storedID, err := s.readMeta(metaRulesID)
	if err != nil {
		return 0, &StorageError{Op: "read rules id", Err: err}
	}
	storedVersion, err := s.readMeta(metaRulesVersion)
	if err != nil {
		return 0, &StorageError{Op: "read rules version", Err: err}
	}
	epoch, err := s.readIntMeta(metaRuleEpoch)
	if err != nil {
		return 0, &StorageError{Op: "read rule epoch", Err: err}
	}
	if storedID == rules.ID && storedVersion == rules.Version {
		s.rules = &rules
		return epoch, nil
	}

	work := func() (Receipt, error) {
		epoch++
		if err := s.setIntMeta(metaRuleEpoch, epoch, "update rule epoch"); err != nil {
			return Receipt{}, err
		}
		for key, value := range map[string]string{metaRulesID: rules.ID, metaRulesVersion: rules.Version} {
			if err := s.conn.exec(`UPDATE meta SET value = ? WHERE key = ?`, value, key); err != nil {
				return Receipt{}, &StorageError{Op: "update rules identity", Err: err}
			}
		}
		// Repairs stay in place but bind to the epoch they were recorded
		// under, so a reinstallation makes all of them inert at once. Keeping
		// them lets Status and Revalidate report and discard them explicitly.
		return Receipt{}, nil
	}
	if _, err := s.inTx("install rules", work); err != nil {
		return 0, err
	}
	s.rules = &rules
	return epoch, nil
}

// UseRules binds the callback for a reopened store without changing the rule
// epoch. It fails with ErrRulesMismatch when the identity differs from the one
// persisted by the last InstallRules, because silently judging old data with
// unregistered rules would defeat the epoch mechanism.
func (s *Store) UseRules(rules Rules) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if rules.Callback == nil {
		return fmt.Errorf("%w: UseRules requires a callback", ErrNoRules)
	}
	storedID, err := s.readMeta(metaRulesID)
	if err != nil {
		return &StorageError{Op: "read rules id", Err: err}
	}
	storedVersion, err := s.readMeta(metaRulesVersion)
	if err != nil {
		return &StorageError{Op: "read rules version", Err: err}
	}
	if storedID != rules.ID || storedVersion != rules.Version {
		return fmt.Errorf("%w: stored %q, provided %q", ErrRulesMismatch, storedID+"@"+storedVersion, rules.identity())
	}
	s.rules = &rules
	return nil
}

// AddRepair records a manual fix for one primary key and immediately re-judges
// the row through the current callback.
//
// The repair is accepted only when it is authored against the current source
// revision and under the current rule installation epoch: a later source
// correction, a deletion, a reuse of the primary key, or a rules upgrade all
// make it stale (ErrStaleRepair). Once a repair is recorded, replaying it with
// the same payload is idempotent, and a later, different repair never
// overwrites an earlier winner: candidates are judged in recorded order and
// the first valid one wins.
func (s *Store) AddRepair(repair Repair) (RepairResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return RepairResult{}, err
	}
	if repair.PrimaryKey == "" || repair.WorkOrder == "" {
		return RepairResult{}, fmt.Errorf("%w: repair needs a primary key and a work order", ErrInvalidBatch)
	}
	if repair.SourceRevision <= 0 {
		return RepairResult{}, fmt.Errorf("%w: repair needs a positive source revision", ErrInvalidBatch)
	}
	if (repair.Value == nil) == (repair.Overrides == nil) {
		return RepairResult{}, fmt.Errorf("%w: set exactly one of Value or Overrides", ErrInvalidBatch)
	}
	callback, err := s.currentCallback()
	if err != nil {
		return RepairResult{}, err
	}

	kind := "value"
	payload := repair.Value
	if repair.Overrides != nil {
		kind = "overrides"
		payload = repair.Overrides
	}
	encoded, err := encodeValue(payload)
	if err != nil {
		return RepairResult{}, err
	}
	payloadSha := sha256Hex(encoded)

	var result RepairResult
	work := func() (Receipt, error) {
		var err error
		result, err = s.addRepairLocked(repair, kind, encoded, payloadSha, callback)
		return Receipt{}, err
	}
	if _, err := s.inTx("add repair", work); err != nil {
		return RepairResult{}, err
	}
	return result, nil
}

func (s *Store) addRepairLocked(repair Repair, kind string, encoded []byte, payloadSha string, callback RowCallback) (RepairResult, error) {
	epoch, err := s.readIntMeta(metaRuleEpoch)
	if err != nil {
		return RepairResult{}, &StorageError{Op: "read rule epoch", Err: err}
	}
	src, found, err := s.readSource(repair.PrimaryKey)
	if err != nil {
		return RepairResult{}, err
	}
	if !found {
		return RepairResult{}, fmt.Errorf("%w: %q", ErrSourceMissing, repair.PrimaryKey)
	}
	if src.Deleted {
		return RepairResult{}, fmt.Errorf("%w: %q", ErrSourceDeleted, repair.PrimaryKey)
	}
	if src.SourceRevision != repair.SourceRevision {
		return RepairResult{}, fmt.Errorf("%w: %q is at source revision %d, repair was authored for %d",
			ErrStaleRepair, repair.PrimaryKey, src.SourceRevision, repair.SourceRevision)
	}

	existing, found, err := s.repairForWorkOrder(repair.PrimaryKey, repair.WorkOrder)
	if err != nil {
		return RepairResult{}, err
	}
	if found {
		if existing.PayloadSha != payloadSha || existing.Kind != kind {
			return RepairResult{}, fmt.Errorf("%w: work order %q for %q", ErrRepairConflict, repair.WorkOrder, repair.PrimaryKey)
		}
		if existing.RuleEpoch != epoch {
			// The work order was recorded under an older epoch and therefore
			// cannot influence the current version.
			return RepairResult{}, fmt.Errorf("%w: work order %q was recorded under rule epoch %d, current is %d",
				ErrStaleRepair, repair.WorkOrder, existing.RuleEpoch, epoch)
		}
	} else {
		if err := s.conn.exec(`INSERT INTO repair(pk, source_rev, rule_epoch, work_order, kind, payload, payload_sha, created_at)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
			repair.PrimaryKey, repair.SourceRevision, epoch, repair.WorkOrder, kind, encoded, payloadSha, s.stamp()); err != nil {
			return RepairResult{}, &StorageError{Op: "insert repair", Err: err}
		}
	}

	repairs, err := s.liveRepairs(repair.PrimaryKey, src.SourceRevision, epoch)
	if err != nil {
		return RepairResult{}, err
	}
	candidates, err := repairCandidates(src.Value, repairs)
	if err != nil {
		return RepairResult{}, err
	}
	row := Row{Op: src.Op, SchemaID: src.SchemaID, PrimaryKey: repair.PrimaryKey}
	version, err := s.readIntMeta(metaVersion)
	if err != nil {
		return RepairResult{}, &StorageError{Op: "read version", Err: err}
	}
	w := &batchWork{epoch: epoch, now: s.stamp(), version: version}
	j := judge(row, src.SourceRevision, epoch, src.Value, candidates, callback)
	settled, err := s.judgementAlreadyCommitted(w, repair.PrimaryKey, src.SourceRevision, src.Op, j)
	if err != nil {
		return RepairResult{}, err
	}
	if !settled {
		if err := s.writeJudgement(w, repair.PrimaryKey, src.SourceRevision, src.Op, j); err != nil {
			return RepairResult{}, err
		}
		if _, err := s.bumpVersion(); err != nil {
			return RepairResult{}, err
		}
	}
	return RepairResult{
		PrimaryKey:     repair.PrimaryKey,
		WorkOrder:      repair.WorkOrder,
		SourceRevision: src.SourceRevision,
		RuleEpoch:      epoch,
		Applied:        j.valid && j.winner == repair.WorkOrder,
		Valid:          j.valid,
		Winner:         j.winner,
		Reason:         j.reason,
		Code:           j.code,
		Idempotent:     found,
	}, nil
}

type storedRepair struct {
	Kind       string
	PayloadSha string
	RuleEpoch  int64
}

func (s *Store) repairForWorkOrder(pk, workOrder string) (storedRepair, bool, error) {
	rows, err := s.conn.query(`SELECT kind, payload_sha, rule_epoch FROM repair WHERE pk = ? AND work_order = ?`, pk, workOrder)
	if err != nil {
		return storedRepair{}, false, &StorageError{Op: "read repair", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return storedRepair{}, false, &StorageError{Op: "read repair", Err: err}
		}
		return storedRepair{}, false, nil
	}
	return storedRepair{Kind: rows.Text(0), PayloadSha: rows.Text(1), RuleEpoch: rows.Int64(2)}, true, nil
}

// bumpVersion advances the committed version and mirrors it into the
// checkpoint, in the caller's transaction. It returns the new version.
func (s *Store) bumpVersion() (int64, error) {
	version, err := s.readIntMeta(metaVersion)
	if err != nil {
		return 0, &StorageError{Op: "read version", Err: err}
	}
	version++
	if err := s.setIntMeta(metaVersion, version, "update version"); err != nil {
		return 0, err
	}
	if err := s.conn.exec(`UPDATE checkpoint SET last_version = ?, updated_at = ? WHERE id = 1`, version, s.stamp()); err != nil {
		return 0, &StorageError{Op: "update checkpoint", Err: err}
	}
	return version, nil
}

// Revalidate replays every raw row through the current callback. It is the
// upgrade path: after InstallRules reports stale data, Revalidate rebuilds
// source, target and quarantine from raw under the new epoch. Manual repairs
// survive only when they still bind to the final source revision of their key
// and to the current epoch; everything else is dropped and counted.
func (s *Store) Revalidate() (Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Report{}, err
	}
	callback, err := s.currentCallback()
	if err != nil {
		return Report{}, err
	}
	var report Report
	work := func() (Receipt, error) {
		var err error
		report, err = s.revalidateLocked(callback)
		return Receipt{}, err
	}
	if _, err := s.inTx("revalidate", work); err != nil {
		return Report{}, err
	}
	return report, nil
}

type rawEntry struct {
	Seq            int64
	SourceRevision int64
	Op             Op
	SchemaID       string
	Encoding       Encoding
	PrimaryKey     string
	Payload        []byte
}

type finalSource struct {
	SourceRevision int64
	Op             Op
	SchemaID       string
	Value          interface{}
	ValueSha       string
	Deleted        bool
}

func (s *Store) revalidateLocked(callback RowCallback) (Report, error) {
	epoch, err := s.readIntMeta(metaRuleEpoch)
	if err != nil {
		return Report{}, &StorageError{Op: "read rule epoch", Err: err}
	}
	rawRows, err := s.loadRaw()
	if err != nil {
		return Report{}, err
	}
	final := make(map[string]*finalSource)
	order := make([]string, 0, len(rawRows))
	for _, entry := range rawRows {
		if _, seen := final[entry.PrimaryKey]; !seen {
			order = append(order, entry.PrimaryKey)
		}
		if entry.Op == OpDelete {
			final[entry.PrimaryKey] = &finalSource{
				SourceRevision: entry.SourceRevision,
				Op:             OpDelete,
				Deleted:        true,
				ValueSha:       emptyDigest,
			}
			continue
		}
		value, err := s.decodeRawValue(entry)
		if err != nil {
			return Report{}, err
		}
		encoded, err := encodeValue(value)
		if err != nil {
			return Report{}, &DecodeError{PrimaryKey: entry.PrimaryKey, SchemaID: entry.SchemaID, Err: err}
		}
		final[entry.PrimaryKey] = &finalSource{
			SourceRevision: entry.SourceRevision,
			Op:             entry.Op,
			SchemaID:       entry.SchemaID,
			Value:          value,
			ValueSha:       sha256Hex(encoded),
		}
	}

	// Rebuild everything downstream of raw.
	for _, table := range []string{"source", "target", "quarantine"} {
		if err := s.conn.execSimple("DELETE FROM " + table); err != nil {
			return Report{}, &StorageError{Op: "clear " + table, Err: err}
		}
	}
	repairRows, err := s.allRepairs()
	if err != nil {
		return Report{}, err
	}
	keep := make(map[string][]repairRecord)
	dropped := 0
	for _, rep := range repairRows {
		fs, ok := final[rep.PrimaryKey]
		if !ok || fs.Deleted || fs.SourceRevision != rep.SourceRevision || rep.RuleEpoch != epoch {
			dropped++
			continue
		}
		keep[rep.PrimaryKey] = append(keep[rep.PrimaryKey], rep)
	}
	report := Report{RuleEpoch: epoch, Sources: len(order), StaleRepairs: dropped}
	if err := s.conn.execSimple("DELETE FROM repair"); err != nil {
		return Report{}, &StorageError{Op: "clear repairs", Err: err}
	}
	for _, pk := range order {
		repairs := keep[pk]
		for _, rep := range repairs {
			if err := s.conn.exec(`INSERT INTO repair(pk, source_rev, rule_epoch, work_order, kind, payload, payload_sha, created_at)
				VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
				rep.PrimaryKey, rep.SourceRevision, rep.RuleEpoch, rep.WorkOrder, rep.Kind, rep.Payload, sha256Hex(rep.Payload), s.stamp()); err != nil {
				return Report{}, &StorageError{Op: "restore repair", Err: err}
			}
		}
	}

	now := s.stamp()
	w := &batchWork{epoch: epoch, now: now}
	for _, pk := range order {
		fs := final[pk]
		rawSeq, err := s.lastRawSeqFor(pk)
		if err != nil {
			return Report{}, err
		}
		valueBlob := []byte{}
		if !fs.Deleted {
			valueBlob, err = encodeValue(fs.Value)
			if err != nil {
				return Report{}, &DecodeError{PrimaryKey: pk, Err: err}
			}
		}
		if err := s.conn.exec(`INSERT INTO source(pk, source_rev, op, deleted, schema_id, value, value_sha, raw_seq, updated_at)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			pk, fs.SourceRevision, string(fs.Op), fs.Deleted, fs.SchemaID, valueBlob, fs.ValueSha, rawSeq, now); err != nil {
			return Report{}, &StorageError{Op: "rebuild source", Err: err}
		}
		if fs.Deleted {
			report.Deleted++
			continue
		}
		repairs := keep[pk]
		candidates, err := repairCandidates(fs.Value, repairs)
		if err != nil {
			return Report{}, err
		}
		row := Row{Op: fs.Op, SchemaID: fs.SchemaID, PrimaryKey: pk}
		j := judge(row, fs.SourceRevision, epoch, fs.Value, candidates, callback)
		if err := s.writeJudgement(w, pk, fs.SourceRevision, fs.Op, j); err != nil {
			return Report{}, err
		}
		if j.valid {
			report.Valid++
		} else {
			report.Invalid++
		}
	}
	version, err := s.bumpVersion()
	if err != nil {
		return Report{}, err
	}
	report.Version = version
	if err := s.conn.exec(`UPDATE meta SET value = ? WHERE key = ?`, fmt.Sprint(maxSourceRevision(rawRows)), metaAppliedRev); err != nil {
		return Report{}, &StorageError{Op: "update applied revision", Err: err}
	}
	return report, nil
}

func maxSourceRevision(entries []rawEntry) int64 {
	var max int64
	for _, e := range entries {
		if e.SourceRevision > max {
			max = e.SourceRevision
		}
	}
	return max
}

func (s *Store) loadRaw() ([]rawEntry, error) {
	rows, err := s.conn.query(`SELECT seq, source_rev, op, schema_id, encoding, pk, payload FROM raw ORDER BY seq`)
	if err != nil {
		return nil, &StorageError{Op: "read raw", Err: err}
	}
	defer rows.Close()
	var out []rawEntry
	for rows.Next() {
		out = append(out, rawEntry{
			Seq:            rows.Int64(0),
			SourceRevision: rows.Int64(1),
			Op:             Op(rows.Text(2)),
			SchemaID:       rows.Text(3),
			Encoding:       Encoding(rows.Text(4)),
			PrimaryKey:     rows.Text(5),
			Payload:        rows.Blob(6),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, &StorageError{Op: "read raw", Err: err}
	}
	return out, nil
}

func (s *Store) lastRawSeqFor(pk string) (int64, error) {
	rows, err := s.conn.query(`SELECT MAX(seq) FROM raw WHERE pk = ?`, pk)
	if err != nil {
		return 0, &StorageError{Op: "read raw sequence", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, &StorageError{Op: "read raw sequence", Err: rows.Err()}
	}
	return rows.Int64(0), nil
}

func (s *Store) allRepairs() ([]repairRecord, error) {
	rows, err := s.conn.query(`SELECT id, pk, work_order, source_rev, rule_epoch, kind, payload FROM repair ORDER BY id`)
	if err != nil {
		return nil, &StorageError{Op: "read repairs", Err: err}
	}
	defer rows.Close()
	var out []repairRecord
	for rows.Next() {
		out = append(out, repairRecord{
			ID:             rows.Int64(0),
			PrimaryKey:     rows.Text(1),
			WorkOrder:      rows.Text(2),
			SourceRevision: rows.Int64(3),
			RuleEpoch:      rows.Int64(4),
			Kind:           rows.Text(5),
			Payload:        rows.Blob(6),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, &StorageError{Op: "read repairs", Err: err}
	}
	return out, nil
}
