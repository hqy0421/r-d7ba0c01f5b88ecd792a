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
	"bytes"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
)

// ApplyBatch validates and applies every row of one batch.
//
// The whole batch is one SQLite transaction: raw rows, source revisions,
// target and quarantine rows, the receipt and the checkpoint are committed
// together. A retried batch ID with identical content returns the stored
// receipt and changes nothing; the same ID with different content is
// ErrBatchConflict. A panic in the callback becomes *CallbackPanicError and a
// SQLite failure becomes *StorageError; in both cases the transaction is
// rolled back and the store keeps its previous state.
func (s *Store) ApplyBatch(batch Batch) (Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return Receipt{}, err
	}
	if err := validateBatch(batch); err != nil {
		return Receipt{}, err
	}
	callback, err := s.currentCallback()
	if err != nil {
		return Receipt{}, err
	}
	work := func() (Receipt, error) {
		// Recognise a retry before decoding: a replayed batch must not fail
		// merely because a schema was since unregistered, and a batch ID reused
		// with different content must be a conflict.
		contentSha := contentDigest(batch)
		if receipt, found, err := s.existingReceipt(batch.ID, contentSha); err != nil {
			return Receipt{}, err
		} else if found {
			return receipt, nil
		}
		rows, err := s.prepareRows(batch)
		if err != nil {
			return Receipt{}, err
		}
		return s.applyPrepared(batch, rows, contentSha, callback)
	}
	return s.inTx("apply batch", work)
}

func validateBatch(batch Batch) error {
	if batch.ID == "" {
		return fmt.Errorf("%w: batch id is empty", ErrInvalidBatch)
	}
	if batch.SourceRevision <= 0 {
		return fmt.Errorf("%w: batch %q has non-positive source revision", ErrInvalidBatch, batch.ID)
	}
	if len(batch.Rows) == 0 {
		return fmt.Errorf("%w: batch %q has no rows", ErrInvalidBatch, batch.ID)
	}
	seen := make(map[string]struct{}, len(batch.Rows))
	for i, row := range batch.Rows {
		if !row.Op.valid() {
			return fmt.Errorf("%w: batch %q row %d has operation %q", ErrInvalidBatch, batch.ID, i, row.Op)
		}
		if row.PrimaryKey == "" {
			return fmt.Errorf("%w: batch %q row %d has empty primary key", ErrInvalidBatch, batch.ID, i)
		}
		if row.Op != OpDelete && len(row.Payload) == 0 {
			return fmt.Errorf("%w: batch %q row %d (%s) has empty payload", ErrInvalidBatch, batch.ID, i, row.Op)
		}
		if row.Op != OpDelete && row.SchemaID == "" {
			return fmt.Errorf("%w: batch %q row %d has empty schema id", ErrInvalidBatch, batch.ID, i)
		}
		if row.Encoding != EncodingBinary && row.Encoding != EncodingJSON {
			return fmt.Errorf("%w: batch %q row %d has encoding %q", ErrInvalidBatch, batch.ID, i, row.Encoding)
		}
		key := string(row.Op) + "\x00" + row.PrimaryKey
		if _, dup := seen[key]; dup {
			return fmt.Errorf("%w: batch %q repeats %s for %q", ErrInvalidBatch, batch.ID, row.Op, row.PrimaryKey)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// preparedRow is a batch row after its payload was decoded by the real Codec.
type preparedRow struct {
	row        Row
	value      interface{}
	encoded    []byte
	valueSha   string
	payload    []byte
	payloadSha string
}

// contentDigest is the identity of a batch's content. It is computed from the
// raw row bytes alone, so a retry is recognised before any decoding happens;
// the source revision is part of the identity, so the same batch ID against a
// different revision is a conflict rather than a silent no-op.
func contentDigest(batch Batch) string {
	digest := make([][]byte, 0, len(batch.Rows)*4+1)
	digest = append(digest, []byte(fmt.Sprint(batch.SourceRevision)))
	for _, row := range batch.Rows {
		digest = append(digest,
			[]byte(string(row.Op)), []byte(row.PrimaryKey), []byte(row.SchemaID),
			[]byte(row.Encoding), []byte(sha256Hex(row.Payload)))
	}
	return sha256Hex(digest...)
}

// prepareRows decodes every payload with the registered goavro Codec. It is
// called inside the transaction, after the receipt lookup, so a decode failure
// rolls the whole batch back and leaves the previous state untouched.
func (s *Store) prepareRows(batch Batch) ([]preparedRow, error) {
	out := make([]preparedRow, len(batch.Rows))
	for i, row := range batch.Rows {
		p := preparedRow{row: row, payload: row.Payload, payloadSha: sha256Hex(row.Payload)}
		if row.Op != OpDelete {
			codec, ok := s.codecs[row.SchemaID]
			if !ok {
				return nil, fmt.Errorf("%w: %q (row %d of batch %q)", ErrUnknownSchema, row.SchemaID, i, batch.ID)
			}
			var (
				native interface{}
				err    error
			)
			if row.Encoding == EncodingJSON {
				native, _, err = codec.NativeFromTextual(row.Payload)
			} else {
				native, _, err = codec.NativeFromBinary(row.Payload)
			}
			if err != nil {
				return nil, &DecodeError{PrimaryKey: row.PrimaryKey, SchemaID: row.SchemaID, Err: err}
			}
			encoded, err := encodeValue(native)
			if err != nil {
				return nil, &DecodeError{PrimaryKey: row.PrimaryKey, SchemaID: row.SchemaID, Err: err}
			}
			p.value = native
			p.encoded = encoded
			p.valueSha = sha256Hex(encoded)
		}
		out[i] = p
	}
	return out, nil
}

// decodeRawValue re-decodes one raw row with the Codec registered for its
// schema. Raw is the only source of truth for revalidation, so the encoding is
// taken from raw and never from the caller.
func (s *Store) decodeRawValue(entry rawEntry) (interface{}, error) {
	codec, ok := s.codecs[entry.SchemaID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownSchema, entry.SchemaID)
	}
	var (
		native interface{}
		err    error
	)
	if entry.Encoding == EncodingJSON {
		native, _, err = codec.NativeFromTextual(entry.Payload)
	} else {
		native, _, err = codec.NativeFromBinary(entry.Payload)
	}
	if err != nil {
		return nil, &DecodeError{PrimaryKey: entry.PrimaryKey, SchemaID: entry.SchemaID, Err: err}
	}
	return native, nil
}

func (s *Store) currentCallback() (RowCallback, error) {
	if s.rules == nil || s.rules.Callback == nil {
		return nil, ErrNoRules
	}
	return s.rules.Callback, nil
}

// txWork is a unit of work that produces a receipt inside one transaction.
type txWork func() (Receipt, error)

// inTx runs work inside BEGIN IMMEDIATE/COMMIT and rolls back on any failure,
// including a panic escaped from a callback. The recovered panic is converted
// into *CallbackPanicError so a caller sees a normal error, never a crashed
// process.
func (s *Store) inTx(op string, work txWork) (receipt Receipt, err error) {
	if err := s.conn.execSimple("BEGIN IMMEDIATE"); err != nil {
		return Receipt{}, &StorageError{Op: op, Err: err}
	}
	committed := false
	defer func() {
		if r := recover(); r != nil {
			_ = s.conn.execSimple("ROLLBACK")
			if cerr, ok := r.(*CallbackPanicError); ok {
				receipt, err = Receipt{}, cerr
				return
			}
			if serr, ok := r.(*StorageError); ok {
				receipt, err = Receipt{}, serr
				return
			}
			panic(r)
		}
		if !committed {
			_ = s.conn.execSimple("ROLLBACK")
		}
	}()
	receipt, err = work()
	if err != nil {
		return Receipt{}, err
	}
	if err := s.conn.execSimple("COMMIT"); err != nil {
		return Receipt{}, &StorageError{Op: op + " commit", Err: err}
	}
	committed = true
	return receipt, nil
}

// batchWork tracks per-batch state while rows are applied in order.
type batchWork struct {
	batch        Batch
	now          int64
	epoch        int64
	version      int64
	valid        int
	invalid      int
	deleted      int
	staleRepairs int
	rawIndex     int64
	lastRawSeq   int64
}

func (s *Store) applyPrepared(batch Batch, rows []preparedRow, contentSha string, callback RowCallback) (Receipt, error) {
	if receipt, found, err := s.existingReceipt(batch.ID, contentSha); err != nil {
		return Receipt{}, err
	} else if found {
		return receipt, nil
	}
	checkpoint, err := s.readCheckpoint()
	if err != nil {
		return Receipt{}, err
	}
	if batch.SourceRevision < checkpoint.LastRevision {
		return Receipt{}, fmt.Errorf("%w: batch %q revision %d < committed %d", ErrOutOfOrder, batch.ID, batch.SourceRevision, checkpoint.LastRevision)
	}

	epoch, err := s.readIntMeta(metaRuleEpoch)
	if err != nil {
		return Receipt{}, &StorageError{Op: "read rule epoch", Err: err}
	}
	version, err := s.readIntMeta(metaVersion)
	if err != nil {
		return Receipt{}, &StorageError{Op: "read version", Err: err}
	}
	w := &batchWork{
		batch:   batch,
		now:     s.stamp(),
		epoch:   epoch,
		version: version + 1,
	}
	for i := range rows {
		if err := s.applyRow(w, &rows[i], callback); err != nil {
			return Receipt{}, err
		}
	}
	stored := Receipt{
		BatchID:        batch.ID,
		SourceRevision: batch.SourceRevision,
		Version:        w.version,
		Rows:           len(rows),
		Valid:          w.valid,
		Invalid:        w.invalid,
		Deleted:        w.deleted,
		StaleRepairs:   w.staleRepairs,
	}
	if err := s.conn.exec(`INSERT INTO receipt(batch_id, content_sha, source_rev, row_count, valid_count, invalid_count, deleted_count, stale_repairs, version, applied_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		stored.BatchID, contentSha, stored.SourceRevision, stored.Rows, stored.Valid, stored.Invalid,
		stored.Deleted, stored.StaleRepairs, stored.Version, w.now); err != nil {
		return Receipt{}, &StorageError{Op: "insert receipt", Err: err}
	}
	if err := s.setIntMeta(metaVersion, w.version, "update version"); err != nil {
		return Receipt{}, err
	}
	if err := s.setIntMeta(metaAppliedRev, batch.SourceRevision, "update applied revision"); err != nil {
		return Receipt{}, err
	}
	if err := s.conn.exec(`UPDATE checkpoint SET last_batch_id = ?, last_source_rev = ?, last_version = ?, updated_at = ? WHERE id = 1`,
		batch.ID, batch.SourceRevision, w.version, w.now); err != nil {
		return Receipt{}, &StorageError{Op: "update checkpoint", Err: err}
	}
	return stored, nil
}

func (s *Store) setIntMeta(key string, value int64, op string) error {
	if err := s.conn.exec(`UPDATE meta SET value = ? WHERE key = ?`, fmt.Sprint(value), key); err != nil {
		return &StorageError{Op: op, Err: err}
	}
	return nil
}

func (s *Store) existingReceipt(batchID, contentSha string) (Receipt, bool, error) {
	rows, err := s.conn.query(`SELECT content_sha, source_rev, row_count, valid_count, invalid_count, deleted_count, stale_repairs, version
		FROM receipt WHERE batch_id = ?`, batchID)
	if err != nil {
		return Receipt{}, false, &StorageError{Op: "read receipt", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Receipt{}, false, &StorageError{Op: "read receipt", Err: err}
		}
		return Receipt{}, false, nil
	}
	if rows.Text(0) != contentSha {
		return Receipt{}, false, fmt.Errorf("%w: batch %q", ErrBatchConflict, batchID)
	}
	receipt := Receipt{
		BatchID:        batchID,
		SourceRevision: rows.Int64(1),
		Rows:           int(rows.Int64(2)),
		Valid:          int(rows.Int64(3)),
		Invalid:        int(rows.Int64(4)),
		Deleted:        int(rows.Int64(5)),
		StaleRepairs:   int(rows.Int64(6)),
		Version:        rows.Int64(7),
		Replayed:       true,
	}
	return receipt, true, nil
}

func (s *Store) applyRow(w *batchWork, p *preparedRow, callback RowCallback) error {
	row := p.row
	prev, prevFound, err := s.readSource(row.PrimaryKey)
	if err != nil {
		return err
	}
	if prevFound && prev.SourceRevision > w.batch.SourceRevision {
		return fmt.Errorf("%w: batch %q for %q is older than source revision %d", ErrOutOfOrder, w.batch.ID, row.PrimaryKey, prev.SourceRevision)
	}
	if prevFound && prev.SourceRevision == w.batch.SourceRevision && row.Op != OpDelete {
		// The same revision cannot be reused by a different operation for the
		// same key; that would make raw replay ambiguous.
		if prev.Op == OpDelete || prev.ValueSha != p.valueSha {
			return fmt.Errorf("%w: batch %q reuses source revision %d for %q", ErrInvalidBatch, w.batch.ID, w.batch.SourceRevision, row.PrimaryKey)
		}
	}

	if err := s.appendRaw(w, p); err != nil {
		return err
	}
	if row.Op == OpDelete {
		return s.applyDelete(w, row.PrimaryKey)
	}
	return s.applyUpsert(w, p, callback)
}

func (s *Store) appendRaw(w *batchWork, p *preparedRow) error {
	row := p.row
	if err := s.conn.exec(`INSERT INTO raw(batch_id, row_index, source_rev, op, schema_id, encoding, pk, payload, payload_sha, received_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		w.batch.ID, w.rawIndex, w.batch.SourceRevision, string(row.Op), row.SchemaID, string(row.Encoding),
		row.PrimaryKey, p.payload, p.payloadSha, w.now); err != nil {
		return &StorageError{Op: "append raw", Err: err}
	}
	w.rawIndex++
	rows, err := s.conn.query(`SELECT last_insert_rowid()`)
	if err != nil {
		return &StorageError{Op: "read raw sequence", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		return &StorageError{Op: "read raw sequence", Err: rows.Err()}
	}
	w.lastRawSeq = rows.Int64(0)
	return nil
}

// deleteRepairsFor removes every recorded repair for a key and reports how
// many were discarded, so a receipt accounts for all invalidated work orders.
func (s *Store) deleteRepairsFor(pk string) (int, error) {
	rows, err := s.conn.query(`SELECT COUNT(*) FROM repair WHERE pk = ?`, pk)
	if err != nil {
		return 0, &StorageError{Op: "count repairs", Err: err}
	}
	var n int
	if rows.Next() {
		n = int(rows.Int64(0))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, &StorageError{Op: "count repairs", Err: err}
	}
	if err := s.conn.exec(`DELETE FROM repair WHERE pk = ?`, pk); err != nil {
		return 0, &StorageError{Op: "invalidate repairs", Err: err}
	}
	return n, nil
}

func (s *Store) applyDelete(w *batchWork, pk string) error {
	if err := s.conn.exec(`INSERT INTO source(pk, source_rev, op, deleted, schema_id, value, value_sha, raw_seq, updated_at)
		VALUES(?, ?, 'D', 1, '', x'', ?, ?, ?)
		ON CONFLICT(pk) DO UPDATE SET source_rev = excluded.source_rev, op = 'D', deleted = 1,
			schema_id = '', value = excluded.value, value_sha = excluded.value_sha,
			raw_seq = excluded.raw_seq, updated_at = excluded.updated_at`,
		pk, w.batch.SourceRevision, emptyDigest, w.lastRawSeq, w.now); err != nil {
		return &StorageError{Op: "write source delete", Err: err}
	}
	if err := s.conn.exec(`DELETE FROM target WHERE pk = ?`, pk); err != nil {
		return &StorageError{Op: "withdraw target", Err: err}
	}
	if err := s.conn.exec(`DELETE FROM quarantine WHERE pk = ?`, pk); err != nil {
		return &StorageError{Op: "withdraw quarantine", Err: err}
	}
	n, err := s.deleteRepairsFor(pk)
	if err != nil {
		return err
	}
	w.staleRepairs += n
	w.deleted++
	return nil
}

func (s *Store) applyUpsert(w *batchWork, p *preparedRow, callback RowCallback) error {
	row := p.row
	prev, prevFound, err := s.readSource(row.PrimaryKey)
	if err != nil {
		return err
	}
	sameRevision := prevFound && prev.SourceRevision == w.batch.SourceRevision && prev.Op != OpDelete
	if prevFound && !sameRevision {
		// A new source revision (a correction or a reuse of the key after a
		// delete) makes every manual repair authored for the old version
		// stale. Remove them before the new version can be judged.
		n, err := s.deleteRepairsFor(row.PrimaryKey)
		if err != nil {
			return err
		}
		w.staleRepairs += n
	}
	if err := s.conn.exec(`INSERT INTO source(pk, source_rev, op, deleted, schema_id, value, value_sha, raw_seq, updated_at)
		VALUES(?, ?, ?, 0, ?, ?, ?, ?, ?)
		ON CONFLICT(pk) DO UPDATE SET source_rev = excluded.source_rev, op = excluded.op, deleted = 0,
			schema_id = excluded.schema_id, value = excluded.value, value_sha = excluded.value_sha,
			raw_seq = excluded.raw_seq, updated_at = excluded.updated_at`,
		row.PrimaryKey, w.batch.SourceRevision, string(row.Op), row.SchemaID, p.encoded, p.valueSha, w.lastRawSeq, w.now); err != nil {
		return &StorageError{Op: "write source", Err: err}
	}

	repairs, err := s.liveRepairs(row.PrimaryKey, w.batch.SourceRevision, w.epoch)
	if err != nil {
		return err
	}
	candidates, err := repairCandidates(p.value, repairs)
	if err != nil {
		return err
	}
	j := judge(row, w.batch.SourceRevision, w.epoch, p.value, candidates, callback)
	return s.writeJudgement(w, row.PrimaryKey, w.batch.SourceRevision, row.Op, j)
}

// candidate materialises one repair's value for judging.
type candidate struct {
	workOrder string
	value     interface{}
}

// judge runs the current callback over the source value and then over every
// live repair in recorded order; the first valid candidate wins. A verdict is
// a normal outcome, never an error.
func judge(row Row, sourceRev, epoch int64, sourceValue interface{}, candidates []candidate, callback RowCallback) judgement {
	view := RowView{
		Op:             row.Op,
		PrimaryKey:     row.PrimaryKey,
		SchemaID:       row.SchemaID,
		SourceRevision: sourceRev,
		RuleEpoch:      epoch,
		Value:          sourceValue,
		Candidate:      0,
	}
	decision := callCallback(callback, view)
	if decision.Valid {
		return judgement{valid: true, value: sourceValue}
	}
	last := decision
	lastWorkOrder := ""
	for i := range candidates {
		view.Candidate = i + 1
		view.Value = candidates[i].value
		view.WorkOrder = candidates[i].workOrder
		d := callCallback(callback, view)
		if d.Valid {
			return judgement{valid: true, value: candidates[i].value, winner: candidates[i].workOrder}
		}
		last = d
		lastWorkOrder = candidates[i].workOrder
	}
	return judgement{valid: false, value: sourceValue, reason: last.Reason, code: normalizeCode(last.Code), rejectedWorkOrder: lastWorkOrder}
}

// callCallback invokes the pure row callback and converts a panic into a
// *CallbackPanicError, which the transaction wrapper turns into a rollback.
func callCallback(callback RowCallback, view RowView) (decision Decision) {
	defer func() {
		if r := recover(); r != nil {
			panic(&CallbackPanicError{
				PrimaryKey: view.PrimaryKey,
				Candidate:  view.Candidate,
				WorkOrder:  view.WorkOrder,
				Panic:      r,
				Stack:      debug.Stack(),
			})
		}
	}()
	return callback(view)
}

// normalizeCode supplies a stable default for an invalid verdict's code.
func normalizeCode(code string) string {
	code = strings.TrimSpace(code)
	if code == "" {
		return "invalid"
	}
	return code
}

// judgement is the resolved outcome for one row.
type judgement struct {
	valid  bool
	value  interface{}
	winner string
	// rejectedWorkOrder names the last repair that was tried and rejected.
	rejectedWorkOrder string
	reason            string
	code              string
}

// judgementAlreadyCommitted reports whether the exact outcome j is already
// stored for this key, revision, epoch and op, so that replaying a repair does
// not write the same rows again or advance the version.
func (s *Store) judgementAlreadyCommitted(w *batchWork, pk string, sourceRev int64, op Op, j judgement) (bool, error) {
	if j.valid {
		encoded, err := encodeValue(j.value)
		if err != nil {
			return false, &DecodeError{PrimaryKey: pk, Err: err}
		}
		rows, err := s.conn.query(`SELECT source_rev, rule_epoch, value, work_order FROM target WHERE pk = ?`, pk)
		if err != nil {
			return false, &StorageError{Op: "read target", Err: err}
		}
		defer rows.Close()
		if !rows.Next() {
			return false, rows.Err()
		}
		return rows.Int64(0) == sourceRev && rows.Int64(1) == w.epoch &&
			bytes.Equal(rows.Blob(2), encoded) && rows.Text(3) == j.winner, nil
	}
	rows, err := s.conn.query(`SELECT source_rev, rule_epoch, op, value, reason, code, work_order FROM quarantine WHERE pk = ?`, pk)
	if err != nil {
		return false, &StorageError{Op: "read quarantine", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		return false, rows.Err()
	}
	encoded, err := encodeValue(j.value)
	if err != nil {
		return false, &DecodeError{PrimaryKey: pk, Err: err}
	}
	quarantineOp := op
	if quarantineOp == "" {
		quarantineOp = OpUpdate
	}
	reason := j.reason
	if reason == "" {
		reason = "rejected by cleaning rules"
	}
	code := j.code
	if code == "" {
		code = "invalid"
	}
	return rows.Int64(0) == sourceRev && rows.Int64(1) == w.epoch &&
		rows.Text(2) == string(quarantineOp) && bytes.Equal(rows.Blob(3), encoded) &&
		rows.Text(4) == reason && rows.Text(5) == code &&
		rows.Text(6) == j.rejectedWorkOrder, nil
}

func (s *Store) writeJudgement(w *batchWork, pk string, sourceRev int64, op Op, j judgement) error {
	encoded, err := encodeValue(j.value)
	if err != nil {
		return &DecodeError{PrimaryKey: pk, Err: err}
	}
	if j.valid {
		if err := s.conn.exec(`INSERT INTO target(pk, source_rev, rule_epoch, value, work_order, updated_at)
			VALUES(?, ?, ?, ?, ?, ?)
			ON CONFLICT(pk) DO UPDATE SET source_rev = excluded.source_rev, rule_epoch = excluded.rule_epoch,
				value = excluded.value, work_order = excluded.work_order, updated_at = excluded.updated_at`,
			pk, sourceRev, w.epoch, encoded, j.winner, w.now); err != nil {
			return &StorageError{Op: "write target", Err: err}
		}
		if err := s.conn.exec(`DELETE FROM quarantine WHERE pk = ?`, pk); err != nil {
			return &StorageError{Op: "clear quarantine", Err: err}
		}
		w.valid++
		return nil
	}
	// Invalid: keep the original value and reason in quarantine, and withdraw
	// any clean row a previous revision or rule set had produced.
	if err := s.conn.exec(`DELETE FROM target WHERE pk = ?`, pk); err != nil {
		return &StorageError{Op: "withdraw target", Err: err}
	}
	reason := j.reason
	if reason == "" {
		reason = "rejected by cleaning rules"
	}
	code := j.code
	if code == "" {
		code = "invalid"
	}
	quarantineOp := op
	if quarantineOp == "" {
		quarantineOp = OpUpdate
	}
	if err := s.conn.exec(`INSERT INTO quarantine(pk, source_rev, rule_epoch, op, value, reason, code, work_order, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pk) DO UPDATE SET source_rev = excluded.source_rev, rule_epoch = excluded.rule_epoch,
			op = excluded.op, value = excluded.value, reason = excluded.reason, code = excluded.code,
			work_order = excluded.work_order, updated_at = excluded.updated_at`,
		pk, sourceRev, w.epoch, string(quarantineOp), encoded, reason, code, j.rejectedWorkOrder, w.now); err != nil {
		return &StorageError{Op: "write quarantine", Err: err}
	}
	w.invalid++
	return nil
}

// sourceRecord mirrors one row of the derived source table.
type sourceRecord struct {
	SourceRevision int64
	Op             Op
	Deleted        bool
	SchemaID       string
	Value          interface{}
	ValueSha       string
}

func (s *Store) readSource(pk string) (sourceRecord, bool, error) {
	rows, err := s.conn.query(`SELECT source_rev, op, deleted, schema_id, value, value_sha FROM source WHERE pk = ?`, pk)
	if err != nil {
		return sourceRecord{}, false, &StorageError{Op: "read source", Err: err}
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return sourceRecord{}, false, &StorageError{Op: "read source", Err: err}
		}
		return sourceRecord{}, false, nil
	}
	rec := sourceRecord{
		SourceRevision: rows.Int64(0),
		Op:             Op(rows.Text(1)),
		Deleted:        rows.Int64(2) != 0,
		SchemaID:       rows.Text(3),
		ValueSha:       rows.Text(5),
	}
	if !rec.Deleted {
		v, err := decodeStoredValue(rows.Blob(4))
		if err != nil {
			return sourceRecord{}, false, &StorageError{Op: "decode source", Err: err}
		}
		rec.Value = v
	}
	return rec, true, nil
}

// repairRecord is one stored manual repair.
type repairRecord struct {
	ID             int64
	PrimaryKey     string
	WorkOrder      string
	SourceRevision int64
	RuleEpoch      int64
	Kind           string
	Payload        []byte
}

// liveRepairs returns the repairs that match exactly this source revision and
// rule epoch, in the order they were recorded.
func (s *Store) liveRepairs(pk string, sourceRev, epoch int64) ([]repairRecord, error) {
	rows, err := s.conn.query(`SELECT id, pk, work_order, source_rev, rule_epoch, kind, payload
		FROM repair WHERE pk = ? AND source_rev = ? AND rule_epoch = ? ORDER BY id`, pk, sourceRev, epoch)
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
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// repairCandidates resolves each live repair into a concrete native value,
// applying override patches over the source record.
func repairCandidates(sourceValue interface{}, repairs []repairRecord) ([]candidate, error) {
	out := make([]candidate, 0, len(repairs))
	for i := range repairs {
		rec := &repairs[i]
		switch rec.Kind {
		case "value":
			v, err := decodeStoredValue(rec.Payload)
			if err != nil {
				return nil, err
			}
			out = append(out, candidate{workOrder: rec.WorkOrder, value: v})
		case "overrides":
			decoded, err := decodeStoredValue(rec.Payload)
			if err != nil {
				return nil, err
			}
			patch, ok := decoded.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("%w: overrides are %T, not a record", ErrRepairValue, decoded)
			}
			merged, err := mergeOverrides(sourceValue, patch)
			if err != nil {
				return nil, err
			}
			out = append(out, candidate{workOrder: rec.WorkOrder, value: merged})
		default:
			return nil, fmt.Errorf("%w: repair kind %q", ErrRepairValue, rec.Kind)
		}
	}
	return out, nil
}
