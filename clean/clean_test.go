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
	"path/filepath"
	"strings"
	"testing"
	"time"

	goavro "github.com/linkedin/goavro/v2"
)

const testSchema = `{
	"type": "record",
	"name": "Product",
	"fields": [
		{"name": "id", "type": "string"},
		{"name": "name", "type": "string"},
		{"name": "qty", "type": "int"}
	]
}`

// testCodec compiles the test schema once per test binary.
func testCodec(tb testing.TB) *goavro.Codec {
	tb.Helper()
	codec, err := goavro.NewCodec(testSchema)
	if err != nil {
		tb.Fatalf("cannot compile test schema: %v", err)
	}
	return codec
}

// encodeRow encodes a record as binary Avro; the caller only supplies business
// fields. This is how a real producer would hand rows to the store.
func encodeRow(tb testing.TB, id, name string, qty interface{}) []byte {
	tb.Helper()
	codec := testCodec(tb)
	buf, err := codec.BinaryFromNative(nil, map[string]interface{}{
		"id": id, "name": name, "qty": qty,
	})
	if err != nil {
		tb.Fatalf("cannot encode row: %v", err)
	}
	return buf
}

// row builds an insert row for the test schema.
func row(tb testing.TB, op Op, id, name string, qty interface{}) Row {
	r := Row{Op: op, SchemaID: "products", PrimaryKey: id}
	if op != OpDelete {
		r.Payload = encodeRow(tb, id, name, qty)
	}
	return r
}

func recordField(tb testing.TB, v interface{}, field string) interface{} {
	tb.Helper()
	m, ok := v.(map[string]interface{})
	if !ok {
		tb.Fatalf("value is %T, want map", v)
	}
	return m[field]
}

// qtyAtLeastOne is the reference callback: a row is clean when its quantity is
// positive. It is pure and versioned like any real rule set.
func qtyAtLeastOne(view RowView) Decision {
	record, ok := view.Value.(map[string]interface{})
	if !ok {
		return Decision{Valid: false, Reason: "value is not a record", Code: "type"}
	}
	qty, ok := record["qty"].(int32)
	if !ok {
		return Decision{Valid: false, Reason: "qty is not an int", Code: "type"}
	}
	if qty < 1 {
		return Decision{Valid: false, Reason: "qty must be at least 1", Code: "qty"}
	}
	return Decision{Valid: true}
}

func openTestStore(tb testing.TB, schemas map[string]string) *Store {
	tb.Helper()
	if !Available() {
		tb.Skip("SQLite is unavailable in this build")
	}
	path := filepath.Join(tb.TempDir(), "clean.db")
	opts := Options{Path: path, Schemas: schemas}
	store, err := Open(opts)
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	return store
}

func mustApply(tb testing.TB, store *Store, batch Batch) Receipt {
	tb.Helper()
	receipt, err := store.ApplyBatch(batch)
	if err != nil {
		tb.Fatalf("ApplyBatch(%q): %v", batch.ID, err)
	}
	return receipt
}

func mustInstall(tb testing.TB, store *Store, id, version string, cb RowCallback) int64 {
	tb.Helper()
	epoch, err := store.InstallRules(Rules{ID: id, Version: version, Callback: cb})
	if err != nil {
		tb.Fatalf("InstallRules: %v", err)
	}
	return epoch
}

func wantErr(tb testing.TB, err error, target error) {
	tb.Helper()
	if !errors.Is(err, target) {
		tb.Fatalf("GOT %v, WANT %v", err, target)
	}
}

func TestValidRowLandsInTarget(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	receipt := mustApply(t, store, Batch{
		ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))},
	})
	if receipt.Valid != 1 || receipt.Invalid != 0 {
		t.Fatalf("GOT valid=%d invalid=%d, WANT 1/0", receipt.Valid, receipt.Invalid)
	}
	got, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target(p1): found=%v err=%v", found, err)
	}
	if got.Value.(map[string]interface{})["name"] != "widget" {
		t.Fatalf("GOT %v", got.Value)
	}
	if _, found, err := store.Quarantine("p1"); err != nil || found {
		t.Fatalf("quarantine should be empty, found=%v err=%v", found, err)
	}
}

func TestInvalidIsQuarantinedNotAnError(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	// First a valid revision so a target row exists, then an invalid correction
	// that must withdraw it.
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	receipt := mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p1", "widget", int32(0))}})
	if receipt.Invalid != 1 {
		t.Fatalf("GOT invalid=%d, WANT 1", receipt.Invalid)
	}
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("invalid row must withdraw target, found=%v err=%v", found, err)
	}
	q, found, err := store.Quarantine("p1")
	if err != nil || !found {
		t.Fatalf("Quarantine(p1): found=%v err=%v", found, err)
	}
	if q.Reason == "" || q.Code != "qty" {
		t.Fatalf("GOT reason=%q code=%q", q.Reason, q.Code)
	}
	// The original value is preserved, not the rejected patch.
	if recordField(t, q.Value, "qty") != int32(0) {
		t.Fatalf("quarantined value lost: %v", q.Value)
	}
	if q.Op != OpUpdate {
		t.Fatalf("GOT quarantine op %q, WANT U", q.Op)
	}
}

func TestDeleteWithdrawsAndNeverResurrects(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpDelete, "p1", "", nil)}})
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("target must be gone, found=%v err=%v", found, err)
	}
	if _, found, err := store.Quarantine("p1"); err != nil || found {
		t.Fatalf("quarantine must be gone, found=%v err=%v", found, err)
	}
	src, found, err := store.Source("p1")
	if err != nil || !found || !src.Deleted {
		t.Fatalf("source should be deleted: %+v found=%v err=%v", src, found, err)
	}

	// A repair cannot bring a deleted row back.
	_, err = store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 2, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(5)},
	})
	wantErr(t, err, ErrSourceDeleted)
}

func TestRepairBindingAndFirstValidWins(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})

	// Both repairs fix the row, but the first recorded one wins.
	res1, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	})
	if err != nil {
		t.Fatalf("AddRepair wo1: %v", err)
	}
	if !res1.Applied || res1.Winner != "wo1" {
		t.Fatalf("GOT %+v", res1)
	}
	res2, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo2",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(9)},
	})
	if err != nil {
		t.Fatalf("AddRepair wo2: %v", err)
	}
	if res2.Applied || res2.Winner != "wo1" {
		t.Fatalf("a later different repair must not override the first: %+v", res2)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	if target.WorkOrder != "wo1" || recordField(t, target.Value, "qty") != int32(3) {
		t.Fatalf("GOT workorder=%q value=%v", target.WorkOrder, target.Value)
	}

	// Retrying the same repair is idempotent.
	res3, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !res3.Idempotent || !res3.Applied {
		t.Fatalf("GOT %+v", res3)
	}
}

func TestOverridesMergeOverSource(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	res, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": int32(4)},
	})
	if err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	if !res.Applied {
		t.Fatalf("GOT %+v", res)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	if recordField(t, target.Value, "name") != "widget" || recordField(t, target.Value, "qty") != int32(4) {
		t.Fatalf("GOT %v", target.Value)
	}
}

func TestStaleRepairRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	// A source correction moves the row forward; a repair authored for the old
	// revision is stale.
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p1", "widget", int32(0))}})

	_, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	})
	wantErr(t, err, ErrStaleRepair)
}

func TestSourceCorrectionDropsOldRepair(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}

	// A corrected source value that is itself invalid must not inherit the old
	// repair: candidates are only live for the exact source revision.
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p1", "gadget", int32(0))}})
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("old repair must not pollute the new revision, found=%v err=%v", found, err)
	}
	q, found, err := store.Quarantine("p1")
	if err != nil || !found {
		t.Fatalf("Quarantine: found=%v err=%v", found, err)
	}
	if recordField(t, q.Value, "name") != "gadget" {
		t.Fatalf("quarantine kept the wrong source value: %v", q.Value)
	}
	if n, err := store.RepairCount("p1"); err != nil || n != 0 {
		t.Fatalf("GOT repairs=%d err=%v, WANT 0", n, err)
	}
}

func TestRuleReinstallInvalidatesRepairs(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}

	// Reinstalling the same identity keeps the epoch and the repair.
	epoch, err := store.InstallRules(Rules{ID: "rules", Version: "1", Callback: qtyAtLeastOne})
	if err != nil || epoch != 1 {
		t.Fatalf("same identity: GOT epoch=%d err=%v, WANT 1", epoch, err)
	}

	// A new generation bumps the epoch and makes the old repair inert.
	epoch2, err := store.InstallRules(Rules{ID: "rules", Version: "2", Callback: qtyAtLeastOne})
	if err != nil || epoch2 != 2 {
		t.Fatalf("new identity: GOT epoch=%d err=%v, WANT 2", epoch2, err)
	}
	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if !status.Stale || status.StaleRows == 0 {
		t.Fatalf("GOT stale=%v rows=%d, WANT stale", status.Stale, status.StaleRows)
	}

	report, err := store.Revalidate()
	if err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	if report.StaleRepairs != 1 {
		t.Fatalf("GOT stale repairs=%d, WANT 1", report.StaleRepairs)
	}
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("row must stay invalid after repairs are dropped, found=%v err=%v", found, err)
	}
	if stale, err := store.Stale(); err != nil || stale {
		t.Fatalf("GOT stale=%v err=%v after revalidate", stale, err)
	}
}

func TestRevalidateUpgradesFromRaw(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("should be quarantined first, found=%v err=%v", found, err)
	}

	// A new rule set that accepts zero quantities must rebuild from raw
	// without any new batch.
	mustInstall(t, store, "rules", "2", func(RowView) Decision { return Decision{Valid: true} })
	report, err := store.Revalidate()
	if err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	if report.Valid != 1 || report.Invalid != 0 {
		t.Fatalf("GOT valid=%d invalid=%d, WANT 1/0", report.Valid, report.Invalid)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	if target.RuleEpoch != 2 {
		t.Fatalf("GOT epoch=%d, WANT 2", target.RuleEpoch)
	}
	if _, found, err := store.Quarantine("p1"); err != nil || found {
		t.Fatalf("quarantine should be cleared, found=%v err=%v", found, err)
	}
}

func TestRevalidateHonoursRepairBoundToFinalRevision(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	// Another key moves the checkpoint, not p1's revision.
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpInsert, "p2", "gadget", int32(1))}})

	report, err := store.Revalidate()
	if err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	if report.StaleRepairs != 0 {
		t.Fatalf("GOT stale repairs=%d, WANT 0", report.StaleRepairs)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found || target.WorkOrder != "wo1" {
		t.Fatalf("GOT %+v found=%v err=%v", target, found, err)
	}
}

func TestRetryIsIdempotentAndDifferentContentConflicts(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	batch := Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}}
	first := mustApply(t, store, batch)
	second := mustApply(t, store, batch)
	if !second.Replayed || second.Version != first.Version {
		t.Fatalf("GOT replayed=%v version=%d, WANT replay of %d", second.Replayed, second.Version, first.Version)
	}
	if n, err := store.Status(); err != nil {
		t.Fatalf("Status: %v", err)
	} else if n.RawRows != 1 {
		t.Fatalf("retry appended raw twice: GOT %d rows", n.RawRows)
	}

	bad := Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "different", int32(5))}}
	_, err := store.ApplyBatch(bad)
	wantErr(t, err, ErrBatchConflict)

	// The same batch ID under another revision is different content too.
	other := Batch{ID: "b1", SourceRevision: 2, Rows: batch.Rows}
	_, err = store.ApplyBatch(other)
	wantErr(t, err, ErrBatchConflict)
}

func TestCallbackPanicRollsBackWholeBatch(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	cb := func(view RowView) Decision {
		if recordField(t, view.Value, "name") == "boom" {
			panic("boom")
		}
		return Decision{Valid: true}
	}
	mustInstall(t, store, "rules", "1", cb)

	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1, Rows: []Row{
		row(t, OpInsert, "p1", "ok", int32(1)),
		row(t, OpInsert, "p2", "boom", int32(1)),
	}})
	var panicErr *CallbackPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("GOT %v, WANT *CallbackPanicError", err)
	}
	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.RawRows != 0 || status.Sources != 0 || status.Targets != 0 {
		t.Fatalf("failed batch left state behind: %+v", status)
	}
	if status.Checkpoint.LastBatchID != "" {
		t.Fatalf("checkpoint advanced on failure: %+v", status.Checkpoint)
	}
}

func TestOutOfOrderBatchRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p2", "gadget", int32(5))}})
	wantErr(t, err, ErrOutOfOrder)
}

func TestDecodeFailureRejectsBatch(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1, Rows: []Row{{
		Op: OpInsert, SchemaID: "products", PrimaryKey: "p1",
		Payload: []byte{0xFF},
	}}})
	var decodeErr *DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("GOT %v, WANT *DecodeError", err)
	}
	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.RawRows != 0 {
		t.Fatalf("rejected batch appended raw: %+v", status)
	}
}

func TestUnknownSchemaRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1, Rows: []Row{{
		Op: OpInsert, SchemaID: "nope", PrimaryKey: "p1", Payload: []byte{0x02},
	}}})
	wantErr(t, err, ErrUnknownSchema)
}

func TestReopenRecognisesStaleAndKeepsState(t *testing.T) {
	if !Available() {
		t.Skip("SQLite is unavailable in this build")
	}
	path := filepath.Join(t.TempDir(), "clean.db")
	store, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	// Reinstall rules and close without revalidating: the stale flag must be
	// derived from committed state alone.
	if _, err := store.InstallRules(Rules{ID: "rules", Version: "2", Callback: qtyAtLeastOne}); err != nil {
		t.Fatalf("InstallRules: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	stale, err := reopened.Stale()
	if err != nil || !stale {
		t.Fatalf("GOT stale=%v err=%v after reopen, WANT true", stale, err)
	}
	target, found, err := reopened.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target after reopen: found=%v err=%v", found, err)
	}
	if recordField(t, target.Value, "qty") != int32(5) {
		t.Fatalf("GOT %v", target.Value)
	}
	// UseRules must match the persisted identity.
	wantErr(t, reopened.UseRules(Rules{ID: "rules", Version: "1", Callback: qtyAtLeastOne}), ErrRulesMismatch)
	if err := reopened.UseRules(Rules{ID: "rules", Version: "2", Callback: qtyAtLeastOne}); err != nil {
		t.Fatalf("UseRules: %v", err)
	}
}

func TestReopenRequiresRulesBeforeApplying(t *testing.T) {
	if !Available() {
		t.Skip("SQLite is unavailable in this build")
	}
	path := filepath.Join(t.TempDir(), "clean.db")
	store, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	_, err = reopened.ApplyBatch(Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	wantErr(t, err, ErrNoRules)
}

func TestDeletedKeyReuseStaysFresh(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2, Rows: []Row{row(t, OpDelete, "p1", "", nil)}})
	// Reuse the same primary key for a new, valid entity.
	mustApply(t, store, Batch{ID: "b3", SourceRevision: 3,
		Rows: []Row{row(t, OpInsert, "p1", "gadget", int32(7))}})

	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	if target.WorkOrder != "" || recordField(t, target.Value, "name") != "gadget" {
		t.Fatalf("reused key inherited the old repair: %+v", target)
	}
	if n, err := store.RepairCount("p1"); err != nil || n != 0 {
		t.Fatalf("GOT repairs=%d err=%v, WANT 0", n, err)
	}
}

func TestRawIsAppendOnly(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p1", "gadget", int32(5))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 2, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": int32(6)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	if _, err := store.Revalidate(); err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	raw, err := store.Raw("p1")
	if err != nil {
		t.Fatalf("Raw: %v", err)
	}
	if len(raw) != 2 {
		t.Fatalf("GOT %d raw rows, WANT 2", len(raw))
	}
	// Both original payloads remain byte-identical.
	if got := raw[0]; len(got) == 0 {
		t.Fatalf("raw payload was modified: %v", got)
	}
}

// failingDriver wraps the real driver and fails one matching statement, which
// lets a test prove that a storage failure mid-batch rolls the whole batch
// back rather than leaving a partial commit.
type failingDriver struct {
	inner  sqlDriver
	failOn string
	failed bool
}

func (d *failingDriver) exec(query string, args ...interface{}) error {
	if !d.failed && d.failOn != "" && strings.Contains(query, d.failOn) {
		d.failed = true
		return errors.New("injected storage failure")
	}
	return d.inner.exec(query, args...)
}

func (d *failingDriver) execSimple(sql string) error { return d.inner.execSimple(sql) }

func (d *failingDriver) query(query string, args ...interface{}) (rowIter, error) {
	return d.inner.query(query, args...)
}

func (d *failingDriver) close() error { return d.inner.close() }

func TestStorageFailureKeepsWholeBatch(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b0", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p0", "existing", int32(1))}})

	statusBefore, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}

	driver := &failingDriver{inner: store.conn, failOn: "INSERT INTO target"}
	store.conn = driver
	_, err = store.ApplyBatch(Batch{ID: "b1", SourceRevision: 2, Rows: []Row{
		row(t, OpInsert, "p1", "widget", int32(5)),
		row(t, OpInsert, "p2", "gadget", int32(5)),
	}})
	var storageErr *StorageError
	if !errors.As(err, &storageErr) {
		t.Fatalf("GOT %v, WANT *StorageError", err)
	}
	if !driver.failed {
		t.Fatalf("failure was never injected")
	}
	store.conn = driver.inner

	statusAfter, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if statusAfter.RawRows != statusBefore.RawRows ||
		statusAfter.Sources != statusBefore.Sources ||
		statusAfter.Targets != statusBefore.Targets ||
		statusAfter.Version != statusBefore.Version ||
		statusAfter.Checkpoint != statusBefore.Checkpoint {
		t.Fatalf("failed batch changed state:\nbefore=%+v\nafter=%+v", statusBefore, statusAfter)
	}
	// No receipt was written, so the same batch can be retried successfully.
	receipt := mustApply(t, store, Batch{ID: "b1", SourceRevision: 2, Rows: []Row{
		row(t, OpInsert, "p1", "widget", int32(5)),
		row(t, OpInsert, "p2", "gadget", int32(5)),
	}})
	if receipt.Replayed || receipt.Valid != 2 {
		t.Fatalf("GOT %+v", receipt)
	}
}

func TestInBatchContradictionRollsBack(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	// Insert then update the same key inside one revision is contradictory and
	// must reject the batch after the first row was already staged.
	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1, Rows: []Row{
		row(t, OpInsert, "p1", "widget", int32(5)),
		row(t, OpUpdate, "p1", "widget", int32(6)),
	}})
	wantErr(t, err, ErrInvalidBatch)
	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.RawRows != 0 || status.Sources != 0 || status.Targets != 0 {
		t.Fatalf("rejected batch left state behind: %+v", status)
	}
}

func TestUnknownSchemaVersionRejected(t *testing.T) {
	if !Available() {
		t.Skip("SQLite is unavailable in this build")
	}
	path := filepath.Join(t.TempDir(), "clean.db")
	store, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Simulate a database written by a future layout.
	if err := store.conn.exec(`UPDATE meta SET value = ? WHERE key = ?`, "99", metaSchemaVersion); err != nil {
		t.Fatalf("rewrite schema version: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}}); err == nil {
		t.Fatalf("Open accepted an unknown schema version")
	}
}

func TestRevalidateKeepsDeletedRowsDeleted(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpDelete, "p1", "", nil)}})
	report, err := store.Revalidate()
	if err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	if report.Deleted != 1 {
		t.Fatalf("GOT deleted=%d, WANT 1", report.Deleted)
	}
	src, found, err := store.Source("p1")
	if err != nil || !found || !src.Deleted {
		t.Fatalf("deleted row resurrected: %+v found=%v err=%v", src, found, err)
	}
	if _, found, err := store.Target("p1"); err != nil || found {
		t.Fatalf("target for a deleted row, found=%v err=%v", found, err)
	}
}

func TestRulesIdentityRequiredForInstall(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	if _, err := store.InstallRules(Rules{ID: "", Version: "1", Callback: qtyAtLeastOne}); err == nil {
		t.Fatalf("InstallRules accepted an empty identity")
	}
	if _, err := store.InstallRules(Rules{ID: "rules", Version: "1"}); !errors.Is(err, ErrNoRules) {
		t.Fatalf("GOT %v, WANT ErrNoRules", err)
	}
	// Nothing was installed, so a batch cannot run.
	_, err := store.ApplyBatch(Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	wantErr(t, err, ErrNoRules)
}

func TestStatusCountsAndCheckpoint(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 4,
		Rows: []Row{row(t, OpInsert, "p1", "good", int32(5)), row(t, OpInsert, "p2", "bad", int32(0))}})

	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.RawRows != 2 || status.Sources != 2 || status.Targets != 1 || status.Quarantined != 1 {
		t.Fatalf("GOT %+v", status)
	}
	if status.Checkpoint.LastBatchID != "b1" || status.Checkpoint.LastRevision != 4 {
		t.Fatalf("GOT %+v", status.Checkpoint)
	}
	if status.Version != status.Checkpoint.LastVersion || status.Version == 0 {
		t.Fatalf("version and checkpoint diverged: %+v", status)
	}
	if status.Stale || status.StaleRows != 0 {
		t.Fatalf("GOT stale=%v rows=%d, WANT fresh", status.Stale, status.StaleRows)
	}
}

func TestRepairRetryDoesNotAdvanceVersion(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})

	repair := Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}
	if _, err := store.AddRepair(repair); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	before, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	res, err := store.AddRepair(repair)
	if err != nil {
		t.Fatalf("retry AddRepair: %v", err)
	}
	if !res.Idempotent {
		t.Fatalf("retry should be idempotent: %+v", res)
	}
	after, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if after.Version != before.Version || after.StaleRows != 0 {
		t.Fatalf("retry changed the store: before=%d after=%d", before.Version, after.Version)
	}
}

func TestRepairThatDoesNotValidateStaysQuarantined(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})

	res, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(-5)},
	})
	if err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	if res.Applied || res.Valid || res.Winner != "" {
		t.Fatalf("GOT %+v", res)
	}
	q, found, err := store.Quarantine("p1")
	if err != nil || !found {
		t.Fatalf("Quarantine: found=%v err=%v", found, err)
	}
	if q.WorkOrder != "wo1" {
		t.Fatalf("quarantine should name the rejected work order, GOT %q", q.WorkOrder)
	}
}

func TestSourceAndTargetSurviveReopen(t *testing.T) {
	if !Available() {
		t.Skip("SQLite is unavailable in this build")
	}
	path := filepath.Join(t.TempDir(), "clean.db")
	store, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1, Rows: []Row{
		row(t, OpInsert, "p1", "widget", int32(5)),
		row(t, OpInsert, "p2", "gadget", int32(0)),
	}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p2", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": int32(2)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	before, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(Options{Path: path, Schemas: map[string]string{"products": testSchema}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if err := reopened.UseRules(Rules{ID: "rules", Version: "1", Callback: qtyAtLeastOne}); err != nil {
		t.Fatalf("UseRules: %v", err)
	}
	if err := reopened.UseRules(Rules{ID: "rules", Version: "1", Callback: qtyAtLeastOne}); err != nil {
		t.Fatalf("UseRules repeat: %v", err)
	}
	after, err := reopened.Status()
	if err != nil {
		t.Fatalf("Status after reopen: %v", err)
	}
	if after.Targets != before.Targets || after.Quarantined != before.Quarantined ||
		after.Repairs != before.Repairs || after.Version != before.Version ||
		after.Stale != before.Stale {
		t.Fatalf("reopen changed state:\nbefore=%+v\nafter=%+v", before, after)
	}
	repaired, found, err := reopened.Target("p2")
	if err != nil || !found || repaired.WorkOrder != "wo1" {
		t.Fatalf("GOT %+v found=%v err=%v", repaired, found, err)
	}
}

func TestEmptyAndMalformedBatchesRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	cases := []Batch{
		{ID: "", SourceRevision: 1, Rows: []Row{row(t, OpInsert, "p1", "w", int32(1))}},
		{ID: "b", SourceRevision: 0, Rows: []Row{row(t, OpInsert, "p1", "w", int32(1))}},
		{ID: "b", SourceRevision: 1},
		{ID: "b", SourceRevision: 1, Rows: []Row{{Op: "X", PrimaryKey: "p1"}}},
		{ID: "b", SourceRevision: 1, Rows: []Row{{Op: OpInsert, PrimaryKey: "", Payload: []byte{1}, SchemaID: "products"}}},
		{ID: "b", SourceRevision: 1, Rows: []Row{{Op: OpInsert, PrimaryKey: "p1", Payload: []byte{1}}}},
	}
	for i, batch := range cases {
		_, err := store.ApplyBatch(batch)
		if !errors.Is(err, ErrInvalidBatch) {
			t.Fatalf("case %d: GOT %v, WANT ErrInvalidBatch", i, err)
		}
	}
	if n, err := store.Status(); err != nil || n.RawRows != 0 {
		t.Fatalf("malformed batches wrote raw: %+v err=%v", n, err)
	}
}

func TestClosedStoreRejectsOperations(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err := store.ApplyBatch(Batch{ID: "b", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(1))}})
	wantErr(t, err, ErrClosed)
	if _, _, err := store.Target("p1"); !errors.Is(err, ErrClosed) {
		t.Fatalf("GOT %v, WANT ErrClosed", err)
	}
}

func TestReceiptCountsInvalidatedRepairs(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: map[string]interface{}{"id": "p1", "name": "widget", "qty": int32(3)},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	// A source correction invalidates the repair; the receipt must say so.
	receipt := mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p1", "widget", int32(0))}})
	if receipt.StaleRepairs != 1 {
		t.Fatalf("GOT stale=%d, WANT 1", receipt.StaleRepairs)
	}
}

func TestRepairForAnotherKeyDoesNotInvalidateOthers(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1, Rows: []Row{
		row(t, OpInsert, "p1", "widget", int32(0)),
		row(t, OpInsert, "p2", "gadget", int32(0)),
	}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": int32(2)},
	}); err != nil {
		t.Fatalf("AddRepair p1: %v", err)
	}
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p2", SourceRevision: 1, WorkOrder: "wo2",
		Overrides: map[string]interface{}{"qty": int32(4)},
	}); err != nil {
		t.Fatalf("AddRepair p2: %v", err)
	}
	// A new revision of p2 must not disturb p1's repair.
	receipt := mustApply(t, store, Batch{ID: "b2", SourceRevision: 2,
		Rows: []Row{row(t, OpUpdate, "p2", "gadget", int32(0))}})
	if receipt.StaleRepairs != 1 {
		t.Fatalf("GOT stale=%d, WANT 1", receipt.StaleRepairs)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found || target.WorkOrder != "wo1" {
		t.Fatalf("p1 lost its repair: %+v found=%v err=%v", target, found, err)
	}
	if _, found, err := store.Target("p2"); err != nil || found {
		t.Fatalf("p2 should be invalid after its repair went stale, found=%v err=%v", found, err)
	}
}

func TestJSONEncodedRowsDecode(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	receipt := mustApply(t, store, Batch{ID: "b1", SourceRevision: 1, Rows: []Row{{
		Op: OpInsert, SchemaID: "products", PrimaryKey: "p1", Encoding: EncodingJSON,
		Payload: []byte(`{"id":"p1","name":"widget","qty":5}`),
	}}})
	if receipt.Valid != 1 {
		t.Fatalf("GOT %+v", receipt)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found || recordField(t, target.Value, "qty") != int32(5) {
		t.Fatalf("GOT %+v found=%v err=%v", target, found, err)
	}
	// Revalidation must replay JSON rows with the encoding stored in raw.
	mustInstall(t, store, "rules", "2", qtyAtLeastOne)
	if _, err := store.Revalidate(); err != nil {
		t.Fatalf("Revalidate: %v", err)
	}
	if target, found, err := store.Target("p1"); err != nil || !found || recordField(t, target.Value, "qty") != int32(5) {
		t.Fatalf("GOT %+v found=%v err=%v", target, found, err)
	}
}

func TestListingAccessors(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 2, Rows: []Row{
		row(t, OpInsert, "p2", "gadget", int32(0)),
		row(t, OpInsert, "p1", "widget", int32(5)),
	}})

	targets, err := store.Targets()
	if err != nil {
		t.Fatalf("Targets: %v", err)
	}
	if len(targets) != 1 || targets[0].PrimaryKey != "p1" {
		t.Fatalf("GOT %+v", targets)
	}
	quarantined, err := store.Quarantined()
	if err != nil {
		t.Fatalf("Quarantined: %v", err)
	}
	if len(quarantined) != 1 || quarantined[0].PrimaryKey != "p2" || quarantined[0].Reason == "" {
		t.Fatalf("GOT %+v", quarantined)
	}
	checkpoint, err := store.Checkpoint()
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if checkpoint.LastBatchID != "b1" || checkpoint.LastRevision != 2 {
		t.Fatalf("GOT %+v", checkpoint)
	}
	if !store.Available() {
		t.Fatalf("Available() should be true for an open test store")
	}
}

func TestRegisterSchemaReplacesCodec(t *testing.T) {
	store := openTestStore(t, nil)
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	if err := store.RegisterSchema("products", testSchema); err != nil {
		t.Fatalf("RegisterSchema: %v", err)
	}
	// Registering a bad schema keeps the old one usable.
	if err := store.RegisterSchema("broken", `{"type":"nope"}`); err == nil {
		t.Fatalf("RegisterSchema accepted an invalid schema")
	}
	receipt := mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	if receipt.Valid != 1 {
		t.Fatalf("GOT %+v", receipt)
	}
}

func TestRatValuesSurviveStorage(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", rejectSource)

	r, err := RatFromString("-51/4")
	if err != nil {
		t.Fatalf("RatFromString: %v", err)
	}
	if _, err := RatFromString("1/0"); err == nil {
		t.Fatalf("RatFromString accepted a zero denominator")
	}
	if _, err := RatFromString("abc"); err == nil {
		t.Fatalf("RatFromString accepted garbage")
	}
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "missing", SourceRevision: 1, WorkOrder: "wo", Value: r,
	}); !errors.Is(err, ErrSourceMissing) {
		t.Fatalf("GOT %v, WANT ErrSourceMissing", err)
	}

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	res, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": int32(1), "price": r, "repaired": true},
	})
	if err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	if !res.Applied {
		t.Fatalf("GOT %+v", res)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	got, ok := recordField(t, target.Value, "price").(*big.Rat)
	if !ok || got.Cmp(r) != 0 {
		t.Fatalf("GOT %v, WANT %v", got, r)
	}
}

// rejectSource accepts only candidates that carry the "repaired" marker, which
// forces the repair candidate, not the source value, to win.
func rejectSource(view RowView) Decision {
	record, ok := view.Value.(map[string]interface{})
	if !ok {
		return Decision{Valid: false, Reason: "not a record", Code: "type"}
	}
	if record["repaired"] != true {
		return Decision{Valid: false, Reason: "source not repaired", Code: "unrepaired"}
	}
	return Decision{Valid: true}
}

func TestTypedContainersSurviveStorage(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", rejectSource)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})

	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{
			"tags":     []string{"a", "b"},
			"meta":     map[string]int{"n": 3},
			"raw":      []byte{1, 2, 3},
			"flag":     true,
			"f":        float64(1.5),
			"repaired": true,
		},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	record := target.Value.(map[string]interface{})
	if tags, ok := record["tags"].([]interface{}); !ok || len(tags) != 2 {
		t.Fatalf("GOT %T %v", record["tags"], record["tags"])
	}
	if meta, ok := record["meta"].(map[string]interface{}); !ok || meta["n"] != 3 {
		t.Fatalf("GOT %T %v", record["meta"], record["meta"])
	}
	if raw, ok := record["raw"].([]byte); !ok || len(raw) != 3 {
		t.Fatalf("GOT %T %v", record["raw"], record["raw"])
	}
	if record["flag"] != true || record["f"] != 1.5 {
		t.Fatalf("GOT %v", record)
	}
}

func TestUnsupportedRepairValueRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})

	_, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Value: struct{ A int }{1},
	})
	if err == nil || !strings.Contains(err.Error(), "cannot store value") {
		t.Fatalf("GOT %v, WANT unsupported-value error", err)
	}
	// Overrides on a non-record source are rejected, not silently ignored.
	_, err = store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"qty": 1},
		Value:     map[string]interface{}{"id": "p1"},
	})
	if !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("GOT %v, WANT ErrInvalidBatch when both Value and Overrides are set", err)
	}
	_, err = store.AddRepair(Repair{PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1"})
	if !errors.Is(err, ErrInvalidBatch) {
		t.Fatalf("GOT %v, WANT ErrInvalidBatch when neither Value nor Overrides is set", err)
	}
}

func TestCallbackReceivesVersionedView(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	var views []RowView
	cb := func(view RowView) Decision {
		views = append(views, view)
		return Decision{Valid: true}
	}
	epoch := mustInstall(t, store, "rules", "7", cb)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 3,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(1))}})
	if len(views) != 1 {
		t.Fatalf("GOT %d callbacks, WANT 1", len(views))
	}
	view := views[0]
	if view.Op != OpInsert || view.PrimaryKey != "p1" || view.SchemaID != "products" ||
		view.SourceRevision != 3 || view.RuleEpoch != epoch || view.Candidate != 0 || view.WorkOrder != "" {
		t.Fatalf("GOT %+v", view)
	}
}

func TestErrorTypes(t *testing.T) {
	base := errors.New("disk on fire")
	storage := &StorageError{Op: "append raw", Err: base}
	if !strings.Contains(storage.Error(), "storage failure during append raw") {
		t.Fatalf("GOT %q", storage.Error())
	}
	decode := &DecodeError{PrimaryKey: "p1", SchemaID: "products", Err: base}
	if !errors.Is(decode, base) {
		t.Fatalf("DecodeError should wrap its cause")
	}
	panicErr := &CallbackPanicError{PrimaryKey: "p1", Candidate: 2, WorkOrder: "wo1", Panic: "boom"}
	for _, want := range []string{"p1", "candidate 2", "boom"} {
		if !strings.Contains(panicErr.Error(), want) {
			t.Fatalf("GOT %q, WANT substring %q", panicErr.Error(), want)
		}
	}
}

func TestFloatValuesInRepairs(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", rejectSource)
	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(0))}})
	if _, err := store.AddRepair(Repair{
		PrimaryKey: "p1", SourceRevision: 1, WorkOrder: "wo1",
		Overrides: map[string]interface{}{"lat": float32(1.25), "lon": float64(2.5), "repaired": true},
	}); err != nil {
		t.Fatalf("AddRepair: %v", err)
	}
	target, found, err := store.Target("p1")
	if err != nil || !found {
		t.Fatalf("Target: found=%v err=%v", found, err)
	}
	record := target.Value.(map[string]interface{})
	if record["lat"] != float32(1.25) || record["lon"] != float64(2.5) {
		t.Fatalf("GOT %T %v / %T %v", record["lat"], record["lat"], record["lon"], record["lon"])
	}
}

func TestSameRevisionRetryInsideBatchIsRejected(t *testing.T) {
	store := openTestStore(t, map[string]string{"products": testSchema})
	mustInstall(t, store, "rules", "1", qtyAtLeastOne)

	mustApply(t, store, Batch{ID: "b1", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	// A second, differently-identified batch for the same revision and key is
	// accepted only when the content matches; different content is a conflict
	// with the source of truth.
	_, err := store.ApplyBatch(Batch{ID: "b2", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "other", int32(5))}})
	wantErr(t, err, ErrInvalidBatch)

	// Identical content for the same revision is a harmless redelivery: raw
	// records the bytes again (it is the append-only landing zone), while the
	// derived source and target stay single-valued.
	receipt := mustApply(t, store, Batch{ID: "b2", SourceRevision: 1,
		Rows: []Row{row(t, OpInsert, "p1", "widget", int32(5))}})
	if receipt.Valid != 1 {
		t.Fatalf("GOT %+v", receipt)
	}
	status, err := store.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Sources != 1 || status.Targets != 1 {
		t.Fatalf("redelivery duplicated derived state: %+v", status)
	}
	if targets, err := store.Targets(); err != nil || len(targets) != 1 {
		t.Fatalf("GOT %d targets err=%v, WANT 1", len(targets), err)
	}
}

func TestStoredValueRoundTrip(t *testing.T) {
	cases := []interface{}{
		nil,
		true,
		int(7),
		int32(-3),
		int64(1 << 40),
		float32(2.5),
		float64(-0.125),
		"hash",
		[]byte{9, 8},
		[]interface{}{int32(1), "two", nil},
		map[string]interface{}{"a": int32(1), "b": []interface{}{true}},
		time.Date(2026, 11, 19, 1, 2, 3, 4, time.UTC),
		big.NewRat(-3, 8),
		[]string{"x"},
		map[string]int{"n": 2},
	}
	for i, value := range cases {
		encoded, err := encodeValue(value)
		if err != nil {
			t.Fatalf("case %d: encode: %v", i, err)
		}
		decoded, err := decodeStoredValue(encoded)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		reencoded, err := encodeValue(decoded)
		if err != nil {
			t.Fatalf("case %d: re-encode: %v", i, err)
		}
		if string(reencoded) != string(encoded) {
			t.Fatalf("case %d: round trip is not stable\n GOT %v\nWANT %v", i, reencoded, encoded)
		}
	}
}

func TestCorruptStoredValueRejected(t *testing.T) {
	for i, blob := range [][]byte{
		{},
		{tagBool},
		{0xFE},
		{tagString, 0x05, 'a'},
		{tagArray, 0x02, tagNil},
		{tagMap, 0x01},
		{tagRat, 0x01, 'x', 0x01, '0'},
		{tagNil, 0x00},
	} {
		if _, err := decodeStoredValue(blob); err == nil {
			t.Fatalf("case %d: accepted corrupt blob %v", i, blob)
		}
	}
}
