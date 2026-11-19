// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// Package clean turns a stream of versioned Avro change rows (insert, update
// and delete) into a SQLite "clean" store.
//
// The package deliberately builds on goavro's real Codec: every row payload is
// decoded with github.com/linkedin/goavro/v2 and the resulting native value is
// what the cleaning callback and the target table see.  No re-implementation
// of the Avro wire format, and no mock decoder, is involved.
//
// # Model
//
// The store keeps six kinds of state, and every mutation of them is committed
// in a single SQLite transaction:
//
//	raw         append-only landing of source bytes; never updated or deleted
//	source      the current source revision of each primary key (derived from raw)
//	target      the clean table: rows that are valid under the current rules
//	quarantine  rejected rows, with their original value and the reason
//	receipt     one idempotency receipt per applied batch, carrying a version
//	checkpoint  the batch stream breakpoint (last revision/sequence/version)
//
// # Batches
//
// A caller supplies a Batch with a stable ID and a SourceRevision.  Each row
// names its operation (I/U/D), the schema ID used to encode its payload and
// the payload itself.  ApplyBatch decodes every row with the registered Codec
// and asks the installed, versioned, pure row callback what to do with it:
//
//   - Valid rows are written to target.
//   - Invalid rows are written to quarantine together with the original value
//     and the reason, and any previously clean target row for that primary
//     key is withdrawn.  An invalid row is a normal outcome: ApplyBatch
//     returns a receipt, never an error, for it.
//   - D rows delete the target and quarantine entries.  A repair can never
//     bring a deleted row back.
//
// # Manual repairs
//
// A Repair is a manual fix tied to a work order, the source revision it was
// authored against, and the rule installation epoch current at that time.
// Candidates for one row are evaluated through the *current* callback in a
// deterministic order: the decoded source value first, then the repairs in the
// order they were recorded.  The first candidate that validates wins.  A
// repair recorded for an older source revision or an older rule epoch is
// stale: AddRepair rejects it, and revalidation ignores it.  Therefore a
// correction of the source (a newer revision), a deletion, a reuse of the same
// primary key by a later revision, or a rule reinstallation all invalidate old
// manual fixes before they can pollute the new version.  The first winning
// repair writes its work order onto the target row; later, different repairs
// for the same revision and epoch never overwrite it, and retries are
// idempotent.
//
// # Rules, upgrades and staleness
//
// Rules are installed with InstallRules, which increments the persisted rule
// epoch; re-installing invalidates every repair bound to an older epoch.
// UseRules binds the callback after a reopen without changing the epoch.  When
// the installed epoch is newer than the epoch recorded on target/quarantine
// rows the store is stale.  Staleness survives Close/Open because it is
// derived from committed state.  Revalidate replays every raw row through the
// current callback ("upgrade re-validates from raw") and restores a
// non-stale store.
//
// # Failure behaviour
//
// ApplyBatch, AddRepair and Revalidate run in one transaction each.  A panic
// inside the callback is recovered and reported as *CallbackPanicError; any
// storage error is reported as an error. In both cases the transaction is
// rolled back and the whole batch keeps the previous state, so a batch is
// either fully applied or not applied at all.  The database schema is fixed:
// the on-disk layout is versioned with meta.schema_version and never migrated
// implicitly.
//
// The package requires cgo and the system SQLite library on Linux. On other
// builds Available reports false, Open returns ErrSQLiteUnavailable, and the
// package still compiles so that users of goavro's existing API are
// unaffected.
package clean
