# 0028. Strict Cutover and In-Phase Resync

## Status

Accepted.

## Context

`safe_lag_anchor` (`dual_write.py`) refuses to advance the sync-lag anchor
past an unabsorbed dual-write mirror failure -- a deliberate fail-closed
clamp, so a reported lag never understates how far the target actually is
from the source. The only thing that releases the clamp is
`DualWriteInterceptor.mark_copy_pass_complete`: the coordinator's
attestation that a bulk-copy pass which began after the interceptor was
installed ran to completion. Before this record, the bulk-copy loop in
`_run_bulk_copy` was the sole caller of `mark_copy_pass_complete` -- its
bounded catch-up rounds (`_MAX_CATCHUP_ROUNDS = 10`) absorb failures that
happen *during* `BULK_COPY`, but once the migration has transitioned to
`DUAL_WRITE` there was no way to run another attested pass. A transient
mirror failure after the copy finished clamped the anchor permanently, and
the only documented remedy was to abort the migration and restart it --
discarding the already-copied history and the routing state built up so
far, for what may have been a single dropped mirror write.

Separately, `MigrationCoordinator` accepts a `position_mapper` and
`MigrationConfig.position_mapping_enabled` documents a default of `True`,
but the flag was read nowhere in `src/`, and the coordinator's `BulkCopier`
construction never passed `position_mapper=` through. A default-path
migration therefore recorded no position mappings, which meant
`migrate_subscriptions=True` -- also a documented default -- silently
translated nothing. `BulkCopier` was already correct: with a mapper
attached it appends one event at a time so each target position can be
recorded, and `_write_batch`/`_write_batch_with_mapper` already branch on
whether `self._position_mapper is None`. The break was entirely in the
coordinator's wiring, not in the copier.

Both gaps share one root cause: there were two ways the coordinator could
end up constructing a `BulkCopier` (the automated bulk-copy path, and now
the operator-triggered resync this record adds), and nothing forced them
to agree on how.

A third, related gap: `MigrationConfig.cutover_max_lag_events` defaulted to
`100`, and `CutoverManager.execute_cutover` (`cutover.py:321`) let the
routing switch proceed with up to 100 source events `safe_lag_anchor` had
already proven absent from the target. Those events are not eventually
consistent -- writes are paused for the entire cutover and nothing in the
sequence copies the residue, so lag remaining at the moment routing flips
is events the target never receives while it becomes authoritative. The
only thing that caught this was a non-fatal post-cutover consistency
check, which contradicts `dual_write.py`'s own documented invariant: the
accepted failure mode is stuck-until-recopied, never a cutover over
missing data. A default that silently discards events on the happy path is
exactly backwards from that stance.

That default was also load-bearing for a bad reason: without a way to
recover a clamped lag anchor from inside `DUAL_WRITE`, a strict threshold
would have turned every transient mirror failure into a forced abort. The
100-event slack was doing the job `run_resync_pass` (above) should have
been doing.

## Decision

**One construction site.** `MigrationCoordinator._build_copier(migration,
target_store=None)` is now the only place `BulkCopier(...)` is
constructed. It resolves `target_store` from the coordinator's in-memory
`_target_stores` registry when the caller omits it (the shape a
resync call has, since it starts from only a migration id), and raises
`MigrationError` when neither an explicit store nor a registered one is
available -- the case a coordinator restart leaves behind, since that
registry does not survive one. It attaches `self._position_mapper` only
when the coordinator was given one **and**
`migration.config.position_mapping_enabled` is `True`. `_run_bulk_copy`'s
inline construction is replaced with a call through this helper, so the
automated path and any future copy entry point read the wiring the same
way by construction, not by convention.

**`run_resync_pass(migration_id) -> int`.** A new public method that runs
one bounded catch-up copy pass while the migration is in `DUAL_WRITE`.
Preconditions, checked in order: the migration exists
(`MigrationNotFoundError`); it is in `DUAL_WRITE`
(`MigrationStateError`); no copier is already active for it
(`MigrationError` -- a resync racing the tail of bulk-copy's own catch-up
rounds, or a second concurrent resync call, is refused rather than
silently interleaved); and a target store is available via `_build_copier`
(`MigrationError` if the registry does not have one). It then runs one
`_run_copy_pass`, and -- only if that pass completed -- calls
`interceptor.mark_copy_pass_complete(current.last_source_position)` with
the freshly re-read migration's checkpoint, exactly the attestation
contract `_run_bulk_copy`'s own loop already relies on. The method never
assigns `migration.phase`: `BulkCopier.run` only ever writes progress and
records errors, so a resync pass is safe to run inside `DUAL_WRITE`
without touching the `VALID_TRANSITIONS` state machine, and re-copying is
safe by construction because the copier treats an event already present in
the target as already copied.

If no interceptor is registered for the migration -- the coordinator
restart shape, since `_interceptors` is also in-memory -- the pass still
advances the persisted checkpoint, and the method returns `0` rather than
attempting to call a method on a `None` interceptor: there is no failure
list left to absorb, and `_lag_anchor` already falls back to the
checkpoint directly when it has no interceptor to consult.

The return value is the number of unabsorbed mirror failures remaining
after the pass. `0` means the lag anchor is unclamped. Bounding retries
across repeated non-zero returns is left to the caller: the library
exposes one pass per call rather than a retry loop, because how many
attempts are worth making, and at what interval, is an operational
judgment about whether the underlying mirror problem is transient -- not
something the library can decide on the caller's behalf.

**`cutover_max_lag_events` defaults to `0` (strict).** No cutover proceeds
while any source event is provably absent from the target. Zero is
*achievable* on the healthy path: writes are paused before the lag check
runs, so a mirror that is keeping up has already drained to exactly zero
by the time `execute_cutover` reads `lag.is_within_threshold`. Zero is
*recoverable* on the unhealthy path: a clamped anchor -- the only way a
healthy-looking mirror can still show nonzero lag -- has a remedy that
does not require aborting the migration, namely `run_resync_pass` from
this same record. Neither half makes the other sufficient on its own:
strict-0 without resync is stuck-until-abort the first time a mirror
write fails; resync without strict-0 leaves a default that still permits
cutover over a hole resync could have closed. Together they read as one
operational sequence -- run a resync pass, then cut over -- which is why
both halves belong in a single record.

The knob survives, non-default. `MigrationConfig(cutover_max_lag_events=N)`
for `N > 0` still works exactly as before: an explicit, documented
operator acceptance of up to `N` lost events at the switch, for cases
where availability of the cutover matters more than that bounded loss.

## Rejected Alternatives

**Automatic background resync.** A timer or lag-threshold trigger that
calls the equivalent of `run_resync_pass` on its own. Rejected because it
hides mirror instability from the operator instead of surfacing it --
repeated automatic passes silently paper over a mirror that is failing
continuously, which is exactly the situation an operator most needs to see
-- and because an unbounded automatic schedule contends with live
dual-write traffic without any operator-visible throttle. Bounding that
contention requires the same judgment call `run_resync_pass` already
leaves to the caller; automating it just moves the judgment somewhere less
visible.

**A public "absorb" API without a copy pass.** E.g. a method that clears
`_unabsorbed_failure_positions` directly, or accepts an operator-supplied
checkpoint to attest against. Rejected because it violates
`mark_copy_pass_complete`'s contract at the root: that method's safety
depends entirely on the coordinator attesting an actual completed copy
pass whose feed snapshot the interceptor's install window is known to
overlap. An API that lets an operator assert completion without a pass
having run would let a truly-missing event get marked absorbed, which is
the exact failure mode the clamp exists to prevent.

**Folding resync into `trigger_cutover` as a pre-pass.** Running one more
catch-up pass automatically at the start of cutover, instead of as a
separate operator-triggered call. Rejected because it couples a
potentially read-heavy bulk operation (streaming and re-verifying
historical events) into the cutover window, which this record's other half
(task 5) is tightening toward a strict, sub-second budget. A resync pass
belongs entirely inside `DUAL_WRITE`, before cutover is even attempted, not
folded into the phase that is supposed to be brief and low-risk.

**Keep the default at 100 and document the loss window.** Rejected because
a default that silently discards events on the happy path contradicts the
module's own documented stance -- `dual_write.py`'s stuck-until-recopied
invariant exists specifically to rule this out -- and because "documented"
does not change what happens at runtime for a caller who never reads the
docstring. Defaults should be safe; overrides should be loud. A safe
default that requires an explicit opt-in to accept loss satisfies both;
a lossy default with a comment does neither.

**Remove `cutover_max_lag_events` entirely.** Rejected, but not for the
same reason as the default question: an operator consciously trading a
bounded loss window for cutover availability -- e.g. an environment where
a stalled cutover is worse than losing a handful of low-value events -- is
a legitimate choice, not a bug to design away. Removing the knob would
take that choice away rather than making it explicit. Flipping the
default and keeping the knob does both: unsafe behavior requires an
explicit ask, and the ask remains available.

## Consequences

- Every migration that uses the documented defaults now actually records
  position mappings and gets working subscription checkpoint translation --
  previously silent no-ops both.
- A transient dual-write mirror failure discovered after `BULK_COPY`
  completes now has a remedy that does not discard migration progress:
  call `run_resync_pass` in a loop bounded by the caller's own policy.
- `_build_copier` becomes the one place future changes to `BulkCopier`
  construction need to land -- a second, divergent construction site would
  defeat the point of this record.
- A cutover that previously succeeded with residual lag now raises
  `CutoverLagError` and rolls back to `DUAL_WRITE` -- a breaking behavior
  change for any caller relying on the old 100-event slack. Restoring the
  old tolerance requires passing `MigrationConfig(cutover_max_lag_events=100)`
  explicitly.
- The failure mode this default flip is meant to prevent -- a cutover
  switching routing over provably-missing data -- is now caught before the
  switch instead of after it, by a non-fatal consistency check.

## ADR Impact

- ADR 0024 (Projection Persistence Ports) -- stands; unrelated to migration
  control-plane wiring.
- ADR 0025 (Legacy Store Retirement) -- stands; this record operates
  entirely within the existing dual-write and bulk-copy machinery that ADR
  0025's slice hardened, and does not change any of its watermark or
  saturation invariants.
- ADR 0027 (Schema Correctness Fixes) -- stands; unrelated schema-layer
  fixes.
