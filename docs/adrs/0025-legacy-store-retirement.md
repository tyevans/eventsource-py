# ADR-0025: Legacy Store Surface Retirement

**Status:** Proposed

## Context

The `EventStore` ABC in `src/eventsource/stores/interface.py` is the legacy store surface. As of slice (b), a new port-based surface (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`) has been introduced to replace it. This ADR documents the multi-slice effort to migrate off the legacy surface and retire it from the public API entirely.

The full design is recorded in `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md`. The decision and consequences are filled in by slice (d).

## Supersedes

Nothing.

## Superseded by

Nothing.
