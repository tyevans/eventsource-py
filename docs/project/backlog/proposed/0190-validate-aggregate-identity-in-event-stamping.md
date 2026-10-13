---
id: "0190"
title: Validate Aggregate Identity in Event Stamping
status: Proposed
target_bc: domain
governing_adrs:
  - ADR-0007
  - ADR-0056
  - ADR-0156
priority: P2
---

# TASK-0190: Validate Aggregate Identity in Event Stamping

## Summary
Enforce strict validation in `DeciderAggregate._stamp` to verify that an explicit event's `aggregate_id` matches the aggregate instance's `self.aggregate_id`, rejecting mismatched identifiers before stream append.

## Problem Statement & Context
- `_stamp` currently validates `aggregate_type` via `_reject_divergent_aggregate_type`, but does not verify that an event `aggregate_id` matches the aggregate instance `self.aggregate_id`.
- When a command specifies an explicit target identifier, mismatched events can append to the stream.
- This mismatch corrupts event stream integrity and breaks domain boundary encapsulation.

## Required Actions
1. Add an identity validation check in `DeciderAggregate._stamp` comparing any explicit event `aggregate_id` against `self.aggregate_id`.
2. Reject mismatched identifiers by raising `CommandRejectedError`.
3. Add unit and property-based tests verifying that `DeciderAggregate` systematically rejects commands or event payloads that produce mismatched aggregate identifiers.

## Definition of Done (Blackbox Frontdoor TDD)
1. Executing commands that produce events with an alien `aggregate_id` raises `CommandRejectedError`.
2. Domain event property invariants verify aggregate identity immutability.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
