---
id: 0483
title: Limit DeltaCodec decompressed size and document its checksum scope
status: Complete
governing_adrs:
- ADR-0105
governing_prds:
- PRD-0001
governing_stories:
- US-0002
target_bc: adapters
priority_rank: 23
---

# TASK-0483: Limit DeltaCodec decompressed size and document its checksum scope

## Summary
`DeltaCodec.decompress` has no output limit and does not compare the output with `raw_size`. The CRC32 and SHA-256 values are stored beside the data, so they detect corruption only, not tampering.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-409 (decompression bomb)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

No store uses `DeltaCodec` today, so the risk applies only to applications that pass untrusted payloads to this public API.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/serialization/delta.py:118`, 122, 124-138
- `src/eventsource/adapters/serialization/delta.py:33`, 61 (raw_size not used)

**Verification**: Proven: 51 KB of compressed data with `raw_size=1` decompressed to 50 MiB without error.

## Definition of Done (Blackbox Frontdoor TDD)
1. Decompression stops at a codec-level limit and raises `DeltaIntegrityError`. It also raises when the output size is not `raw_size`.
2. The docstrings state that CRC32 and SHA-256 detect accidental corruption only.
3. A test with a crafted bomb payload expects `DeltaIntegrityError`.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
