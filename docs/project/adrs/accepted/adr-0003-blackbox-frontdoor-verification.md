# ADR-0003: Blackbox Frontdoor Verification and Zero Backdoor Testing

## Status
Accepted

## Context
Tests that manipulate internal state via private backdoors (e.g., executing raw `INSERT INTO` queries in integration tests or spying on private methods) create brittle test suites. When internal implementations change, mock-heavy tests break even when public user behavior remains completely correct. Worse, backdoors bypass aggregate validation and authorization checks, masking production failure modes.

## Decision
We mandate **Blackbox Frontdoor Testing (Hard Invariant)**:
1. All tests must interact exclusively through public entrypoints: public REST endpoints, WebSockets, public domain events, or user interface forms.
2. Test setup (`Given`) must use public API fixtures or standard session injection; direct database manipulation in feature tests is strictly forbidden.
3. Assertions (`Then`) must verify observable outputs: rendered DOM elements, status codes, query projections, or emitted domain events.

## Consequences
- **Positive**: Tests verify true behavioral contracts; refactoring private internals does not break tests; eliminates false-positive mock pass rates.
- **Negative**: Setting up complex test states via public frontdoor APIs requires running supporting platform dependencies.
