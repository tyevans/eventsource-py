# SpecOps Multi-Agent SDLC Orchestration Protocol

This document defines the multi-agent coordination protocol used by the SpecOps inline orchestrator. It establishes how autonomous subagents are dispatched, how they consult living specifications, and how failures are treated as first-class actionable work items.

---

## 1. Core Principles

1. **Inline Coordination (No Blind Process Forking)**:
   - The primary orchestrator runs directly within the active agent session.
   - Subagents are invoked via native agent tools (`invoke_subagent`, `send_message`) rather than detached background shell forks. This preserves context, allows dynamic intervention, and keeps the lead orchestrator informed.

2. **Living Specification as Ground Truth**:
   - All subagents must consult version-locked PMaC specifications in `docs/project/` before making architectural or code modifications:
     - Personas: `docs/project/user_stories/PERSONAS.md`
     - PRDs: `docs/project/product/accepted/`
     - User Stories: `docs/project/user_stories/accepted/`
     - ADRs: `docs/project/adrs/accepted/`
     - Backlog: `docs/project/backlog/`

3. **Strict Invariant Governance**:
   - Every subagent must honor hard invariants:
     - File length <500 lines (proactive warning at >=400 lines) per ADR-0002.
     - Blackbox frontdoor verification (ADR-0003, ADR-0006) without private backdoor mocks.
     - Strict backlog isolation on feature branches (ADR-0005).
     - Lockfile immutability and allowlisted commands.

4. **Actionable Orchestration Failure Protocol**:
   - When running on this project, any orchestration failure (subagent stall, tool error, spec ambiguity, preflight failure) is an **actionable task**:
     - Document the failure immediately as a high-priority bug in `docs/project/backlog/proposed/`.
     - Dispatch remediation to resolve the root cause before moving forward.

5. **Autonomous Continuous Balancing Loop**:
   - Maintain continuous development velocity across PRD value, BDD user journeys, ADR architectural invariants, and Diataxis documentation without waiting for manual step-by-step steering. Governed by [Continuous Balancing Loop](./balancing_loop.md).

---

## 2. Specialized Subagent Archetypes

When orchestrating across the SDLC, the lead agent dispatches specialized subagents:

| Role Name | Scope | Primary Inputs | Primary Outputs |
|---|---|---|---|
| `persona-agent` | Persona discovery & maintenance | User feedback, market shifts, codebase evolution | Updates to `docs/project/user_stories/PERSONAS.md` |
| `prd-agent` | Product discovery & PRD authoring | Unmet user needs, persona pain points | PRD draft in `docs/project/product/accepted/` |
| `story-agent` | Multi-faceted BDD user stories | Accepted PRD, personas, bounded contexts | Gherkin stories in `docs/project/user_stories/accepted/` |
| `slicing-agent` | INVEST task decomposition & spikes | User stories, architecture seams | Proposed tasks in `docs/project/backlog/proposed/` |
| `refinement-agent` | JIT backlog curation & DoR validation | Proposed queue, living ADRs, AST seams | Refined tasks in `docs/project/backlog/refined/` |
| `implementation-agent` | In-worktree TDD implementation | Refined task, governing ADRs/stories | Code changes, tests, commit trailers in worktree |
| `review-agent` | Architectural verification & preflight | Task diff, test suite, security policies | Verification report, preflight sign-off |

---

## 3. Step-by-Step Lifecycle Workflow

### Phase 1: Persona Maintenance
1. Dispatch `persona-agent` to evaluate existing personas in `docs/project/user_stories/PERSONAS.md`.
2. Ask: Are new archetypes emerging (e.g. security officers, non-technical PMs, external API consumers)? Are existing pain points addressed or evolving?
3. Update `PERSONAS.md` atomically with clear roles, pain points, and goals.

### Phase 2: Product Discovery & Living PRDs
1. Dispatch `prd-agent` to draft or shape a PRD under `docs/project/product/accepted/`.
2. Include: "Who this is for", "What the person cannot do today", "What good looks like", and "Checkable Outcomes".
3. Verify PRD validity: `uv run spec-ops prd lint docs/project/product/accepted/PRD-XXXX.md`.
4. Register in `docs/project/product/REGISTRY.md`.

### Phase 3: BDD User Story Generation
1. Dispatch `story-agent` to author multi-faceted user stories:
   - By Persona: Grounded in target archetypes.
   - By Bounded Context: Aligned with domain architecture (ADR-0007).
   - By Feature: Clear vertical slices.
2. Author executable Gherkin scenarios (`Given ... When ... Then`) verifiable via public frontdoors.
3. Save in `docs/project/user_stories/accepted/us-XXXX-....md` and register in `REGISTRY.md`.

### Phase 4: INVEST Task Decomposition
1. Dispatch `slicing-agent` to decompose stories into thin vertical slices (<500 lines per file):
   - Independent, Negotiable, Valuable, Estimable, Small, Testable.
   - If technical uncertainty exists, scaffold an architectural spike (`SPIKE-XXXX`).
2. Scaffold tasks using `uv run spec-ops task create` into `docs/project/backlog/proposed/`.

### Phase 5: JIT Refinement & Definition of Ready
1. Maintain a ready buffer of ~10 tasks in `docs/project/backlog/refined/`.
2. Run cognitive curation: `uv run spec-ops curate --infer`.
3. Validate Definition of Ready (DoR):
   - Dependencies mapped.
   - Governing PRDs, stories, and ADRs cited.
   - Executable Gherkin scenarios defined.
   - Property invariants and mutation targets identified.

### Phase 6: Subagent Implementation & Peer Consultation
1. Create isolated task worktree: `uv run spec-ops worktree start TASK-XXXX`.
2. Dispatch `implementation-agent` inside worktree.
3. Subagent must:
   - Consult cited ADRs, PRDs, and stories in `docs/project/`.
   - Write tests first (TDD) through public frontdoors.
   - Maintain file length <500 lines.
   - Keep commits structured with RFC 822 trailers (`SpecOps-Task: TASK-XXXX`).

### Phase 7: Verification, Preflight & Integration
1. Run preflight verification:
   ```bash
   uv run spec-ops health
   uv run pytest
   uv lock --check
   ```
2. Open pull request or perform merge under `MERGE_LOCK`.
3. Synchronize `PRIORITY.md` and transition task to `complete/` on `main`.

---

## 4. Subagent Specification & Peer Consultation Guidelines

### 4.1. Approved Specification Consultation Protocol (`docs/project/`)
Subagents must consult version-locked PMaC specifications in `docs/project/` in strict hierarchical sequence before implementing changes:
1. **Persona Grounding** (`docs/project/user_stories/PERSONAS.md`): Identify target user archetypes and pain points.
2. **Living PRDs** (`docs/project/product/accepted/*.md`): Verify checkable outcomes and business value commitments.
3. **Executable User Stories** (`docs/project/user_stories/accepted/*.md`): Extract BDD Gherkin acceptance criteria (`Given ... When ... Then`).
4. **Governing ADRs** (`docs/project/adrs/accepted/*.md`): Enforce non-negotiable architectural invariants (ADR-0002 file length, ADR-0003 frontdoor TDD, ADR-0005 backlog isolation, ADR-0007 bounded contexts, ADR-0009 property/mutation testing).
5. **Backlog & Priority** (`docs/project/backlog/refined/`, `PRIORITY.md`): Confirm dependencies and task boundaries.
*Ground Truth Invariant*: Specs in `docs/project/` are the authoritative source of truth. Subagents must never introduce unapproved interfaces or circumvent documented invariants.

### 4.2. Inter-Subagent Peer Consultation Protocol
Subagents coordinate through structured inquiries and peer reviews via native tools (`send_message`, `invoke_subagent`):
1. **Stage Handoff Consultations**: Upstream agents pass structured context down the lifecycle (`persona-agent` -> `prd-agent` -> `story-agent` -> `slicing-agent` -> `refinement-agent` -> `implementation-agent`).
2. **In-Worktree Peer Review**: Before staging commits, `implementation-agent` submits proposed diffs to `review-agent` or the lead orchestrator to check boundary seams (ADR-0007), file lengths (ADR-0002), and anti-mock rules (ADR-0003).
3. **Ambiguity Resolution & Escalation**: When interface boundaries or story criteria are ambiguous, subagents query peer agents or escalate to the lead orchestrator instead of hallucinating contracts.
4. **Execution State Reporting**: Subagents must report back structured execution state (`APPROVED`, `NEEDS_REVISION`, `BLOCKED`), consulted specification IDs, and commit trailers to the lead orchestrator.

---

## 5. Lifecycle Gate Invariants: Definition of Ready (DoR) & Definition of Done (DoD)

### 5.1. Definition of Ready (DoR) Gate (Phase 5)
Tasks may only transition to `refined/` and enter active development when all 7 DoR rules pass:
1. **Task Metadata Complete**: `id`, `title`, `status: Refined`, `target_bc`, and explicit dependencies.
2. **Governing Artifacts Linked**: Reference PRD in `docs/project/product/accepted/`, Persona in `docs/project/user_stories/PERSONAS.md`, and governing ADRs in `docs/project/adrs/accepted/` are cited.
3. **Executable BDD Specification**: Executable Gherkin scenarios verifiable without private mocks (ADR-0006).
4. **Generative Property Invariants Identified**: Domain invariants identified for Hypothesis `@given(...)` testing (ADR-0009).
5. **Mutation Testing Scope Defined**: Target domain modules identified with target >=80% mutant kill score under Mutmut (ADR-0009).
6. **INVEST Criteria Satisfied**: Validated as a thin vertical slice with files strictly <500 lines (ADR-0002).
7. **Documentation Review**: Existing docs reviewed to prevent conflicting conventions.

### 5.2. Definition of Done (DoD) Gate (Phase 7)
Work is complete and ready for integration into `main` only when all 10 DoD rules pass:
1. **Blackbox Frontdoor Verification**: 100% test pass rate verifying contracts with zero private mocks (ADR-0003).
2. **Executable BDD Scenarios Passing**: All Gherkin acceptance criteria executed via pytest-bdd pass cleanly (ADR-0006).
3. **Hypothesis Property Tests Passing**: Generative property tests verify invariants without shrinking failures (ADR-0009).
4. **Mutmut Mutation Score Attained**: Target domain modules achieve >=80% mutant kill score (ADR-0009).
5. **Codebase Health Check**: `spec-ops health` reports 0 file limit violations (<500 lines), 0 warnings, and PRIORITY.md sync (ADR-0002).
6. **Lockfile Integrity**: `uv lock --check` passes cleanly without unstaged dependency drifts.
7. **Documentation Integrity (Diataxis)**: Reusable patterns documented in `docs/how-to/` or `docs/reference/` and docs build cleanly.
8. **Strict Backlog Progression**: Task is moved from `refined/` to `complete/` and `PRIORITY.md` updated atomically upon integration (ADR-0005).
9. **Commit Provenance**: Commits include structured RFC 822 trailers (`SpecOps-Task: TASK-XXXX`).
10. **Dual-Custody Human Sign-Off**: Task frontmatter contains verified `signed_off_by`, review signed, and commit signatures cryptographically validated (ADR-0016).

---

## 6. Failure Recovery & Continuous Dogfooding

If any subagent encounters a blocker, stall, or test failure:
1. **Never Silently Discard**: Inspect diagnostics using `uv run spec-ops rescue inspect TASK-XXXX`.
2. **Salvage or Reset**: Salvage working files with `spec-ops rescue salvage` or reset with anti-loop memory via `spec-ops rescue reset --anti-loop`.
3. **Log Orchestration Bug**: If the toolchain, script, or prompt caused the failure, record an actionable task in `docs/project/backlog/proposed/` to fix it for future runs.
