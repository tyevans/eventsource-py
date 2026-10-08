# CLI Reference

SpecOps provides a unified command-line interface (`spec-ops`).

---

## Subcommands

| Command | Arguments | Description |
|---|---|---|
| `spec-ops init` | `[--dir PATH] [--name NAME] [--profile PROFILES] [--agent AGENTS] [--diataxis/--no-diataxis] [--github-pages/--no-github-pages] [--pre-commit/--no-pre-commit] [--interactive] [--headless] [--non-interactive] [--dry-run] [--ci CI] [--bc BC] [--bounded-context BC] [--yes]` | Bootstrap a new PMaC project with profile ADRs, interactive wizard, and headless CI automation |
| `spec-ops adopt` | `[--name NAME] [--dir DIR] [--profile PROFILES] [--grandfather-debt] [--no-grandfather-debt] [--github-pages] [--deconflict-workflow] [--bridge-docs]` | Adopt SpecOps into an existing brownfield codebase with debt baseline |
| `spec-ops profiles list` | None | List available architectural profiles |
| `spec-ops profiles apply` | `<PROFILE_NAME>` | Apply architectural profile to current repository |
| `spec-ops profiles sync` | `<PROFILE_NAME>` | Synchronize or restore architectural profile artifacts |
| `spec-ops profiles info` | `[--json]` | Inspect active architectural profile rules and quality preflight commands |
| `spec-ops profiles package` | `<SOURCE> [--out OUT] [--output OUT]` | Package custom profile into distributable bundle (.sop / .tar.gz) |
| `spec-ops profiles export` | `<SOURCE> [--out OUT] [--output OUT]` | Package custom profile into distributable bundle (.sop / .tar.gz) |
| `spec-ops profiles install` | `<BUNDLE>` | Install custom profile bundle into repository |
| `spec-ops profiles validate` | `<TARGET>` | Validate profile manifest and resolve inheritance DAG |
| `spec-ops profiles inspect` | `[TARGET]` | Inspect resolved profile inheritance, merged ADRs and invariant constraints |
| `spec-ops profiles diff` | `[PROFILE] [TARGET] [--json]` | Compute semantic diff of profile ADR additions, invariant clauses, and breaking limits |
| `spec-ops profiles upgrade` | `[PROFILE] [--force] [--action ACTION]` | Upgrade profile version, migrate baseline ADRs, and perform safe 3-way conflict resolution |
| `spec-ops profiles migrate` | `[--check] [--target-version TARGET_VERSION] [--dry-run] [--path PATH] [--json]` | Migrate profile configuration schema and validate evolvability |
| `spec-ops architecture seams` | `[--strict] [--json] [--export-heatmap EXPORT_HEATMAP]` | Audit bounded context seams and cross-context coupling heatmap |
| `spec-ops adr supersede` | `[<OLD_ID>] [--old OLD] [--title TITLE] [--by BY] [--with WITH] [--dry-run]` | Supersede an existing Architectural Decision Record with a new decision and audit active backlog citations |
| `spec-ops adr amend` | `[<OLD_ID>] [--old OLD] [--title TITLE] [--by BY] [--with WITH] [--dry-run]` | Incrementally amend an active Architectural Decision Record without retiring the predecessor |
| `spec-ops pathfinder inspect` | `<ENTITY>` | Inspect entity metadata, lineage card, active ADR amendments, and neighborhood (alias for spec-ops graph inspect) |
| `spec-ops pathfinder path` | `--from ORIGIN --to DEST` | Reachability pathfinding and lineage tracing (alias for spec-ops graph path) |
| `spec-ops scaffold` | `[--agent AGENT] [--agents AGENTS]` | Scaffold or regenerate project components and agent adapters |
| `spec-ops scaffold agents` | None | Regenerate AGENTS.md constitution from installed profiles |
| `spec-ops scaffold docs` | `[--bc BC] [--bounded-context BC] [--title TITLE] [--force] [--overwrite]` | Scaffold 4-quadrant Diataxis documentation for a bounded context (alias: diataxis) |
| `spec-ops scaffold hooks` | `[--force] [--native]` | Scaffold zero-dependency native POSIX shell git hooks and propagate to worktrees |
| `spec-ops scaffold ci` | `[--platform PLATFORM] [--force] [--update] [--matrix MATRIX] [--timeout TIMEOUT] [--timeout-minutes TIMEOUT_MINUTES]` | Scaffold multi-platform CI/CD quality gate workflows across GitHub Actions and GitLab CI |
| `spec-ops scaffold skill` | `[--target TARGET] [--output-dir OUTPUT_DIR] [--dry-run] [--force]` | Package and scaffold universal multi-platform skill bundles across agent platforms |
| `spec-ops constitution sync` | `[--repo PATH]` | Synchronize AGENTS.md constitution and docs/operating-manual.md while preserving human custom sections |
| `spec-ops constitution check` | `[--repo PATH]` | CI drift detection gate comparing specops.toml settings against AGENTS.md |
| `spec-ops health` | `[--security] [--architecture] [--suggest-splits] [--emit-task] [--generate-refactor-tasks] [--check-uat] [--numbering] [--modularity] [--json]` | Verify file length limits, artifact numbering uniqueness, architecture boundaries, and security profile guardrails |
| `spec-ops check` | `[--fast] [--file FILE] [POSITIONAL_FILE] [--format {text,json,sarif}]` | Sub-second IDE invariant diagnostics and real-time editor feedback |
| `spec-ops decompose` | `[--suggest PATH] [PATH]` | Analyze AST seams and recommend modular file decomposition |
| `spec-ops stats` | `[--cache] [--persona-coverage]` | Report project statistics and entity counts |
| `spec-ops parse` | `PATH` | Parse specification file with resilient AST diagnostics |
| `spec-ops graph compile` | `[--incremental] [--json] [--force-cold]` | Compile repository relational knowledge graph backed by content-addressed cache |
| `spec-ops graph cycles` | `[--format {text,json}] [--json] [--resolve] [--prune-chokepoints] [--check-edge SOURCE TARGET] [--cache]` | Deterministic cycle detection via Tarjan SCC |
| `spec-ops graph sort` | `[--type TYPE]` | Deterministic topological backlog execution sorting |
| `spec-ops graph order` | `[--type TYPE]` | Deterministic topological backlog execution ordering |
| `spec-ops graph path` | `--from ORIGIN --to DEST` | Reachability pathfinding and lineage tracing |
| `spec-ops graph reach` | `--source SOURCE --target TARGET [--json]` | Verify reachability between graph entities |
| `spec-ops graph blast-radius` | `<ENTITY>` | Calculate downstream blast radius of entity |
| `spec-ops graph inspect` | `<ENTITY>` | Inspect entity metadata, lineage card, and neighborhood |
| `spec-ops graph audit` | None | Full bidirectional graph traceability and orphan work item audit |
| `spec-ops graph watch` | `[--interval INTERVAL] [--debounce-ms DEBOUNCE_MS] [--json] [--event-stream] [--dir DIR] [--once] [--max-iterations MAX_ITERATIONS]` | Real-time in-memory graph event bus and workspace change watcher |
| `spec-ops graph mermaid` | `[--root ROOT] [--depth DEPTH] [--format {mermaid,dot}] [--direction {TD,LR,TB,RL}] [--output OUTPUT]` | Export relational knowledge graph subgraph as Mermaid or Graphviz diagram |
| `spec-ops graph deadlock` | `[--resolve] [--dry-run] [--json]` | Detect circular task dependencies and compute minimal feedback arc cuts |
| `spec-ops watch` | `[--debounce-ms DEBOUNCE_MS] [--event-stream] [--dir DIR] [--once] [--max-iterations MAX_ITERATIONS]` | Real-time in-memory graph event bus and workspace change watcher |
| `spec-ops trace` | `[--verify]` | Audit end-to-end bidirectional graph linkages and traceability |
| `spec-ops backlog bottlenecks` | `[--forecast]` | Detect circular dependency deadlocks and choke points |
| `spec-ops prd create` | `[--title TITLE] [--persona PERSONA] [--component COMPONENT] [--summary SUMMARY] [--stage STAGE]` | Scaffold a new PRD specification |
| `spec-ops prd discover` | `[--title TITLE] [--persona PERSONA] [--bc BC] [--summary SUMMARY] [--non-interactive]` | Discover and scaffold a new PRD idea draft |
| `spec-ops prd shape` | `[PRD_ID] [--id PRD_ID] [--outcomes OUTCOMES] [--anti-goals ANTI_GOALS] [--stage {shaped,accepted}] [--accept]` | Shape PRD idea into falsifiable specifications and advance lifecycle stage |
| `spec-ops prd new` | `[--title TITLE] [--persona PERSONA] [--component COMPONENT] [--friction FRICTION] [--good GOOD] [--anti-goals ANTI_GOALS] [--outcomes OUTCOMES] [--non-interactive]` | Interactively scaffold a new PRD specification in idea stage |
| `spec-ops prd lint` | `[PATH] [--remediate] [--json]` | Lint PRD markdown files for mandatory sections and falsifiable outcomes |
| `spec-ops prd promote` | `<PRD_ID> --stage STAGE` | Advance PRD through lifecycle stage gates |
| `spec-ops prd ship` | `<PRD_ID>` | Transition accepted PRD to shipped upon backlog completion |
| `spec-ops prd audit` | `[PRD_ID] [--deep]` | Audit PRD lifecycle statuses |
| `spec-ops prd decompose` | `<PRD_ID> [--no-spike] [--by-outcomes] [--diff]` | Decompose PRD into vertical slices |
| `spec-ops prd studio` | `[--open] [--port PORT] [--host HOST]` | Run interactive Web PRD Studio and Low-Code Story Assistant |
| `spec-ops prd uat status` | `[--json]` | Display customer UAT readiness matrix and overall delivery percentage |
| `spec-ops prd uat sign` | `--prd PRD --outcome OUTCOME --reviewer REVIEWER [--notes NOTES] [--status {Approved,Rejected,Pending}]` | Record PM business acceptance sign-off into docs/project/product/uat-signoff.json |
| `spec-ops prd uat receipt` | `[--prd PRD] [--out OUT] [--verify]` | Generate or verify tamper-evident cryptographic Customer UAT receipt |
| `spec-ops prd uat export` | `--prd PRD [--format {html}] [--output OUTPUT]` | Export standalone interactive Customer UAT acceptance matrix HTML report |
| `spec-ops prd journey` | `[--persona PERSONA] [--format {markdown,html,json}] [--output OUTPUT] [--json]` | Interactive Persona Customer Journey Map and Pain Point Matrix Visualizer |
| `spec-ops prd coverage` | `[--strict] [--json] [--bc BC]` | Audit BDD scenario coverage matrix and living acceptance dashboard |
| `spec-ops prd friction` | `[--persona PERSONA] [--json] [--threshold THRESHOLD]` | Autonomous user persona journey friction auditor and heuristic evaluator |
| `spec-ops prd gate` | `--prd PRD [--strict] [--json] [--export]` | Evaluate customer UAT sign-off tokens and verify release gate readiness |
| `spec-ops curate` | `[ACTION] [--infer] [--dry-run] [--model MODEL] [--json]` | Perform JIT backlog refinement, cognitive drift reconciliation, and scope slicing |
| `spec-ops visualizer` | `[--serve] [--build OUT] [--port PORT] [--entity ENTITY]` | Interactive 2D graph visualizer |
| `spec-ops visualizer export` | `[--output OUT]` | Export standalone single-file HTML visualizer bundle |
| `spec-ops export roadmap` | `[--format {svg,html}] [-o OUTPUT] [--out OUTPUT] [--output OUTPUT] [--audience AUDIENCE] [--granularity GRANULARITY]` | Export executive roadmap vector visual or interactive presentation |
| `spec-ops release notes` | `[PRD_ID] [--milestone MILESTONE] [--format {markdown,html,json}] [--branded] [-o OUTPUT] [--output OUTPUT] [--publish]` | Generate customer-facing release notes from shipped PRD capabilities and passed user stories |
| `spec-ops release velocity` | `[--format {markdown,html,json}] [-o OUTPUT] [--output OUTPUT] [--json]` | Analyze team delivery velocity and cognitive churn heatmap |
| `spec-ops release verify` | `--manifest MANIFEST [--keyring KEYRING] [--strict] [--json]` | Verify cryptographic release manifest integrity and authorized signer signatures |
| `spec-ops milestone rollover` | `--from FROM_M --to TO_M [--dry-run] [--json]` | Transition uncompleted tasks from one milestone to another |
| `spec-ops milestone plan` | `[--simulate] [--assign TASK=MILESTONE] [--save] [--json] [--non-interactive]` | Interactive milestone planning studio, workload balancing, and capacity simulation |
| `spec-ops worker` | `[ACTION] [TASK_ID] [--task TASK_ID] [--auto] [--drain] [--max-concurrency N] [--max-tasks M] [--dry-run] [--no-merge] [--no-review] [--skip-review] [--worker-id WORKER_ID] [--claimant CLAIMANT] [--telemetry] [--json]` | Execute backlog task in isolated worktree with concurrent review, or monitor fleet telemetry |
| `spec-ops worker orchestrate` | `[TASK_ID] [--task-id TASK_ID] [--task TASK_ID] [--max-attempts N] [--peer-review] [--consultation] [--no-peer-review] [--dry-run] [--json]` | Multi-agent in-worktree execution loop with active spec consultation, peer review, and AST self-healing |
| `spec-ops worker rebase` | `[task-id] [--abort-on-conflict] [--dry-run] [--json]` | Autonomous worktree auto-rebase against main with conflict resolution |
| `spec-ops worker diagnose` | `[--log LOG] [--text TEXT] [--json]` | AST self-healing diagnostic analysis of preflight failures and retry prompt synthesis |
| `spec-ops worker lease` | `[--status] [--reclaim] [--heartbeat] [--task TASK_ID] [--task-id TASK_ID] [--dry-run] [--ttl TTL] [--json]` | Inspect worker process leases, record liveness heartbeats, and auto-reclaim dead zombie claims |
| `spec-ops orchestrate retrospect` | `[--log-dir LOG_DIR] [--dry-run] [--json]` | Analyze session artifacts and failure logs to categorize invariant breaches and synthesize proposed remediation tasks |
| `spec-ops orchestrate health` | `[--log-dir LOG_DIR] [--json]` | Summarize orchestration health, pass/fail rates, stalled worktrees, and unaddressed bugs |
| `spec-ops cycle` | `[--max-tasks N] [--max-concurrency N] [--drain] [--dry-run] [--no-merge] [--build-docs] [--no-review] [--skip-review] [--adaptive]` | Run end-to-end autonomous development cycle |
| `spec-ops rescue` | `[ACTION] [TASK_ID] [--list] [--complete] [--discard] [--reset] [--reason REASON] [--demote] [--prune] [--dry-run] [--action ACTION] [--file FILE] [--step STEP] [--only-failed] [--salvage]` | Inspect, triage, and recover stalled or failed autonomous worktrees, render quickstart cheatsheets, or reset worktree with failure memory |
| `spec-ops rescue reset` | `[task_id] [--task-id TASK_ID] [--stash] [--list-stashes] [--apply STASH_ID] [--force] [--json] [--reason REASON] [--demote]` | Safe worktree discard with anti-loop failure memory, automated stash, and clean reset recovery |
| `spec-ops rescue salvage` | `<TASK_ID> --files FILES [FILES ...]` | Selectively salvage specified files from stalled worktree into clean rescue branch |
| `spec-ops rescue patch` | `<TASK_ID> --include INCLUDE` | Incrementally stage files into the rescue index |
| `spec-ops rescue quota` | `[--threshold THRESHOLD] [--json]` | Worktree disk quota monitor and storage consumption audit |
| `spec-ops rescue prune` | `[--older-than OLDER_THAN] [--dry-run] [--force] [--json]` | Safely prune merged or abandoned orphan worktrees and reclaim disk space |
| `spec-ops rescue cluster` | `[--json] [--task TASK]` | Autonomous failure post-mortem clustering and prompt anti-loop synthesizer |
| `spec-ops rescue playbooks` | `[--query QUERY] [-q QUERY] [--json] [--export EXPORT]` | Search and generate autonomous failure healing playbooks |
| `spec-ops worktree start` | `<TASK_ID>` | Spawn an isolated development worktree for a task |
| `spec-ops worktree finish` | `[--task-id TASK_ID]` | Verify preflight, merge into main under MERGE_LOCK, and clean up worktree |
| `spec-ops spike create` | `--name NAME --question QUESTION [--timebox TIMEBOX] [--task TASK_ID] [--prd PRD_ID]` | Author a new architectural spike task and isolated test harness |
| `spec-ops spike start` | `<SPIKE_ID> [--hypothesis HYPOTHESIS] [--timebox TIMEBOX]` | Instantiate disposable sandboxed spike worktree |
| `spec-ops spike check` | `[SPIKE_ID] [--elapsed ELAPSED]` | Check spike timebox and write isolation |
| `spec-ops spike preflight` | `[SPIKE_ID]` | Enforce in-worktree write isolation preflight hook |
| `spec-ops spike graduate` | `<SPIKE_ID> --result {proven,disproven} [--title TITLE] [--notes NOTES] [--findings FINDINGS] [--status STATUS]` | Graduate empirical spike findings into an Architectural Decision Record |
| `spec-ops tui` | `[--once] [--view {overview,backlog,tree,health}]` | Launch interactive Terminal UI (TUI) dashboard |
| `spec-ops monitor live` | `[--headless] [--interval INTERVAL] [--tab {workers,events,health}] [--json]` | Multi-tab interactive terminal dashboard streaming real-time events and worker status |
| `spec-ops queue next` | `[--json]` | Inspect next ready, unblocked backlog task |
| `spec-ops queue claim` | `[TASK_ID] [--auto] [--worker-id WORKER_ID] [--claimant CLAIMANT] [--json]` | Claim next ready unblocked task or specific task under cross-process lock |
| `spec-ops queue refine` | `<TASK_ID>` | Validate Definition of Ready and promote task to refined |
| `spec-ops queue complete` | `<TASK_ID> [--base BASE]` | Gate and complete task integration under merge lock |
| `spec-ops queue tree` | `[--task TASK] [--direction {blocks,blocked-by}] [--reverse] [--waves] [--all] [--json]` | Display task dependency tree, execution waves, and blockers |
| `spec-ops queue block` | `<TASK_ID> --question QUESTION [--type {unknown,spike_needed,external,dependency}] [--spike] [--timebox TIMEBOX] [--raised-by RAISED_BY]` | Mark a task as blocked by an unknown question or impediment |
| `spec-ops queue unblock` | `<TASK_ID> --resolution RESOLUTION [--adr ADR]` | Resolve an unknown/blocker and restore ready/proposed state |
| `spec-ops queue blockers` | `[--json]` | List all currently blocked tasks, open questions, and linked spikes |
| `spec-ops queue monitor` | `[--once]` | Interactive terminal backlog flow monitor and JIT buffer telemetry |
| `spec-ops queue doctor` | `[--fix] [--repair] [--json] [--dir DIR]` | Audit backlog health, dangling dependencies, and index drift with automated self-healing repair |
| `spec-ops queue digest` | `[--format FORMAT] [--window WINDOW]` | Generate automated daily standup curation digest |
| `spec-ops queue reorder` | `[--dry-run] [--topological] [--by-weights] [--json]` | Deterministic topological backlog re-ordering and multi-criteria priority scoring |
| `spec-ops queue reclaim-stalled` | `[--timeout-hours TIMEOUT_HOURS] [--dry-run] [--json]` | Automated detection and reclamation of abandoned task claims and stale worker leases |
| `spec-ops backlog` | `[--once]` | Backlog flow monitor, buffer telemetry, and bottleneck detection |
| `spec-ops backlog flow` | `[--once]` | Interactive terminal backlog flow monitor and JIT buffer telemetry |
| `spec-ops backlog bottlenecks` | `[--forecast]` | Detect circular dependency deadlocks and choke points |
| `spec-ops backlog doctor` | `[--fix] [--repair] [--json] [--dir DIR]` | Audit backlog health, dangling dependencies, and index drift with automated self-healing repair |
| `spec-ops backlog sweep` | `[--format FORMAT] [--window WINDOW] [--reclaim-stalled]` | Daily standup curation digest and backlog sweep |
| `spec-ops backlog reorder` | `[--dry-run] [--topological] [--by-weights] [--json]` | Deterministic topological backlog re-ordering and multi-criteria priority scoring |
| `spec-ops backlog import` | `[--source {github,jira,linear,auto}] [--file FILE] [--repo REPO] [--label LABEL] [--bc/--target-bc BC]` | Ingest issues from GitHub Issues, Jira, or Linear into schema-compliant proposed task files |
| `spec-ops backlog export` | `[--format {markdown,json}] [--out/--output OUT] [--target {github,jira,linear}] [--sync-status] [--dry-run]` | Export structured backlog status snapshot and external tracker sync |
| `spec-ops bridge import` | `[--source {github,jira,linear,auto}] [--file FILE] [--repo REPO] [--label LABEL] [--bc/--target-bc BC]` | Ingest issues from GitHub Issues, Jira, or Linear into schema-compliant proposed task files |
| `spec-ops bridge export` | `[--format {markdown,json}] [--out/--output OUT] [--target {github,jira,linear}] [--sync-status] [--dry-run]` | Export structured backlog status snapshot and external tracker sync |
| `spec-ops report burndown` | `[--milestone MILESTONE] [--format {deck,html,digest}] [-o OUTPUT] [--output OUTPUT] [--check-alignment]` | Milestone burndown velocity and presentation slide deck export |
| `spec-ops report milestone` | `[milestone] [--milestone MILESTONE] [--audit-scope] [--check-alignment] [--export {html,markdown}] [--format {digest,deck,html}] [-o OUTPUT] [--output OUTPUT] [--out OUTPUT]` | Milestone executive briefing digest, scope alignment, and HTML export |
| `spec-ops report velocity` | `[--window WINDOW] [--rescues] [--json] [--export EXPORT] [--format {table,json,html}] [-o OUTPUT] [--out OUTPUT] [--output OUTPUT]` | Hybrid delivery velocity and autonomous worker rescue analytics |
| `spec-ops task create` | `[--title TITLE] [--bc TARGET_BC] [--prd PRD] [--story STORY] [--adr ADR] [--dependencies/--deps DEPS] [--stage STAGE] [--non-interactive]` | Scaffold a new PMaC task with Definition of Ready scaffolding |
| `spec-ops task decompose` | `[PRD_ID] [--prd PRD] [--output-dir OUTPUT_DIR] [--dry-run]` | Decompose PRD into INVEST-compliant vertical tasks and architectural spikes |
| `spec-ops task synthesize-dor` | `[TASK_ID] [--task TASK] [--dry-run]` | Evaluate task Definition of Ready completeness and synthesize missing contracts |
| `spec-ops doctor` | `[--fix] [--json]` | Audit and repair local developer workspace and tooling |
| `spec-ops security verify-lock` | `[--path PATH]` | Verify supply-chain lockfile cryptographic hashes and pinning |
| `spec-ops security verify-commits` | `[--range REV_RANGE] [--keyring KEYRING] [--strict] [--json]` | Verify cryptographic commit signatures across rev-range against authorized keyring |
| `spec-ops security check-trailers` | `[--range REV_RANGE] [--strict] [--json]` | Verify conventional commit messages and RFC-822 SpecOps traceability trailers |
| `spec-ops security sentinel` | `[--path PATH] [--fix] [--json]` | Inspect and enforce supply-chain lockfile mutation immutability |
| `spec-ops security audit-lockfile` | `[--verify] [--json] [--record]` | Audit supply-chain lockfile integrity, record attestations, and detect tampering |
| `spec-ops security scan` | `[--path PATH] [--entropy] [--threshold THRESHOLD] [--json]` | Scan source files using Shannon entropy analysis and custom rule plugins |
| `spec-ops security scan-secrets` | `[--path PATH] [--staged] [--threshold THRESHOLD] [--json]` | Scan worktree diffs and source files for high-entropy secrets and credential leaks |
| `spec-ops security hook install` | `[--path PATH] [--force]` | Install automated pre-commit hook into git repository |
| `spec-ops security hook uninstall` | `[--path PATH]` | Uninstall automated pre-commit hook from git repository |
| `spec-ops security hook verify` | `[--path PATH]` | Verify automated pre-commit hook installation and integrity |
| `spec-ops security hook run` | `[--path PATH]` | Execute pre-commit sentinel checks against currently staged files |
| `spec-ops audit dependencies` | `[--path PATH] [--offline]` | Scan direct and transitive dependencies for High/Critical CVEs and enforce license allowlists |
| `spec-ops audit export` | `[--standard STANDARD] [--output OUTPUT]` | Compile and export tamper-evident Merkle compliance audit manifest |
| `spec-ops audit verify` | `[--manifest MANIFEST] [--repo REPO]` | Verify cryptographic compliance manifest integrity and SDLC traceability |
| `spec-ops audit provenance` | `[--strict] [--contributions] [--repo REPO] [--since SINCE]` | Audit unbroken commit trailers, SDLC traceability lineage, and contributor provenance (alias: traceability) |
| `spec-ops audit proof` | `--deliverable DELIVERABLE [--manifest MANIFEST] [--out OUT] [--json]` | Generate self-contained Merkle inclusion proof for a single deliverable |
| `spec-ops audit verify-proof` | `PROOF_FILE --root ROOT [--json]` | Verify Merkle inclusion proof against trusted root in offline execution |
| `spec-ops audit merkle` | `[--verify VERIFY] [--output OUTPUT] [--json]` | Compile, output, or verify tamper-evident Merkle compliance manifest for repository artifacts |
| `spec-ops audit sink` | `[--format {jsonl,sqlite}] [--output OUTPUT] [--since SINCE] [--until UNTIL] [--category CATEGORY] [--aggregate-type AGGREGATE_TYPE] [--compress] [--db DB] [--json]` | Export structured event audit sink archive and historical streams |
| `spec-ops review` | `[TASK_ID] [--identity IDENTITY] [--provenance]` | Generate structured architectural review brief or cryptographically sign review |
| `spec-ops review radar` | `[--json] [--bc BC]` | Cross-context interface auditor and architectural review radar |
| `spec-ops docs build` | `[--out OUT_DIR] [--base-url BASE_URL] [--include-visualizer]` | Compile Diataxis documentation static site and embedded 2D visualizer |
| `spec-ops docs audit` | `[--dir DIR] [--strict]` | Audit Diataxis quadrant structure, CLI drift, and documentation code snippets |
| `spec-ops docs check` | `[--dir DIR]` | Living Diataxis documentation drift guard auditing CLI commands against docs |
| `spec-ops test audit-anti-mock` | `[PATH] [--path OPT_PATH] [--strict-mutation] [--threshold THRESHOLD] [--json]` | Audit test ASTs for prohibited mock backdoors and verify ADR-0003 frontdoor compliance |
| `spec-ops test verify-frontdoors` | `[PATH] [--path OPT_PATH] [--strict-mutation] [--threshold THRESHOLD] [--json]` | Verify blackbox frontdoors, audit anti-mock AST violations, and enforce mutation score invariants |
| `spec-ops test properties` | `[PATH] [--path OPT_PATH] [--max-examples MAX_EXAMPLES] [-k/--filter FILTER_EXPR] [--json]` | Execute Hypothesis generative property invariant verification tests (ADR-0009) |
| `spec-ops test mutation` | `[PATH] [--path OPT_PATH] [--threshold THRESHOLD] [--bc TARGET_BC] [--json] [--force-run]` | Run Mutmut mutation testing quality gate on core domain modules per ADR-0009 |
| `spec-ops verify` | `[PATH] [--invariants] [--max-examples MAX_EXAMPLES] [--path OPT_PATH] [-k/--filter FILTER_EXPR] [--json]` | Execute verification suites and invariant checks |
| `spec-ops invariants verify-mutations` | `[PATH] [--path OPT_PATH] [--threshold THRESHOLD] [--bc TARGET_BC] [--json] [--force-run]` | Verify mutation testing kill score quality gate per ADR-0009 |
| `spec-ops schema check` | `[PATH] [--path OPT_PATH]` | Audit specification documents against schema v2.0 Pydantic models with compiler-grade diagnostic pointers |
| `spec-ops schema validate` | `[PATH] [--path OPT_PATH]` | Alias for schema check auditing specification frontmatter against active Pydantic models |
| `spec-ops schema migrate` | `[PATH] [--path OPT_PATH] [--dry-run] [--in-place]` | Safely migrate legacy specification frontmatter fields to schema v2.0 while preserving Markdown body byte-for-byte |
| `spec-ops persona audit` | `[--json]` | Inspect accepted PRDs, stories, and git commits against PERSONAS.md, identify uncovered or emerging archetypes, and report coverage statistics |
| `spec-ops persona sync` | `[--diff] [--apply] [--json]` | Generate synthesized persona profile additions for emerging archetypes, preview diff, or apply updates to PERSONAS.md |
| `spec-ops story create` | `[--title TITLE] [--prd PRD] [--persona PERSONA] [--bc/--target-bc BC] [--id ID] [--feature FEATURE] [--scenarios SCENARIOS] [--dry-run]` | Scaffold a new BDD user story specification with Gherkin acceptance criteria |
| `spec-ops story trace` | `[--story STORY] [--prd PRD] [--json]` | Audit end-to-end bidirectional traceability across Personas, PRDs, Stories, Tasks, and Commits |
