# Goal
Deep analysis of `src/eventsource/domain` (the Tier-0 domain ring, ~3.5k lines across 12 modules): identify insecure patterns, poor architecture, and concrete consolidation or decomposition opportunities.

## Dimensions
1. Structural soundness: are module boundaries right? Should anything be split (e.g. aggregate.py 850 lines, exceptions.py 758 lines) or merged (small modules like types.py, stream_id.py, command.py)?
2. Insecure or fragile patterns: mutable global state, dynamic dispatch/registry risks, contextvar misuse, deserialization/registry injection surfaces, thread/async safety.
3. Coupling and layering: does domain stay dependency-free (Tier-0)? Any leakage of application/adapter concerns (tenancy, registries) into domain? Do exports in __init__.py match the intended public surface?
4. Duplication/consolidation: overlapping responsibilities between event.py, event_registry.py, tenant_events.py, tenant_context.py; exception taxonomy sprawl.

## Boundaries
- Out of scope: adapters/, application/, ports/ internals (only their imports FROM domain matter).
- Out of scope: performance benchmarking; docs quality.
