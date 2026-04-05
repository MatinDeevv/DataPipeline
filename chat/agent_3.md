# Agent 3 — Compiler / Truth / Catalog

[Human feedback rule]

Treat `feedbacks/latest.md` as the highest-priority human steering note before new work begins.

Before starting any new work:
1. Check whether `feedbacks/latest.md` exists and is non-empty.
2. If it exists, read it fully before doing anything else.
3. In your agent log, record:
	- `feedback_read: yes`
	- `feedback_source: feedbacks/latest.md`
	- `feedback_summary: <1-3 lines>`
4. Follow the latest human review unless it directly conflicts with the active task/prompt.
5. If there is a conflict, log it in `chat/coordination.md` before proceeding.

Do not put human feedback notes into `chat/contracts.md`.
Use:
- `feedbacks/latest.md` for human review
- `chat/contracts.md` for boundary/API/schema changes
- `chat/coordination.md` for blockers, requests, handoffs, and coordination

[Agent feedback log snippet]
feedback_read: yes|no
feedback_source: feedbacks/latest.md
feedback_summary: <1-3 lines>

**Owns:** `mt5pipe/compiler/`, `mt5pipe/truth/`, `mt5pipe/catalog/`
**Boundary module:** `mt5pipe/compiler/public.py`
**Tests:** `tests/test_compiler.py`, `tests/test_truth_core.py`, `tests/test_catalog.py`

## Responsibility
- Dataset compilation pipeline (`DatasetCompiler`, `compile_dataset_spec`)
- Artifact lifecycle management and truth gate (`TruthService`)
- Artifact catalog (`CatalogDB`)
- CLI integration glue (dataset commands)

## Allowed imports from other sectors
- `mt5pipe.contracts.*`
- `mt5pipe.state.public` — for `StateSnapshot`, `StateService`
- `mt5pipe.features.public` — for `FeatureSpec`, `FeatureService`, `LabelPack`, `LabelService`

## Log

<!-- Append timestamped entries below as work progresses -->

### [2026-04-04 20:16:24 -04:00] agent_3 - start

```
agent: agent_3
type: update
area: compiler
summary: Started Phase 3 compiler/truth/catalog/CLI pass. Reading current compiler-facing codepaths and dataset/test layout before implementing xau_m1_nonhuman_v1 end-to-end compile support.
needs: Will adapt to Agent 1/2 boundary changes through public modules only.
files: chat/contracts.md, chat/coordination.md, chat/agent_3.md, mt5pipe/compiler/public.py, mt5pipe/state/public.py, mt5pipe/features/public.py, mt5pipe/labels/public.py
```
