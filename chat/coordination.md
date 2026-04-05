# Coordination Log

Use this file for blockers, requests, progress updates, and handoffs between agents.

Use this file to log human-feedback conflicts when `feedbacks/latest.md` conflicts with the active task/prompt.

Scope guard:
- `feedbacks/latest.md` = human steering
- `chat/contracts.md` = boundary/API/schema changes only
- `chat/coordination.md` = blockers/requests/handoffs/coordination only

---

## Active Blockers

_None_

---

## Log

<!-- Append new entries at the bottom -->

### [2026-04-04] Setup agent — structure ready

```
agent: setup
type: update
area: contracts
summary: Created boundary structure (contracts, public.py surfaces, internal/ packages). Ready for parallel development.
needs: Agent 1/2/3 to populate their sectors
files: mt5pipe/contracts/, mt5pipe/state/public.py, mt5pipe/features/public.py, mt5pipe/compiler/public.py, docs/parallel_dev_architecture.md
```

---

<!-- TEMPLATE — copy this block for new entries:

```
[YYYY-MM-DD HH:MM]
agent: <agent_1|agent_2|agent_3>
type: update|blocker|request|handoff
area: state|features|compiler|contracts
summary: <short summary>
needs: <optional>
files: <optional list>
```

-->
### [2026-04-04] Agent 2 - Phase 3 feature families ready

```
agent: agent_2
type: handoff
area: features
summary: Stable selectors disagreement/*, event_shape/*, and entropy/* are registered and tested. Public feature/label loaders and artifact refs are available via features.public and labels.public.
needs: Agent 3 can include these selectors in DatasetSpec builds once mt5pipe.compiler.service is restored/importable in this worktree.
files: mt5pipe/features/registry/defaults.py, mt5pipe/features/disagreement/, mt5pipe/features/event_shape/, mt5pipe/features/entropy/, mt5pipe/features/public.py, mt5pipe/labels/public.py, tests/test_features_phase3.py
```
### [2026-04-04] Agent 1 - compiler public import blocker

```
agent: agent_1
type: blocker
area: compiler
summary: state/contracts tests are green, but global boundary import test currently fails because mt5pipe.compiler.public imports mt5pipe.compiler.service and that module is absent in this worktree.
needs: Agent 3 to restore or re-export compiler service boundary symbols so mt5pipe.compiler.public is importable again.
files: mt5pipe/compiler/public.py
```
### [2026-04-04] Agent 1 - state substrate ready for feature work

```
agent: agent_1
type: handoff
area: state
summary: published typed TickArtifactRef/StateArtifactRef/StateWindowArtifactRef + StateWindowRequest in mt5pipe.contracts, and expanded mt5pipe.state.public with StateWindowRecord/load_state_artifact/materialize_state_windows. Rolling state windows and canonical tick state are implemented and tested.
needs: Agent 2 can build against mt5pipe.state.public now. Agent 3 only needs to restore compiler.public importability in this worktree.
files: mt5pipe/contracts/state.py, mt5pipe/state/public.py, mt5pipe/state/service.py, mt5pipe/state/models.py
```

### [2026-04-04] Agent 3 - Phase 3 compiler/truth handoff

```
agent: agent_3
type: handoff
area: compiler
summary: Compiler/truth/catalog/CLI path is green for artifact-backed Phase 3 datasets. Artifact-backed state reuse now honors exclude:filled_rows through public state trust flags, and focused tests cover compile success, truth rejection, inspect/diff, and catalog lifecycle.
needs: The real workspace still only exposes stable feature view directories under local_data/pipeline_data/feature_views and currently has no catalog.db. For config/datasets/xau_m1_nonhuman_v1.yaml to compile end-to-end outside the tests, Agent 2 machine-native feature artifacts (disagreement/event_shape/entropy) must exist and be registered under the refs used by the spec, or the feature registry implementation must be present in this worktree so the compiler can materialize them.
files: mt5pipe/compiler/service.py, mt5pipe/truth/service.py, mt5pipe/cli/dataset_cmds.py, config/datasets/xau_m1_nonhuman_v1.yaml, tests/test_compiler.py
```
