# Contract Change Log

All public boundary changes **must** be logged here before or immediately after the change.

Cross-package imports are only allowed via:
- `mt5pipe.contracts.*`
- `mt5pipe.state.public`
- `mt5pipe.features.public`
- `mt5pipe.labels.public`
- `mt5pipe.compiler.public`

---

## Ownership Map

| Sector        | Owner   | Packages                                         |
|---------------|---------|--------------------------------------------------|
| contracts + state | Agent 1 | `mt5pipe/contracts/`, `mt5pipe/state/`          |
| features      | Agent 2 | `mt5pipe/features/`, `mt5pipe/labels/`           |
| compiler      | Agent 3 | `mt5pipe/compiler/`, `mt5pipe/truth/`, `mt5pipe/catalog/` |

---

## Contract Changes

<!-- Append new entries at the bottom -->

### [2026-04-04] Setup agent — initial boundary definitions

```
agent: setup
type: contract-change
module: mt5pipe.contracts
symbol: ArtifactRef, DatasetId, TrustVerdict, LineageNode
old: N/A (new)
new: Shared contract types for cross-sector communication
impact: all agents
docs_updated: yes
notes: Initial shared contracts package. Types are placeholders — agents must finalize signatures.
```

### [2026-04-04] Setup agent — boundary cleanup pass

```
agent: setup
type: contract-change
module: mt5pipe.labels.public (new), mt5pipe.features.public, mt5pipe.state.__init__, mt5pipe.compiler.__init__, mt5pipe.features.__init__, mt5pipe.labels.__init__
symbol: LabelPack, LabelService (added to features.public), all __init__ re-route through public.py
old: __init__.py exported directly from models/service internals; labels had no public.py
new: all sector __init__.py re-export from public.py only; labels.public.py added as intra-sector boundary; features.public.py re-exports labels surface for cross-sector consumers
impact: all agents — no import paths changed externally, but __init__ now routes through public
docs_updated: yes
notes: Cleanup only. No implementation changes.
```

---

<!-- TEMPLATE — copy this block for new entries:

```
[YYYY-MM-DD HH:MM]
agent: <agent_1|agent_2|agent_3>
type: contract-change
module: <module path>
symbol: <public symbol>
old: <old signature/behavior>
new: <new signature/behavior>
impact: <who is affected>
docs_updated: yes|no
notes: <short note>
```

-->
