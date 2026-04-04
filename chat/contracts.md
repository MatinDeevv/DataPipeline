# Contract Change Log

All public boundary changes **must** be logged here before or immediately after the change.

Cross-package imports are only allowed via:
- `mt5pipe.contracts.*`
- `mt5pipe.state.public`
- `mt5pipe.features.public`
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
