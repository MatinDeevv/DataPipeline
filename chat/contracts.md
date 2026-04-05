# Contract Change Log

All public boundary changes **must** be logged here before or immediately after the change.

Human review notes do NOT belong in this file.

Cross-package imports are only allowed via:
- `mt5pipe.contracts.*`
- `mt5pipe.state.public`
- `mt5pipe.features.public`
- `mt5pipe.labels.public`
- `mt5pipe.compiler.public`

Scope guard:
- `feedbacks/latest.md` = human steering
- `chat/contracts.md` = boundary/API/schema changes only
- `chat/coordination.md` = blockers/requests/handoffs/coordination only

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

### [2026-04-04 20:05]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.public, mt5pipe.labels.public
symbol: FeatureBuilder, FeatureArtifactRef, load_feature_artifact, get_default_feature_specs, resolve_feature_selectors, LabelArtifactRef, load_label_artifact, get_default_label_packs, resolve_label_pack
old: public surfaces exported only FeatureSpec/FeatureService/basic family builders and LabelPack/LabelService
new: public surfaces now expose registry helpers plus stable artifact ref/loader symbols for persisted feature and label views
impact: compiler/tests/future cross-sector consumers can depend on features.public and labels.public without reaching into registry or storage internals
docs_updated: yes
notes: loaders use StoragePaths + ParquetStore and keep label behavior unchanged
```

### [2026-04-04 20:05]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.registry.defaults
symbol: disagreement.microstructure_pressure@1.0.0, event_shape.flow_shape@1.0.0, entropy.market_complexity@1.0.0
old: stable registry covered only time/session/quality/htf_context families
new: adds three stable Phase 3 machine-native feature families with explicit dependencies, warmup rows, PIT safety, and output columns
impact: dataset specs can now resolve disagreement/*, event_shape/*, and entropy/*
docs_updated: yes
notes: all three families remain M1/BuiltBar-compatible so Agent 3 can compile them without compiler redesign
```
### [2026-04-04 20:40]
agent: agent_1
type: contract-change
module: mt5pipe.contracts.artifacts, mt5pipe.contracts.state
symbol: ArtifactKind(CANONICAL_TICK, STATE_WINDOW), TickArtifactRef, StateArtifactRef, StateWindowArtifactRef, StateWindowRequest, parse_window_size
old: shared contracts only exposed generic ArtifactRef/DatasetId/TrustVerdict/LineageNode
new: state-specific artifact refs and rolling-window request contract are now first-class shared types for cross-sector state access
impact: Agent 2 can depend on typed state/tick/window refs; Agent 3 can pass/record state refs without importing state internals
docs_updated: no
notes: window sizes use compact strings like 30s/60s/5m; refs are range-based and artifact-id driven.

### [2026-04-04 20:40]
agent: agent_1
type: contract-change
module: mt5pipe.state.public
symbol: StateArtifactManifest, StateArtifactRef, StateWindowArtifactRef, StateWindowRecord, StateWindowRequest, TickArtifactRef, StateBuilder, StateService, load_state_artifact, materialize_state_windows
old: state.public only re-exported StateSnapshot and StateService
new: state.public now exposes the stable state-sector substrate for state/tick artifacts and machine-native rolling state windows
impact: Agent 2 should import state substrate from mt5pipe.state.public only; Agent 3 can keep calling StateService while the boundary is now explicit
docs_updated: no
notes: state sector no longer imports compiler/catalog internals; manifests are written by local state helpers and can still be handed to compiler catalog duck-typed.
