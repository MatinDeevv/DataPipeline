# Parallel Development Architecture

## Overview

The codebase is organized into **3 sectors** with strict import boundaries, enabling 3 agents to work in parallel without merge conflicts or coupling regressions.

The top-level package is `mt5pipe/`. In architecture discussions, "pipeline" maps to `mt5pipe/`.

---

## Sectors

| Sector    | Owner   | Packages                                              |
|-----------|---------|-------------------------------------------------------|
| **State**     | Agent 1 | `mt5pipe/contracts/`, `mt5pipe/state/`               |
| **Features**  | Agent 2 | `mt5pipe/features/`, `mt5pipe/labels/`               |
| **Compiler**  | Agent 3 | `mt5pipe/compiler/`, `mt5pipe/truth/`, `mt5pipe/catalog/` |

### Agent 1 — State + Contracts
- **Owns:** `mt5pipe/contracts/`, `mt5pipe/state/`
- **Boundary module:** `mt5pipe/state/public.py`
- **Tests:** `tests/test_schema.py`, state-related tests
- **Responsibility:** Shared contract types, state materialization, state models

### Agent 2 — Features
- **Owns:** `mt5pipe/features/`, `mt5pipe/labels/`
- **Boundary module:** `mt5pipe/features/public.py`
- **Tests:** `tests/test_labels.py`, feature-related tests
- **Responsibility:** Feature builders, feature registry, label generation, label registry

### Agent 3 — Compiler
- **Owns:** `mt5pipe/compiler/`, `mt5pipe/truth/`, `mt5pipe/catalog/`
- **Boundary module:** `mt5pipe/compiler/public.py`
- **Tests:** `tests/test_compiler.py`, `tests/test_truth_core.py`, `tests/test_catalog.py`
- **Responsibility:** Dataset compilation, truth gate, artifact catalog, CLI integration

---

## Neutral Packages (shared, no single owner)

These packages are **not** sector-gated. Any sector may import from them directly:

- `mt5pipe/models/` — domain models (ticks, bars, market, etc.)
- `mt5pipe/config/` — pipeline configuration
- `mt5pipe/storage/` — Parquet I/O, paths, checkpoints
- `mt5pipe/quality/` — QA / cleaning / gap detection
- `mt5pipe/merge/` — canonical tick merge
- `mt5pipe/mt5/` — MT5 connection layer
- `mt5pipe/ingestion/` — raw data ingestion
- `mt5pipe/backfill/` — backfill engine
- `mt5pipe/bars/` — bar builder
- `mt5pipe/live/` — live data collector
- `mt5pipe/cli/` — CLI commands
- `mt5pipe/utils/`, `mt5pipe/tools/`

---

## Import Rules

```
┌──────────────────────────────────────────────┐
│              ALLOWED IMPORTS                  │
│                                              │
│   Any module → mt5pipe.contracts.*           │
│   Any module → mt5pipe.state.public          │
│   Any module → mt5pipe.features.public       │
│   Any module → mt5pipe.compiler.public       │
│   Any module → neutral packages              │
│   Intra-sector → anything within own sector  │
│                                              │
│              FORBIDDEN                       │
│                                              │
│   state → mt5pipe.features.builder           │
│   state → mt5pipe.compiler.service           │
│   features → mt5pipe.state.service           │
│   features → mt5pipe.compiler.models         │
│   compiler → mt5pipe.state.models            │
│   ... (any direct cross-sector internal)     │
└──────────────────────────────────────────────┘
```

**Rule:** Cross-sector imports may ONLY target:
1. `mt5pipe.contracts.*`
2. `mt5pipe.<sector>.public`

This is enforced by `tests/test_boundary_imports.py`.

---

## Coordination System

### `chat/contracts.md`
- **Purpose:** Log all public boundary changes (new exports, signature changes, schema changes)
- **Rule:** Any change to a `public.py` or `mt5pipe/contracts/` file MUST be logged here
- **Format:** Structured entries with agent name, module, old/new signature, impact

### `chat/coordination.md`
- **Purpose:** Blockers, requests, progress updates, handoffs between agents
- **Rule:** Use this for any cross-agent communication
- **Format:** Structured entries with agent name, type, area, summary

---

## Package Structure

```
mt5pipe/
├── contracts/          ← Shared types (Agent 1 owns, all read)
│   ├── __init__.py
│   ├── artifacts.py    ← ArtifactRef, ArtifactKind
│   ├── dataset.py      ← DatasetId, DatasetSplitKind, DATASET_JOIN_KEYS
│   ├── trust.py        ← TrustVerdict
│   └── lineage.py      ← LineageNode
├── state/              ← Agent 1
│   ├── __init__.py
│   ├── public.py       ← BOUNDARY: StateSnapshot, StateService
│   ├── internal/
│   ├── models.py
│   └── service.py
├── features/           ← Agent 2
│   ├── __init__.py
│   ├── public.py       ← BOUNDARY: FeatureSpec, FeatureService, builders
│   ├── internal/
│   ├── builder.py
│   ├── service.py
│   ├── registry/
│   └── ...
├── labels/             ← Agent 2
│   └── ...
├── compiler/           ← Agent 3
│   ├── __init__.py
│   ├── public.py       ← BOUNDARY: DatasetSpec, LineageManifest, DatasetCompiler
│   ├── internal/
│   ├── models.py
│   ├── service.py
│   └── manifest.py
├── truth/              ← Agent 3
│   └── ...
├── catalog/            ← Agent 3
│   └── ...
└── [neutral packages]
```

---

## Migration Notes

The existing codebase has direct cross-sector imports that predate this boundary setup. For example, `compiler.service` imports from `state.service` directly. These will be migrated incrementally:

1. New code MUST use boundary modules
2. Existing imports will be migrated to use `public.py` + `contracts/`
3. `tests/test_boundary_imports.py` tracks violations (currently `xfail`)
4. Target: zero violations

---

## Workflow

1. Pick your sector's files
2. Import only from contracts + other sectors' public.py
3. Log boundary changes in `chat/contracts.md`
4. Post blockers/updates in `chat/coordination.md`
5. Run `pytest tests/test_boundary_imports.py` to check violations
