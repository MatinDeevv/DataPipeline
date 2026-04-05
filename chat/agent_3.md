# Agent 3 — Compiler / Truth / Catalog

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
