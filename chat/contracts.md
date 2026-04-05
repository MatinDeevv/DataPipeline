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

### [2026-04-04 20:58]

```
agent: agent_3
type: contract-change
module: mt5pipe.compiler.public, mt5pipe.compiler.service, mt5pipe.compiler.models.LineageManifest.metadata
symbol: compile_dataset_spec, inspect_artifact, diff_artifacts, ArtifactInspection, ArtifactDiff, DatasetSpec.state_artifact_ref/feature_artifact_refs/label_artifact_ref
old: compiler-era dataset builds mainly assumed version-ref materialization, compiler.public did not present compile/inspect/diff as one stable surface, and artifact-ref state builds could not honor filled-row filtering from public state artifacts
new: compiler.public re-exports compile/inspect/diff dataclasses and helpers; compiler manifests/inspection metadata now carry requested_feature_selectors, feature_artifact_refs, source_modes, build_row_stats, split_row_counts, state_artifact_ref, and label_artifact_ref; artifact-ref state builds derive filled-row exclusion from public state trust flags when raw _filled markers are not present
impact: CLI/tests/other agents can stay on mt5pipe.compiler.public and artifact-backed Phase 3 datasets remain inspectable/diffable without cross-sector internal imports
docs_updated: yes
notes: config/datasets/xau_m1_nonhuman_v1.yaml is the Phase 3 example spec; real workspace execution still requires the referenced machine-native feature artifacts to be present in the local catalog/worktree
```

### [2026-04-04 20:58]

```
agent: agent_3
type: contract-change
module: mt5pipe.truth.service
symbol: TruthService.evaluate_dataset / TrustReport publish gate
old: truth evaluation only covered coarse dataset quality and did not explicitly reject the main Phase 3 artifact failure modes
new: truth gate now enforces coverage, split integrity, duplicate primary-clock rows/leakage, required feature columns, per-family missingness thresholds for time/session/quality/htf_context/disagreement/event_shape/entropy, warmup/drop-row sanity, source quality, lineage completeness, and manifest hash integrity; hard-failure codes now include dataset_coverage_failure, split_integrity_failure, leakage_or_duplicate_timestamp_failure, missing_required_feature_columns, feature_family_missingness_threshold_exceeded, warmup_or_drop_row_sanity_failure, source_quality_below_threshold, lineage_incomplete, and manifest_hash_mismatch
impact: compiler publication is deterministically blocked on bad Phase 3 artifacts and inspect/diff consumers can rely on richer trust reports
docs_updated: yes
notes: source quality now treats merge QA as a modifier on state quality instead of double-penalizing conflict behavior already reflected in state quality_score
```

### [2026-04-04 20:58]

```
agent: agent_3
type: contract-change
module: mt5pipe.cli.dataset_cmds
symbol: dataset compile-dataset, inspect-dataset, diff-dataset
old: CLI summaries omitted artifact-ref/source-mode/build-row detail and compile output did not expose explicit publish control
new: compile-dataset supports --publish/--no-publish and prints split_rows; inspect-dataset and diff-dataset emit deterministic summaries for requested feature selectors, feature artifact refs, source modes, and build row stats
impact: Phase 3 datasets can be inspected and compared from the CLI without manually opening manifest JSON
docs_updated: yes
notes: output mirrors compiler manifest metadata for inspectability and diffability
```

### [2026-04-04 21:37]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.public, mt5pipe.features.registry.defaults
symbol: add_multiscale_features, multiscale.consistency@1.0.0
old: public surface exposed disagreement/event_shape/entropy as the only machine-native families; no stable multiscale selector existed
new: features.public now re-exports add_multiscale_features and the default registry now resolves multiscale/* to multiscale.consistency@1.0.0 with explicit warmup/dependencies/output columns
impact: compiler/dataset specs can consume multiscale/* without touching feature internals; Agent 3 can compile the new stable selector directly
docs_updated: yes
notes: multiscale.consistency@1.0.0 outputs trend_alignment_5_15_60, return_energy_ratio_5_60, volatility_ratio_5_60, range_expansion_ratio_15_60, and tick_intensity_ratio_5_60
```

### [2026-04-04 21:37]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.disagreement, mt5pipe.features.event_shape, mt5pipe.features.entropy, mt5pipe.features.labels, mt5pipe.labels.service
symbol: Phase 3 machine-native family warmup/missingness behavior; triple_barrier_* tail semantics; label manifest metadata.label_diagnostics
old: disagreement/event_shape/entropy emitted some early-row values before warmup and could synthesize numeric outputs from missing core inputs; triple_barrier_* treated insufficient forward horizon as time-expiry 0; label manifests exposed only basic row/column metadata
new: disagreement/event_shape/entropy now null all family outputs through declared warmup rows and degrade to typed-null columns when core inputs are missing; triple_barrier_* now returns null for insufficient forward horizon and includes the full horizon endpoint; label manifests now include compact horizon/class-balance diagnostics plus exclusions
impact: compiler/truth consumers should expect cleaner warmup/tail nulls for machine-native features and labels, plus richer label artifact metadata for inspectability
docs_updated: yes
notes: label pack key and output column names stay unchanged; only tail availability semantics and manifest metadata became stricter
```
### [2026-04-04 21:38]

```
agent: agent_1
type: contract-change
module: mt5pipe.state.models.StateSnapshot, mt5pipe.state.models.StateArtifactManifest, mt5pipe.state.models.StateWindowRecord
symbol: StateSnapshot.expected_interval_ms/observed_interval_ms/source_participation_score/overlap_confidence_hint/gap_fill_flag, StateCoverageSummary, StateSourceQualitySummary, StateArtifactManifest.coverage_summary/source_quality_summary/time-range fields, StateWindowRecord warmup/completeness/gap/source-quality fields
old: state artifacts exposed basic snapshot/window structure with limited completeness and source-quality metadata
new: state artifacts now carry typed coverage and source-quality summaries plus per-snapshot/per-window completeness, participation, overlap-confidence, staleness, and gap-fill annotations suitable for wider-range machine-native builds
impact: Agent 2 can consume richer PIT-safe state/window metadata from mt5pipe.state.public; Agent 3/compiler/truth can reason about time range, coverage, and source quality from state manifests without guessing
docs_updated: no
notes: additive shape expansion only; no merge/backfill semantics changed
```

### [2026-04-04 21:38]

```
agent: agent_1
type: contract-change
module: mt5pipe.state.public
symbol: StateCoverageSummary, StateSourceQualitySummary, load_state_window_artifact
old: state.public exposed snapshot/window contracts and load_state_artifact/materialize_state_windows but not typed coverage/source-quality summaries or a public window-artifact loader
new: state.public now exports typed coverage/source-quality summary models and load_state_window_artifact for stable cross-sector access to persisted rolling windows
impact: Agent 2/3 can stay on mt5pipe.state.public for window metadata and persisted window loading
docs_updated: no
notes: public surface remains additive and boundary-clean
```

### [2026-04-04 22:44]

```
agent: agent_3
type: contract-change
module: mt5pipe.truth.service, mt5pipe.cli.dataset_cmds
symbol: TrustReport.metrics.dataset_quality, warning_reasons semantics, compile-dataset|inspect-dataset|diff-dataset trust summaries
old: truth warning reasons were coarse and sometimes duplicative; trust metrics did not surface compact dataset-quality detail; CLI summaries omitted decision_summary, rejection/warning reasons, and check-status counts
new: truth reports now include dataset_quality metrics (quality_score, total_nulls, null-columns map, constant-columns list, duplicate timestamp count) and emit more specific warning reasons for source quality, null columns, and constant columns without duplicating generic warning codes; compile/inspect/diff now print trust_decision, trust_check_counts, trust_warning_reasons, and trust_rejection_reasons, with diff exposing trust-reason deltas
impact: research users can diagnose accepted/rejected artifacts directly from compiler outputs without opening manifest/trust JSON by hand
docs_updated: yes
notes: this is a reporting hardening change only; trust hard-fail thresholds were not relaxed
```

### [2026-04-04 22:44]

```
agent: agent_3
type: contract-change
module: config/datasets/xau_m1_nonhuman_v1.yaml, config/datasets/xau_m1_core_v1.yaml
symbol: example DatasetSpec selector bundle and synchronized date range
old: example specs targeted a narrow 2026 slice; xau_m1_nonhuman_v1 pinned stale feature_artifact_refs that were no longer the stable machine-native path
new: example specs now target the wider synchronized range 2024-02-26..2024-03-01; xau_m1_nonhuman_v1 compiles from stable public selectors only and includes multiscale/* while removing explicit feature_artifact_refs
impact: compiler-facing workflows and tests now exercise the real Phase 4 checkpoint path through state_version_ref + stable selectors instead of stale artifact aliases
docs_updated: yes
notes: multiscale/* is included because the current public feature registry resolves multiscale.consistency@1.0.0 cleanly in the live workspace and focused tests
```

### [2026-04-04 23:34]

```
agent: agent_3
type: contract-change
module: mt5pipe.truth.service, mt5pipe.cli.dataset_cmds
symbol: TrustReport.metrics.quality_caveat_summary semantics, TrustReport.metrics.source_quality observability detail, compile-dataset|inspect-dataset|diff-dataset quality/trust summary lines
old: expected sparse nulls and slice-trivial constants were emitted as generic dataset warnings alongside real blockers; source-quality reporting did not distinguish merge_qa from merge_diagnostics fallback; CLI output did not surface accepted caveats, per-family caveat summaries, or source-quality context directly
new: truth reports now classify expected sparse nulls and slice-trivial constants as accepted_caveats, keep unexpected nulls/blocking constants/source-quality shortfalls in green_blockers or publication_blockers, and expose merge_diagnostics fallback metrics inside source_quality; compile/inspect/diff now print quality_caveats, source_quality_metrics, and per-family quality summaries, with diff also printing feature_families_left/right
impact: Phase 4 checkpoint review can distinguish accepted slice behavior from true blockers deterministically from CLI output; artifacts can return to green when only accepted caveats remain without hiding those caveats
docs_updated: yes
notes: trust hard-fail thresholds remain unchanged; this is a truth/reporting hardening pass rather than a publication-gate relaxation
```

### [2026-04-04 22:31]

```
agent: agent_2
type: contract-change
module: mt5pipe.labels.service
symbol: label manifest metadata.label_diagnostics.horizons_minutes/max_horizon_minutes/recommended_min_embargo_rows
old: label_diagnostics exposed per-horizon null/class-balance summaries plus purge_rows/exclusions, but did not explicitly summarize the pack-wide horizon span or recommended embargo floor
new: label_diagnostics now also includes horizons_minutes, max_horizon_minutes, and recommended_min_embargo_rows to make purge/embargo expectations explicit for compiler/inspection consumers
impact: inspect/diff/trust consumers can reason about label horizon scope and minimum safe embargo directly from label artifact metadata without inferring it from raw columns
docs_updated: yes
notes: additive metadata only; label pack key, output columns, and generation logic stay unchanged
```
### [2026-04-04 22:30]

```
agent: agent_1
type: contract-change
module: mt5pipe.state.public / mt5pipe.state.service materialize_state_windows
symbol: materialize_state_windows request-range behavior
old: when a source StateArtifactRef/TickArtifactRef spanned a wider date range than the request, state windows were built across the full source and returned anchors outside the requested date range; lineage refs also only covered the request dates
new: state windows may still use a wider source artifact for PIT-safe warmup context, but emitted window anchors are filtered to the requested date range, request dates must lie within the source ref range, and lineage/input refs cover the full source range actually used
impact: Agent 2 can rely on state-window artifacts matching requested anchor dates while preserving prior-context warmup; Agent 3 can rely on state-window lineage covering the actual source partitions used
docs_updated: no
notes: no public symbol additions; boundary behavior is stricter and more deterministic for wider-range requests
```

### [2026-04-04 23:33]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.registry.defaults
symbol: htf_context.standard_context@1.0.0, disagreement.microstructure_pressure@1.0.0
old: stable htf_context/* exposed higher-timeframe *_tick_count columns; stable disagreement/* exposed spread_divergence_proxy_bps, conflict_burst_15, staleness_asymmetry_15, and disagreement_entropy_30 alongside the core pressure fields
new: stable htf_context/* now excludes *_tick_count from production output columns; stable disagreement/* is narrowed to mid_divergence_proxy_bps, disagreement_pressure_bps, disagreement_zscore_60, and disagreement_burst_15
impact: compiler/truth/dataset specs keep the same family selectors, but published feature artifacts for the stable selector set now omit the null-heavy HTF tick-count columns and the slice-trivial disagreement columns
docs_updated: yes
notes: builders still compute the broader internal disagreement surface; only the stable registry contract was tightened for the current nonhuman path
```

### [2026-04-04 23:33]

```
agent: agent_2
type: contract-change
module: mt5pipe.features.disagreement, mt5pipe.features.event_shape, mt5pipe.labels.service
symbol: disagreement/* warmup semantics, event_shape/* warmup semantics, metadata.label_diagnostics.constant_output_columns
old: disagreement/event_shape cleanup still effectively assumed family-wide warmup in tests, and label diagnostics did not explicitly report constant output columns
new: disagreement/* and event_shape/* now treat warmup at the column level (instantaneous columns can materialize immediately while rolling columns stay null until ready); label diagnostics now include constant_output_columns for the current label artifact
impact: compiler/truth consumers should expect earlier availability for non-rolling event/disagreement columns and can classify trivial labels directly from manifest metadata
docs_updated: yes
notes: no label pack keys or column names changed
```
### [2026-04-04 23:32]

```
agent: agent_1
type: contract-change
module: mt5pipe.state.service / mt5pipe.state.public load_state_artifact, load_state_window_artifact, materialize_state
symbol: persisted state/state-window idempotence and canonical-quality-backed source_quality behavior
old: repeated state or state-window materialization could append duplicate persisted rows because state partitions were not rewritten idempotently and loaders returned concatenated duplicates; bar-backed state quality/source_quality_hint relied only on coarse bar-level heuristics even when canonical tick quality evidence was available
new: state and state-window partition writes are reset before rewrite and public loaders deduplicate on stable keys, making persisted loads idempotent; bar-backed state quality_score/source_quality_hint now prefer per-bar canonical tick quality/conflict/dual-source evidence when available
impact: Agent 2 and Agent 3 can rely on stable persisted state/state-window loads and more informative source-quality inputs on the current nonhuman dataset path without importing truth/compiler internals
docs_updated: no
notes: additive behavioral hardening only; no new public symbols or contract fields were introduced
```
