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

[2026-04-04T19:06:28.4687860-04:00]
owner: mt5pipe/cli/dataset_cmds.py, mt5pipe/cli/app.py (registration only if needed), mt5pipe/features/dataset.py, config/datasets/xau_m1_core_v1.yaml, tests/* (CLI + compatibility + spec path)
plan: switch dataset CLI to service-level compiler APIs, add stable output formatting + failure handling, add legacy build compatibility facade when compiler/spec path is safe, harden example DatasetSpec and interface tests.

[2026-04-04T19:08:16.8631103-04:00]
update: CLI will prefer compiler.service compile_dataset_spec/inspect_artifact/diff_artifacts and fall back to the existing DatasetCompiler adapter only if the new functions are absent.
update: legacy build compatibility path will route through compiler only for compiler-compatible M1/default stable configs; it will use publish=false and mirror split outputs back to legacy datasets/name=*/split=* paths.

[2026-04-04T19:14:34.9594404-04:00]
update: compile-dataset/inspect-dataset/diff-dataset now format stable key-value output from service-level results and return explicit non-zero errors on rejection/exceptions.
update: legacy build now generates a temporary DatasetSpec with stable selectors (time/session/quality/htf_context) and mirrors compiler artifact splits back into legacy dataset paths when compile_dataset_spec is available.

[2026-04-04T19:16:09.9298113-04:00]
blocker-for-agent-a: mt5pipe/compiler/service.py is still absent in this worktree. My wrappers now handle the missing-module case, but they are ready to bind to compile_dataset_spec/inspect_artifact/diff_artifacts as soon as that module lands.
update: focused interface tests pass for the stubbed service contract and legacy compatibility facade.

[2026-04-04T19:16:50.1786266-04:00]
blocker-for-agent-a: verified current mt5pipe/compiler/service.py contains DatasetCompiler but not compile_dataset_spec/inspect_artifact/diff_artifacts yet. My CLI wrappers still fall back cleanly; once those functions exist they will be used first.

[2026-04-04T20:16:14.2124821-04:00]
agent: agent_2
area: features|labels
summary: starting Phase 3 feature-family work; auditing public surfaces, registry defaults, builder architecture, and PIT-safe alignment against mt5pipe.state.public only.
needs: none
files: mt5pipe/features/, mt5pipe/labels/, tests/*feature* tests/*label* registry/public surfaces

[2026-04-04T20:30:19.0936436-04:00]
agent: agent_2
area: features|labels
summary: shipped stable Phase 3 families disagreement/*, event_shape/*, entropy/*; expanded features.public and labels.public with registry helpers plus artifact ref/load helpers; labels remain compatible.
needs: Agent 3 can compile selectors disagreement/*, event_shape/*, entropy/* once compiler module is restored in this worktree.
files: mt5pipe/features/public.py, mt5pipe/features/service.py, mt5pipe/features/registry/defaults.py, mt5pipe/features/disagreement/, mt5pipe/features/event_shape/, mt5pipe/features/entropy/, mt5pipe/features/artifacts.py, mt5pipe/features/types.py, mt5pipe/labels/public.py, mt5pipe/labels/artifacts.py, tests/test_features_phase3.py, chat/contracts.md, docs/parallel_dev_architecture.md
selector-notes: disagreement.microstructure_pressure@1.0.0 -> outputs mid_divergence_proxy_bps, spread_divergence_proxy_bps, disagreement_pressure_bps, disagreement_zscore_60, conflict_burst_15, disagreement_burst_15, staleness_asymmetry_15, disagreement_entropy_30; event_shape.flow_shape@1.0.0 -> outputs tick_rate_hz, interarrival_mean_ms, burstiness_20, silence_ratio_20, direction_switch_rate_20, signed_run_length, path_efficiency_20, tortuosity_20; entropy.market_complexity@1.0.0 -> outputs return_sign_shannon_entropy_30, return_permutation_entropy_30, return_sample_entropy_30, volatility_approx_entropy_30.
blocker: tests/test_compiler.py and tests/test_boundary_imports.py::test_public_modules_importable are blocked by missing mt5pipe.compiler.service in the current tree, not by feature-sector failures.

