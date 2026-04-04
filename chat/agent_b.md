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

