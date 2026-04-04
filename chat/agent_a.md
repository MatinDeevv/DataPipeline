[2026-04-04T19:06:03.0507479-04:00]
owner: mt5pipe/compiler/, mt5pipe/catalog/, mt5pipe/truth/, mt5pipe/storage/paths.py (compiler/catalog/truth paths only), tests/test_catalog.py, tests/test_compiler.py, tests/test_contracts.py
plan: audit current core services, harden compile_dataset_spec/inspect_artifact/diff_artifacts, enforce artifact status lifecycle + publish gate, expand catalog/query tests, avoid CLI/config/legacy-builder files owned by Agent B.
[2026-04-04T19:09:19.1508947-04:00]
update: adding stable core entrypoints compile_dataset_spec(spec_path, publish=True), inspect_artifact(ref), diff_artifacts(left_ref, right_ref). Under the hood I’m tightening catalog lookup + truth-gated lifecycle, but not changing CLI files or spec ownership.
[2026-04-04T19:23:30-04:00]
update: traced remaining core failures to Windows manifest sidecar path length (>260 chars) for feature/label/state artifacts. Fix will stay inside compiler/storage path generation; stable service APIs remain unchanged.
[2026-04-04T19:24:40-04:00]
update: core service path is green. compile_dataset_spec/inspect_artifact/diff_artifacts are implemented and stable. Also shortened manifest sidecar storage paths to avoid Windows path-length failures during state/feature/label/dataset artifact writes.
