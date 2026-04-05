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

[2026-04-04T19:06:03.0507479-04:00]
owner: mt5pipe/compiler/, mt5pipe/catalog/, mt5pipe/truth/, mt5pipe/storage/paths.py (compiler/catalog/truth paths only), tests/test_catalog.py, tests/test_compiler.py, tests/test_contracts.py
plan: audit current core services, harden compile_dataset_spec/inspect_artifact/diff_artifacts, enforce artifact status lifecycle + publish gate, expand catalog/query tests, avoid CLI/config/legacy-builder files owned by Agent B.
[2026-04-04T19:09:19.1508947-04:00]
update: adding stable core entrypoints compile_dataset_spec(spec_path, publish=True), inspect_artifact(ref), diff_artifacts(left_ref, right_ref). Under the hood I�m tightening catalog lookup + truth-gated lifecycle, but not changing CLI files or spec ownership.
[2026-04-04T19:23:30-04:00]
update: traced remaining core failures to Windows manifest sidecar path length (>260 chars) for feature/label/state artifacts. Fix will stay inside compiler/storage path generation; stable service APIs remain unchanged.
[2026-04-04T19:24:40-04:00]
update: core service path is green. compile_dataset_spec/inspect_artifact/diff_artifacts are implemented and stable. Also shortened manifest sidecar storage paths to avoid Windows path-length failures during state/feature/label/dataset artifact writes.
[2026-04-04T19:25:00-04:00]
owner: mt5pipe/contracts/, mt5pipe/state/, tests for contracts/state/public boundary
plan: harden state-side artifact refs and window contracts, add machine-native state/window materialization + public exports, keep cross-sector boundary clean for Agent 2/3.
[2026-04-04T20:40:00-04:00]
update: published new shared state contracts in mt5pipe.contracts.state and expanded mt5pipe.state.public. Stable imports now include TickArtifactRef, StateArtifactRef, StateWindowArtifactRef, StateWindowRequest, StateWindowRecord, load_state_artifact, and materialize_state_windows.
update: state sector no longer imports compiler/catalog internals. StateService keeps compiler compatibility for materialize_state(...) but now also supports canonical tick state + rolling state-window artifacts.
