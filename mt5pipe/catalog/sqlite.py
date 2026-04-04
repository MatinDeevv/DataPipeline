"""SQLite metadata catalog for compiler-era artifacts."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

from mt5pipe.catalog.models import ArtifactRecord, BuildRunRecord
from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.features.registry.models import FeatureSpec
from mt5pipe.labels.registry.models import LabelPack
from mt5pipe.truth.models import TrustReport
from mt5pipe.utils.time import utc_now

_CREATE_FEATURE_SPECS_SQL = """
CREATE TABLE IF NOT EXISTS feature_specs (
    feature_key TEXT PRIMARY KEY,
    family TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

_CREATE_LABEL_PACKS_SQL = """
CREATE TABLE IF NOT EXISTS label_packs (
    label_pack_key TEXT PRIMARY KEY,
    label_pack_name TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

_CREATE_DATASET_SPECS_SQL = """
CREATE TABLE IF NOT EXISTS dataset_specs (
    dataset_spec_key TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    version TEXT NOT NULL,
    status TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

_CREATE_BUILD_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS build_runs (
    build_id TEXT PRIMARY KEY,
    dataset_spec_key TEXT NOT NULL,
    status TEXT NOT NULL,
    code_version TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error_message TEXT,
    artifact_id TEXT
)
"""

_CREATE_ARTIFACTS_SQL = """
CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    artifact_kind TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    logical_version TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    manifest_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    build_id TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

_CREATE_ARTIFACT_INPUTS_SQL = """
CREATE TABLE IF NOT EXISTS artifact_inputs (
    artifact_id TEXT NOT NULL,
    input_kind TEXT NOT NULL,
    input_ref TEXT NOT NULL,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (artifact_id, input_kind, input_ref, role)
)
"""

_CREATE_TRUST_REPORTS_SQL = """
CREATE TABLE IF NOT EXISTS trust_reports (
    report_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    truth_policy_version TEXT NOT NULL,
    status TEXT NOT NULL,
    score_total REAL NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

_CREATE_QA_RESULTS_SQL = """
CREATE TABLE IF NOT EXISTS qa_results (
    report_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    score REAL NOT NULL,
    metric_json TEXT NOT NULL,
    threshold_json TEXT NOT NULL,
    failure_reason TEXT,
    PRIMARY KEY (report_id, check_name)
)
"""

_CREATE_ARTIFACT_ALIASES_SQL = """
CREATE TABLE IF NOT EXISTS artifact_aliases (
    alias_key TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    alias_type TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class CatalogDB:
    """SQLite-backed metadata catalog for dataset compiler artifacts."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), isolation_level="DEFERRED")
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()

    def _init_tables(self) -> None:
        with self._conn:
            self._conn.execute(_CREATE_FEATURE_SPECS_SQL)
            self._conn.execute(_CREATE_LABEL_PACKS_SQL)
            self._conn.execute(_CREATE_DATASET_SPECS_SQL)
            self._conn.execute(_CREATE_BUILD_RUNS_SQL)
            self._conn.execute(_CREATE_ARTIFACTS_SQL)
            self._conn.execute(_CREATE_ARTIFACT_INPUTS_SQL)
            self._conn.execute(_CREATE_TRUST_REPORTS_SQL)
            self._conn.execute(_CREATE_QA_RESULTS_SQL)
            self._conn.execute(_CREATE_ARTIFACT_ALIASES_SQL)

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _checksum(payload: str) -> str:
        import hashlib

        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def register_feature_specs(self, specs: list[FeatureSpec]) -> None:
        now = utc_now().isoformat()
        with self._conn:
            for spec in specs:
                spec_json = json.dumps(spec.model_dump(mode="json"), sort_keys=True, default=str)
                self._conn.execute(
                    """INSERT OR REPLACE INTO feature_specs
                       (feature_key, family, feature_name, version, status, spec_json, checksum, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (spec.key, spec.family, spec.feature_name, spec.version, spec.status, spec_json, self._checksum(spec_json), now),
                )

    def register_label_packs(self, packs: list[LabelPack]) -> None:
        now = utc_now().isoformat()
        with self._conn:
            for pack in packs:
                pack_json = json.dumps(pack.model_dump(mode="json"), sort_keys=True, default=str)
                self._conn.execute(
                    """INSERT OR REPLACE INTO label_packs
                       (label_pack_key, label_pack_name, version, status, spec_json, checksum, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (pack.key, pack.label_pack_name, pack.version, pack.status, pack_json, self._checksum(pack_json), now),
                )

    def register_dataset_spec(self, spec: DatasetSpec, *, status: str = "active") -> None:
        spec_json = json.dumps(spec.model_dump(mode="json"), sort_keys=True, default=str)
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO dataset_specs
                   (dataset_spec_key, dataset_name, version, status, spec_json, checksum, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (spec.key, spec.dataset_name, spec.version, status, spec_json, self._checksum(spec_json), utc_now().isoformat()),
            )

    def start_build(self, dataset_spec_key: str, code_version: str, build_id: str) -> BuildRunRecord:
        started_at = utc_now()
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO build_runs
                   (build_id, dataset_spec_key, status, code_version, started_at)
                   VALUES (?, ?, 'running', ?, ?)""",
                (build_id, dataset_spec_key, code_version, started_at.isoformat()),
            )
        return BuildRunRecord(
            build_id=build_id,
            dataset_spec_key=dataset_spec_key,
            status="running",
            code_version=code_version,
            started_at=started_at,
        )

    def finish_build(self, build_id: str, status: str, artifact_id: str | None = None, error_message: str = "") -> None:
        with self._conn:
            self._conn.execute(
                """UPDATE build_runs
                   SET status=?, finished_at=?, error_message=?, artifact_id=?
                   WHERE build_id=?""",
                (status, utc_now().isoformat(), error_message, artifact_id, build_id),
            )

    def register_artifact(self, manifest: LineageManifest, manifest_uri: str) -> None:
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO artifacts
                   (artifact_id, artifact_kind, logical_name, logical_version, artifact_uri, manifest_uri,
                    content_hash, status, build_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    manifest.artifact_id,
                    manifest.artifact_kind,
                    manifest.logical_name,
                    manifest.logical_version,
                    manifest.artifact_uri,
                    manifest_uri,
                    manifest.content_hash,
                    manifest.status,
                    manifest.build_id,
                    manifest.created_at.isoformat(),
                ),
            )
            self._conn.execute("DELETE FROM artifact_inputs WHERE artifact_id=?", (manifest.artifact_id,))
            for ordinal, input_ref in enumerate(manifest.input_partition_refs):
                self._conn.execute(
                    """INSERT INTO artifact_inputs (artifact_id, input_kind, input_ref, role, ordinal)
                       VALUES (?, 'partition', ?, 'upstream', ?)""",
                    (manifest.artifact_id, input_ref, ordinal),
                )
            for ordinal, input_ref in enumerate(manifest.state_artifact_refs):
                self._conn.execute(
                    """INSERT INTO artifact_inputs (artifact_id, input_kind, input_ref, role, ordinal)
                       VALUES (?, 'state_artifact', ?, 'state', ?)""",
                    (manifest.artifact_id, input_ref, ordinal),
                )
            for ordinal, input_ref in enumerate(manifest.feature_spec_refs):
                self._conn.execute(
                    """INSERT INTO artifact_inputs (artifact_id, input_kind, input_ref, role, ordinal)
                       VALUES (?, 'feature_spec', ?, 'feature', ?)""",
                    (manifest.artifact_id, input_ref, ordinal),
                )
            if manifest.label_pack_ref:
                self._conn.execute(
                    """INSERT INTO artifact_inputs (artifact_id, input_kind, input_ref, role, ordinal)
                       VALUES (?, 'label_pack', ?, 'label', 0)""",
                    (manifest.artifact_id, manifest.label_pack_ref),
                )
            for ordinal, input_ref in enumerate(manifest.parent_artifact_refs):
                self._conn.execute(
                    """INSERT INTO artifact_inputs (artifact_id, input_kind, input_ref, role, ordinal)
                       VALUES (?, 'artifact', ?, 'parent', ?)""",
                    (manifest.artifact_id, input_ref, ordinal),
                )

    def register_trust_report(self, report: TrustReport) -> None:
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO trust_reports
                   (report_id, artifact_id, truth_policy_version, status, score_total, report_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    report.report_id,
                    report.artifact_id,
                    report.truth_policy_version,
                    report.status,
                    report.trust_score_total,
                    json.dumps(report.model_dump(mode="json"), sort_keys=True, default=str),
                    report.generated_at.isoformat(),
                ),
            )
            self._conn.execute("DELETE FROM qa_results WHERE report_id=?", (report.report_id,))
            for check in report.checks:
                self._conn.execute(
                    """INSERT INTO qa_results
                       (report_id, check_name, status, score, metric_json, threshold_json, failure_reason)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        report.report_id,
                        check.check_name,
                        check.status,
                        check.score,
                        json.dumps(check.metrics, sort_keys=True),
                        json.dumps(check.thresholds, sort_keys=True),
                        check.failure_reason,
                    ),
                )

    def upsert_alias(self, alias_key: str, artifact_id: str, alias_type: str = "logical") -> None:
        with self._conn:
            self._conn.execute(
                """INSERT OR REPLACE INTO artifact_aliases (alias_key, artifact_id, alias_type, created_at)
                   VALUES (?, ?, ?, ?)""",
                (alias_key, artifact_id, alias_type, utc_now().isoformat()),
            )

    def get_artifact(self, artifact_id: str) -> ArtifactRecord | None:
        row = self._conn.execute("SELECT * FROM artifacts WHERE artifact_id=?", (artifact_id,)).fetchone()
        if row is None:
            return None
        return ArtifactRecord(
            artifact_id=row["artifact_id"],
            artifact_kind=row["artifact_kind"],
            logical_name=row["logical_name"],
            logical_version=row["logical_version"],
            artifact_uri=row["artifact_uri"],
            manifest_uri=row["manifest_uri"],
            content_hash=row["content_hash"],
            status=row["status"],
            build_id=row["build_id"],
            created_at=dt.datetime.fromisoformat(row["created_at"]),
        )

    def resolve_artifact(self, ref: str) -> ArtifactRecord | None:
        direct = self.get_artifact(ref)
        if direct is not None:
            return direct

        alias = self._conn.execute(
            "SELECT artifact_id FROM artifact_aliases WHERE alias_key=?",
            (ref,),
        ).fetchone()
        if alias is not None:
            return self.get_artifact(alias["artifact_id"])

        if ref.startswith("dataset://") and "@" in ref:
            logical = ref[len("dataset://") :]
            name, version = logical.split("@", 1)
            row = self._conn.execute(
                """SELECT * FROM artifacts
                   WHERE logical_name=? AND logical_version=? AND status='published'
                   ORDER BY created_at DESC LIMIT 1""",
                (name, version),
            ).fetchone()
            if row is not None:
                return self.get_artifact(row["artifact_id"])
        return None

    def get_trust_report_json(self, artifact_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT report_json FROM trust_reports WHERE artifact_id=? ORDER BY created_at DESC LIMIT 1",
            (artifact_id,),
        ).fetchone()
        return row["report_json"] if row is not None else None
