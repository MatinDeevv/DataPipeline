"""Manifest and spec helpers for the dataset compiler."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from mt5pipe.compiler.models import DatasetSpec, LineageManifest
from mt5pipe.config.models import MergeConfig
from mt5pipe.storage.paths import StoragePaths


def load_dataset_spec(path: Path) -> DatasetSpec:
    """Load a DatasetSpec from YAML or JSON."""
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        payload = yaml.safe_load(raw)
    else:
        payload = json.loads(raw)
    return DatasetSpec.model_validate(payload)


def build_id_now() -> str:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    return f"build.{ts}"


def code_version() -> str:
    """Best-effort code version identifier without assuming git is available."""
    return "workspace-local-no-git"


def merge_config_ref(cfg: MergeConfig) -> str:
    payload = json.dumps(cfg.model_dump(mode="json"), sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]
    return f"merge.default@{digest}"


def compute_content_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_stage_artifact_id(artifact_kind: str, logical_name: str, created_at: dt.datetime, content_hash: str) -> str:
    ts = created_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{artifact_kind}.{logical_name}.{ts}.{content_hash[:8]}"


def build_artifact_id(dataset_name: str, created_at: dt.datetime, content_hash: str) -> str:
    return build_stage_artifact_id("dataset", dataset_name, created_at, content_hash)


def build_stage_manifest_id(artifact_kind: str, logical_name: str, created_at: dt.datetime, content_hash: str) -> str:
    ts = created_at.strftime("%Y%m%dT%H%M%SZ")
    return f"manifest.{artifact_kind}.{logical_name}.{ts}.{content_hash[:8]}"


def build_manifest_id(dataset_name: str, created_at: dt.datetime, content_hash: str) -> str:
    return build_stage_manifest_id("dataset", dataset_name, created_at, content_hash)


def write_manifest_sidecar(manifest: LineageManifest, paths: StoragePaths) -> Path:
    path = paths.manifest_file(manifest.artifact_kind, manifest.logical_name, manifest.manifest_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True, default=str), encoding="utf-8")
    return path


def read_manifest_sidecar(path: Path) -> LineageManifest:
    return LineageManifest.model_validate_json(path.read_text(encoding="utf-8"))
