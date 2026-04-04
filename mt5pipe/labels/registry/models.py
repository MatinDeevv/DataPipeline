"""Typed contracts for label registry entries."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class LabelPack(BaseModel):
    """Declarative description of a label family pack."""

    schema_version: str = Field(default="1.0.0")
    label_pack_name: str
    version: str
    description: str | None = None
    base_clock: str
    horizons_minutes: list[int]
    generator_refs: list[str]
    parameters: dict[str, int | float | str | bool] = Field(default_factory=dict)
    exclusions: list[str] = Field(default_factory=list)
    purge_rows: int
    output_columns: list[str]
    status: str = "draft"

    @property
    def key(self) -> str:
        return f"{self.label_pack_name}@{self.version}"

    @model_validator(mode="after")
    def validate_pack(self) -> "LabelPack":
        if not self.horizons_minutes:
            raise ValueError("horizons_minutes must not be empty")
        if any(h <= 0 for h in self.horizons_minutes):
            raise ValueError("horizons_minutes must be positive")
        if len(set(self.horizons_minutes)) != len(self.horizons_minutes):
            raise ValueError("horizons_minutes must be unique")
        if self.purge_rows < max(self.horizons_minutes):
            raise ValueError("purge_rows must be >= max(horizons_minutes)")
        if not self.generator_refs:
            raise ValueError("generator_refs must not be empty")
        if len(set(self.output_columns)) != len(self.output_columns):
            raise ValueError("output_columns must be unique")
        return self
