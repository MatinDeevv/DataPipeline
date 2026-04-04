"""Default Phase 1 feature registry entries."""

from __future__ import annotations

from fnmatch import fnmatch

from mt5pipe.features.registry.models import FeatureSpec


def get_default_feature_specs() -> list[FeatureSpec]:
    """Return the core Phase 1 feature specs backed by existing builder code."""
    return [
        FeatureSpec(
            feature_name="cyclical_time",
            family="time",
            version="1.0.0",
            description="Hour-of-day and weekday cyclical encodings",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.time:add_time_features",
            output_columns=["hour", "minute", "weekday", "time_sin", "time_cos", "weekday_sin", "weekday_cos"],
            dependencies=["time_utc"],
            missingness_policy="fail",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "time"],
        ),
        FeatureSpec(
            feature_name="session_flags",
            family="session",
            version="1.0.0",
            description="Asia/London/NY session flags",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.session:add_session_features",
            output_columns=["session_asia", "session_london", "session_ny", "session_overlap"],
            dependencies=["time_utc"],
            missingness_policy="fail",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "session"],
        ),
        FeatureSpec(
            feature_name="spread_quality",
            family="quality",
            version="1.0.0",
            description="Relative spread, conflict ratio, broker diversity",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.quality:add_spread_quality_features",
            output_columns=["relative_spread", "conflict_ratio", "broker_diversity"],
            dependencies=["spread_mean", "close", "conflict_count", "tick_count", "source_count"],
            missingness_policy="fail",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "quality"],
        ),
        FeatureSpec(
            feature_name="standard_context",
            family="htf_context",
            version="1.0.0",
            description="Lagged higher-timeframe bar context from configured context clocks",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.context:add_lagged_bar_features",
            output_columns=[
                "M5_open", "M5_high", "M5_low", "M5_close", "M5_tick_count", "M5_spread_mean", "M5_mid_return", "M5_realized_vol",
                "M15_open", "M15_high", "M15_low", "M15_close", "M15_tick_count", "M15_spread_mean", "M15_mid_return", "M15_realized_vol",
                "H1_open", "H1_high", "H1_low", "H1_close", "H1_tick_count", "H1_spread_mean", "H1_mid_return", "H1_realized_vol",
                "H4_open", "H4_high", "H4_low", "H4_close", "H4_tick_count", "H4_spread_mean", "H4_mid_return", "H4_realized_vol",
                "D1_open", "D1_high", "D1_low", "D1_close", "D1_tick_count", "D1_spread_mean", "D1_mid_return", "D1_realized_vol",
            ],
            dependencies=["time_utc"],
            missingness_policy="allow",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "context"],
        ),
    ]


def resolve_feature_selectors(selectors: list[str]) -> list[FeatureSpec]:
    """Resolve selectors like ``time/*`` or explicit feature keys."""
    specs = get_default_feature_specs()
    resolved: list[FeatureSpec] = []
    seen: set[str] = set()

    for selector in selectors:
        selector = selector.strip()
        if not selector:
            continue

        matched = False
        for spec in specs:
            short_ref = f"{spec.family}/{spec.feature_name}"
            if (
                selector == spec.key
                or selector == short_ref
                or selector == f"{spec.family}/*"
                or fnmatch(spec.key, selector)
                or fnmatch(short_ref, selector)
            ):
                matched = True
                if spec.key not in seen:
                    resolved.append(spec)
                    seen.add(spec.key)

        if not matched:
            raise KeyError(f"Feature selector '{selector}' did not match any registered feature spec")

    return resolved
