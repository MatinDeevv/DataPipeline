"""Default stable feature registry entries."""

from __future__ import annotations

from fnmatch import fnmatch

from mt5pipe.features.registry.models import FeatureSpec


def get_default_feature_specs() -> list[FeatureSpec]:
    """Return the stable registry-backed feature specs."""
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
        FeatureSpec(
            feature_name="microstructure_pressure",
            family="disagreement",
            version="1.0.0",
            description="Dual-source disagreement pressure proxies and burst metrics",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.disagreement:add_disagreement_features",
            output_columns=[
                "mid_divergence_proxy_bps",
                "spread_divergence_proxy_bps",
                "disagreement_pressure_bps",
                "disagreement_zscore_60",
                "conflict_burst_15",
                "disagreement_burst_15",
                "staleness_asymmetry_15",
                "disagreement_entropy_30",
            ],
            dependencies=[
                "close",
                "spread_mean",
                "tick_count",
                "conflict_count",
                "dual_source_ticks",
                "secondary_present_ticks",
                "dual_source_ratio",
            ],
            lookback_rows=60,
            warmup_rows=60,
            point_in_time_safe=True,
            missingness_policy="allow",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "microstructure", "phase3"],
        ),
        FeatureSpec(
            feature_name="flow_shape",
            family="event_shape",
            version="1.0.0",
            description="Arrival-rate, burstiness, silence, run, and path-shape features",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.event_shape:add_event_shape_features",
            output_columns=[
                "tick_rate_hz",
                "interarrival_mean_ms",
                "burstiness_20",
                "silence_ratio_20",
                "direction_switch_rate_20",
                "signed_run_length",
                "path_efficiency_20",
                "tortuosity_20",
            ],
            dependencies=["tick_count", "mid_return"],
            lookback_rows=20,
            warmup_rows=20,
            point_in_time_safe=True,
            missingness_policy="allow",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "flow", "phase3"],
        ),
        FeatureSpec(
            feature_name="market_complexity",
            family="entropy",
            version="1.0.0",
            description="Trailing entropy and complexity metrics over returns and volatility",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.entropy:add_entropy_features",
            output_columns=[
                "return_sign_shannon_entropy_30",
                "return_permutation_entropy_30",
                "return_sample_entropy_30",
                "volatility_approx_entropy_30",
            ],
            dependencies=["mid_return", "realized_vol"],
            lookback_rows=30,
            warmup_rows=30,
            point_in_time_safe=True,
            missingness_policy="allow",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "complexity", "phase3"],
        ),
        FeatureSpec(
            feature_name="consistency",
            family="multiscale",
            version="1.0.0",
            description="Cross-window coherence ratios for returns, volatility, range, and activity",
            input_contract="BuiltBar",
            input_clock="M1",
            output_clock="M1",
            builder_ref="mt5pipe.features.multiscale:add_multiscale_features",
            output_columns=[
                "trend_alignment_5_15_60",
                "return_energy_ratio_5_60",
                "volatility_ratio_5_60",
                "range_expansion_ratio_15_60",
                "tick_intensity_ratio_5_60",
            ],
            dependencies=["mid_return", "high", "low", "tick_count"],
            lookback_rows=60,
            warmup_rows=60,
            point_in_time_safe=True,
            missingness_policy="allow",
            qa_policy_ref="qa.feature.default@1.0.0",
            status="stable",
            tags=["core", "multiscale", "phase4"],
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
