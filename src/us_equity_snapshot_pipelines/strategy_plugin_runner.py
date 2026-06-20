from __future__ import annotations

from quant_strategy_plugins import strategy_plugin_runner as _base
from quant_strategy_plugins.strategy_plugin_runner import *  # noqa: F403
from quant_strategy_plugins.strategy_plugin_runner import main
from us_equity_snapshot_pipelines.ibit_zscore_exit_plugin import (
    IBIT_ZSCORE_EXIT_SCHEMA_VERSION,
    PLUGIN_IBIT_ZSCORE_EXIT,
    build_ibit_zscore_exit_signal,
    write_ibit_zscore_exit_outputs,
)

IBIT_SMART_DCA_STRATEGY = "ibit_smart_dca"

IBIT_ZSCORE_EXIT_POLICY = _base.PluginConsumptionPolicy(
    plugin=PLUGIN_IBIT_ZSCORE_EXIT,
    strategy=IBIT_SMART_DCA_STRATEGY,
    notification_allowed=True,
    position_control_allowed=False,
    evidence_status=_base.EVIDENCE_NOTIFICATION_ONLY,
    since_version="strategy_plugins.v1",
    description=(
        "Research-only dynamic MVRV Z-Score exit/parking signal for the IBIT Smart DCA strategy; "
        "position control requires a separate positive promotion artifact."
    ),
)
IBIT_ZSCORE_EXIT_SPEC = _base.PluginExecutionSpec(
    default_plugin=PLUGIN_IBIT_ZSCORE_EXIT,
    build_payload=build_ibit_zscore_exit_signal,
    write_outputs=write_ibit_zscore_exit_outputs,
)


def _register_ibit_zscore_exit_plugin() -> None:
    _base.PLUGIN_SCHEMA_VERSIONS[PLUGIN_IBIT_ZSCORE_EXIT] = (IBIT_ZSCORE_EXIT_SCHEMA_VERSION,)
    _base.PLUGIN_CONSUMPTION_POLICY_REGISTRY[
        (PLUGIN_IBIT_ZSCORE_EXIT, IBIT_SMART_DCA_STRATEGY)
    ] = IBIT_ZSCORE_EXIT_POLICY
    _base.PLUGIN_COMPATIBLE_STRATEGIES[PLUGIN_IBIT_ZSCORE_EXIT] = (IBIT_SMART_DCA_STRATEGY,)
    _base.LOCALIZED_PLUGIN_LABELS[PLUGIN_IBIT_ZSCORE_EXIT] = {
        "en-US": "IBIT Z-Score exit",
        "zh-CN": "IBIT Z-Score 逃顶",
    }
    _base.PLUGIN_SPECS[PLUGIN_IBIT_ZSCORE_EXIT] = IBIT_ZSCORE_EXIT_SPEC
    _base.PLUGIN_RUNNERS[PLUGIN_IBIT_ZSCORE_EXIT] = run_ibit_zscore_exit_plugin


def run_ibit_zscore_exit_plugin(plugin_config, default_mode):
    normalized_config = dict(plugin_config)
    if not str(normalized_config.get("prices", "")).strip() and str(
        normalized_config.get("zscore_metrics", "")
    ).strip():
        normalized_config["prices"] = normalized_config["zscore_metrics"]
    return _base._run_table_strategy_plugin(normalized_config, default_mode, IBIT_ZSCORE_EXIT_SPEC)


_register_ibit_zscore_exit_plugin()

if __name__ == "__main__":
    raise SystemExit(main())
