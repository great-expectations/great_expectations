"""Test v3 API RuleBasedProfiler serialization."""
import pytest

from great_expectations.rule_based_profiler.config.base import (
    RuleBasedProfilerConfig,
    ruleBasedProfilerConfigSchema,
)


@pytest.mark.parametrize(
    "rbp_config,expected_serialized_rbp_config",
    [
        pytest.param(
            RuleBasedProfilerConfig(
                name="my_rbp",
                config_version=1.0,
                rules={},
            ),
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_rbp",
                "rules": {},
                "variables": None,
            },
            id="minimal_with_name",
        ),
        pytest.param(
            RuleBasedProfilerConfig(
                name="my_rbp",
                id="dd223ad9-as12-d823-239a-382sadaf8112",
                config_version=1.0,
                rules={},
            ),
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "id": "dd223ad9-as12-d823-239a-382sadaf8112",
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_rbp",
                "rules": {},
                "variables": None,
            },
            id="minimal_with_name_and_id",
        ),
    ],
)
@pytest.mark.unit
def test_rule_based_profiler_config_is_serialized(
    rbp_config: RuleBasedProfilerConfig, expected_serialized_rbp_config: dict
):
    """RBP Config should be serialized appropriately with/without optional params."""
    observed_dump = ruleBasedProfilerConfigSchema.dump(rbp_config)
    assert observed_dump == expected_serialized_rbp_config

    loaded_data = ruleBasedProfilerConfigSchema.load(observed_dump)
    for attr in ("class_name", "module_name"):
        loaded_data.pop(attr)
    observed_load = RuleBasedProfilerConfig(**loaded_data)
    assert observed_load.to_json_dict() == rbp_config.to_json_dict()
