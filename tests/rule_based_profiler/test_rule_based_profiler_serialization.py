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
                id_="dd223ad9-as12-d823-239a-382sadaf8112",
                config_version=1.0,
                rules={},
            ),
            {
                "class_name": "RuleBasedProfiler",
                "config_version": 1.0,
                "id_": "dd223ad9-as12-d823-239a-382sadaf8112",
                "module_name": "great_expectations.rule_based_profiler",
                "name": "my_rbp",
                "rules": {},
                "variables": None,
            },
            id="minimal_with_name_and_id",
        ),
    ],
)
def test_rule_based_profiler_config_is_serialized(
    rbp_config: RuleBasedProfilerConfig, expected_serialized_rbp_config: dict
):
    """RBP Config should be serialized appropriately with/without optional params."""
    observed = ruleBasedProfilerConfigSchema.dump(rbp_config)

    assert dict(observed) == expected_serialized_rbp_config
