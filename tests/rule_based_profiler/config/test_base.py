from typing import Dict

import pytest
from marshmallow.exceptions import ValidationError
from ruamel.yaml.comments import CommentedMap

from great_expectations.rule_based_profiler.config import (
    DomainBuilderConfig,
    DomainBuilderConfigSchema,
    ExpectationConfigurationBuilderConfig,
    ExpectationConfigurationBuilderConfigSchema,
    NotNullSchema,
    ParameterBuilderConfig,
    ParameterBuilderConfigSchema,
    RuleBasedProfilerConfig,
    RuleBasedProfilerConfigSchema,
    RuleConfig,
    RuleConfigSchema,
)
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

# module level markers
pytestmark = [pytest.mark.unit]


def test_not_null_schema_raises_error_with_improperly_implemented_subclass():
    class MySchema(NotNullSchema):
        pass

    with pytest.raises(NotImplementedError) as e:
        MySchema().load({})

    assert "must define its own custom __config_class__" in str(e.value)


def test_not_null_schema_removes_null_values_when_dumping():
    schema = DomainBuilderConfigSchema()
    config = DomainBuilderConfig(
        class_name="DomainBuilder",
        module_name="great_expectations.rule_based_profiler.domain_builder",
        batch_request=None,
    )

    data = schema.dump(config)
    assert isinstance(data, dict)
    assert "class_name" in data and "module_name" in data
    assert "batch_request" not in data


def test_domain_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "DomainBuilder",
    }
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert config.module_name == "great_expectations.rule_based_profiler.domain_builder"


def test_domain_builder_config_successfully_loads_with_optional_args():
    data = {
        "class_name": "DomainBuilder",
        "module_name": "great_expectations.rule_based_profiler.domain_builder",
        "batch_request": {"datasource_name": "my_datasource"},
    }
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_parameter_builder_config_successfully_loads_with_required_args():
    data = {"class_name": "ParameterBuilder", "name": "my_parameter_builder"}
    schema = ParameterBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ParameterBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert (
        config.module_name == "great_expectations.rule_based_profiler.parameter_builder"
    )


def test_parameter_builder_config_successfully_loads_with_optional_args():
    data = {
        "name": "my_parameter_builder",
        "class_name": "ParameterBuilder",
        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
        "batch_request": {"datasource_name": "my_datasource"},
    }
    schema = ParameterBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ParameterBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_parameter_builder_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = ParameterBuilderConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert "'name': ['Missing data for required field.']" in str(e.value)


def test_expectation_configuration_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "ExpectationConfigurationBuilder",
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
    }
    schema = ExpectationConfigurationBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ExpectationConfigurationBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert (
        config.module_name
        == "great_expectations.rule_based_profiler.expectation_configuration_builder"
    )


def test_expectation_configuration_builder_config_successfully_loads_with_optional_args():
    data = {
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
        "class_name": "ExpectationConfigurationBuilder",
        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
        "mostly": 0.9,
        "meta": {"foo": "bar"},
    }
    schema = ExpectationConfigurationBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ExpectationConfigurationBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_expectation_configuration_builder_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = ExpectationConfigurationBuilderConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert (
        "'expectation_type': ['expectation_type missing in expectation configuration builder']"
        in str(e.value)
    )


def test_rule_config_successfully_loads_with_required_args():
    data = {
        "domain_builder": {"class_name": "DomainBuilder"},
        "parameter_builders": [
            {"class_name": "ParameterBuilder", "name": "my_parameter"}
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "ExpectationConfigurationBuilder",
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
            }
        ],
    }
    schema = RuleConfigSchema()
    config = schema.load(data)

    assert isinstance(config, RuleConfig)
    assert isinstance(config.domain_builder, DomainBuilderConfig)
    assert len(config.parameter_builders) == 1 and isinstance(
        config.parameter_builders[0], ParameterBuilderConfig
    )
    assert len(config.expectation_configuration_builders) == 1 and isinstance(
        config.expectation_configuration_builders[0],
        ExpectationConfigurationBuilderConfig,
    )


def test_rule_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = RuleConfigSchema()

    config = schema.load(data)

    assert isinstance(config, RuleConfig)
    assert config.domain_builder is None
    assert config.parameter_builders is None
    assert config.expectation_configuration_builders is None


def test_rule_based_profiler_config_successfully_loads_with_required_args():
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "rules": {
            "rule_1": {
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "name": "my_parameter"}
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "ExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
            },
        },
    }
    schema = RuleBasedProfilerConfigSchema()
    config = schema.load(data)
    assert isinstance(config, dict)
    assert len(config["rules"]) == 1 and isinstance(
        config["rules"]["rule_1"], RuleConfig
    )


def test_rule_based_profiler_config_successfully_loads_with_optional_args():
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "variables": {"foo": "bar"},
        "rules": {
            "rule_1": {
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "name": "my_parameter"}
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "ExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
            },
        },
    }
    schema = RuleBasedProfilerConfigSchema()
    config = schema.load(data)
    assert isinstance(config, dict)
    assert data["variables"] == config["variables"]


def test_rule_based_profiler_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = RuleBasedProfilerConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert all(
        f"'{attr}': ['Missing data for required field.']" in str(e.value)
        for attr in (
            "name",
            "config_version",
            "rules",
        )
    )


def test_rule_based_profiler_from_commented_map():
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "variables": {"foo": "bar"},
        "rules": {
            "rule_1": {
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "name": "my_parameter"}
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "ExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
            },
        },
    }
    commented_map = CommentedMap(data)
    config = RuleBasedProfilerConfig.from_commented_map(commented_map)
    assert all(hasattr(config, k) for k in data)


def test_resolve_config_using_acceptable_arguments(
    profiler_with_placeholder_args: RuleBasedProfiler,
) -> None:
    old_config: RuleBasedProfilerConfig = profiler_with_placeholder_args.config

    # Roundtrip through schema validation to add/or restore any missing fields.
    old_deserialized_config: dict = ruleBasedProfilerConfigSchema.load(
        old_config.to_json_dict()
    )
    old_deserialized_config.pop("class_name")
    old_deserialized_config.pop("module_name")

    old_config = RuleBasedProfilerConfig(**old_deserialized_config)

    # Brand new config is created but existing attributes are unchanged
    new_config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args,
        )
    )

    # Roundtrip through schema validation to add/or restore any missing fields.
    # new_deserialized_config: dict = ruleBasedProfilerConfigSchema.load(new_config.to_json_dict())
    new_deserialized_config: dict = new_config.to_json_dict()
    new_deserialized_config.pop("class_name")
    new_deserialized_config.pop("module_name")

    new_config = RuleBasedProfilerConfig(**new_deserialized_config)

    assert id(old_config) != id(new_config)
    assert all(
        old_config[attr] == new_config[attr] for attr in ("config_version", "name")
    )


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides(
    profiler_with_placeholder_args: RuleBasedProfiler,
) -> None:
    runtime_override_rule_name: str = "my_runtime_override_rule"
    assert all(
        rule.name != runtime_override_rule_name
        for rule in profiler_with_placeholder_args.rules
    )

    runtime_override_rule: dict = {
        "domain_builder": {
            "class_name": "TableDomainBuilder",
            "module_name": "great_expectations.rule_based_profiler.domain_builder",
        },
        "parameter_builders": [
            {
                "class_name": "MetricMultiBatchParameterBuilder",
                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                "metric_name": "my_other_metric",
                "name": "my_additional_parameter",
            },
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {
                    "details": {
                        "note": "Here's another rule",
                    },
                },
            },
        ],
    }

    runtime_override_rules: Dict[str, dict] = {
        runtime_override_rule_name: runtime_override_rule,
    }

    config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args,
            rules=runtime_override_rules,
        )
    )

    assert len(config.rules) == 2 and runtime_override_rule_name in config.rules


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides_with_batch_requests(
    profiler_with_placeholder_args: RuleBasedProfiler,
) -> None:
    runtime_override_rule: dict = {
        "domain_builder": {
            "class_name": "TableDomainBuilder",
            "module_name": "great_expectations.rule_based_profiler.domain_builder",
        },
        "parameter_builders": [
            {
                "class_name": "MetricMultiBatchParameterBuilder",
                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                "metric_name": "my_other_metric",
                "name": "my_additional_parameter",
            },
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {
                    "details": {
                        "note": "Here's another rule",
                    },
                },
            },
        ],
    }

    runtime_override_rule_name: str = "rule_with_batch_request"
    runtime_override_rules: Dict[str, dict] = {
        runtime_override_rule_name: runtime_override_rule
    }

    config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args,
            rules=runtime_override_rules,
        )
    )

    domain_builder: dict = config.rules[runtime_override_rule_name]["domain_builder"]

    assert domain_builder == {
        "class_name": "TableDomainBuilder",
        "module_name": "great_expectations.rule_based_profiler.domain_builder.table_domain_builder",
    }
