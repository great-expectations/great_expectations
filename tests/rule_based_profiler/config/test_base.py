from typing import Dict

import pytest
from ruamel.yaml.comments import CommentedMap

from great_expectations.core.batch import BatchRequest
from great_expectations.marshmallow__shade.exceptions import ValidationError
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
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler


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

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert (
        "'expectation_configuration_builders': ['Missing data for required field.']"
        in str(e.value)
    )


def test_rule_based_profiler_config_successfully_loads_with_required_args():
    data = {
        "name": "my_RBP",
        "class_name": "RuleBasedProfiler",
        "module_name": "great_expectations.rule_based_profiler",
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
        "class_name": "RuleBasedProfiler",
        "module_name": "great_expectations.rule_based_profiler",
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
        "class_name": "RuleBasedProfiler",
        "module_name": "great_expectations.rule_based_profiler",
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
    config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args
        )
    )

    old_config: RuleBasedProfilerConfig = profiler_with_placeholder_args.config

    # Brand new config is created but existing attributes are unchanged
    assert id(old_config) != id(config)
    assert all(
        old_config[attr] == config[attr]
        for attr in ("class_name", "config_version", "module_name", "name")
    )


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides(
    profiler_with_placeholder_args: RuleBasedProfiler,
) -> None:
    rule_name: str = "my_new_rule"
    assert all(rule.name != rule_name for rule in profiler_with_placeholder_args.rules)

    rule: dict = {
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
            }
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {"details": {"note": "Here's another rule"}},
            }
        ],
    }

    rules: Dict[str, dict] = {rule_name: rule}

    config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args, rules=rules
        )
    )

    assert len(config.rules) == 2 and rule_name in config.rules


def test_resolve_config_using_acceptable_arguments_with_runtime_overrides_with_batch_requests(
    profiler_with_placeholder_args: RuleBasedProfiler,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
) -> None:
    datasource_name: str = "my_datasource"
    data_connector_name: str = "my_basic_data_connector"
    data_asset_name: str = "my_data_asset"

    batch_request: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
    )

    rule: dict = {
        "domain_builder": {
            "class_name": "TableDomainBuilder",
            "module_name": "great_expectations.rule_based_profiler.domain_builder",
            "batch_request": batch_request,
        },
        "parameter_builders": [
            {
                "class_name": "MetricMultiBatchParameterBuilder",
                "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                "metric_name": "my_other_metric",
                "name": "my_additional_parameter",
            }
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "DefaultExpectationConfigurationBuilder",
                "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                "expectation_type": "expect_column_values_to_be_between",
                "meta": {"details": {"note": "Here's another rule"}},
            }
        ],
    }

    rule_name: str = "rule_with_batch_request"
    rules: Dict[str, dict] = {rule_name: rule}

    config: RuleBasedProfilerConfig = (
        RuleBasedProfilerConfig.resolve_config_using_acceptable_arguments(
            profiler=profiler_with_placeholder_args, rules=rules
        )
    )

    domain_builder: dict = config.rules[rule_name]["domain_builder"]
    converted_batch_request: dict = domain_builder["batch_request"]

    assert converted_batch_request["datasource_name"] == datasource_name
    assert converted_batch_request["data_connector_name"] == data_connector_name
    assert converted_batch_request["data_asset_name"] == data_asset_name
