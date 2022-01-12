import logging

import pytest

from great_expectations.marshmallow__shade.exceptions import ValidationError
from great_expectations.rule_based_profiler.config.base import (
    DomainBuilderConfig,
    DomainBuilderConfigSchema,
    ExpectationConfigurationBuilderConfig,
    ExpectationConfigurationBuilderConfigSchema,
    ParameterBuilderConfig,
    ParameterBuilderConfigSchema,
    RuleBasedProfilerConfig,
    RuleBasedProfilerConfigSchema,
    RuleConfig,
    RuleConfigSchema,
)


def test_domain_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "DomainBuilder",
    }
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_domain_builder_config_successfully_loads_and_populates_missing_values():
    data = {
        "class_name": "DomainBuilder",
    }
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
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


def test_domain_builder_config_successfully_loads_with_kwargs(caplog):
    data = {"class_name": "DomainBuilder", "author": "Charles Dickens"}
    schema = DomainBuilderConfigSchema()

    with caplog.at_level(logging.INFO):
        config = schema.load(data)

    assert isinstance(config, DomainBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert len(caplog.messages) == 1  # author kwarg


def test_domain_builder_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = DomainBuilderConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert "'class_name': ['Missing data for required field.']" in str(e.value)


def test_parameter_builder_config_successfully_loads_with_required_args():
    data = {"class_name": "ParameterBuilder", "parameter_name": "my_parameter_builder"}
    schema = ParameterBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ParameterBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_parameter_builder_config_successfully_loads_and_populates_missing_values():
    data = {"class_name": "ParameterBuilder", "parameter_name": "my_parameter_builder"}
    schema = ParameterBuilderConfigSchema()
    config = schema.load(data)
    assert (
        config.module_name == "great_expectations.rule_based_profiler.parameter_builder"
    )


def test_parameter_builder_config_successfully_loads_with_optional_args():
    data = {
        "parameter_name": "my_parameter_builder",
        "class_name": "ParameterBuilder",
        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
        "batch_request": {"datasource_name": "my_datasource"},
    }
    schema = ParameterBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ParameterBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def tests_parameter_builder_config_successfully_loads_with_kwargs(caplog):
    data = {
        "parameter_name": "my_parameter_builder",
        "class_name": "ParameterBuilder",
        "created_on": "2022-01-12",
    }
    schema = ParameterBuilderConfigSchema()

    with caplog.at_level(logging.INFO):
        config = schema.load(data)

    assert isinstance(config, ParameterBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert len(caplog.messages) == 1  # created_on kwarg


def test_parameter_builder_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = ParameterBuilderConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert all(
        f"'{attr}': ['Missing data for required field.']" in str(e.value)
        for attr in ("class_name", "parameter_name")
    )


def test_expectation_configuration_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "ExpectationConfigurationBuilder",
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
    }
    schema = ExpectationConfigurationBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ExpectationConfigurationBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


def test_expectation_configuration_builder_config_successfully_loads_and_populates_missing_values():
    data = {
        "class_name": "ExpectationConfigurationBuilder",
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
    }
    schema = ExpectationConfigurationBuilderConfigSchema()
    config = schema.load(data)
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


def tests_expectation_configuration_builder_config_successfully_loads_with_kwargs(
    caplog,
):
    data = {
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
        "class_name": "ExpectationConfigurationBuilder",
        "created_on": "2022-01-12",
        "author": "Charles Dickens",
    }
    schema = ExpectationConfigurationBuilderConfigSchema()

    with caplog.at_level(logging.INFO):
        config = schema.load(data)

    assert isinstance(config, ExpectationConfigurationBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())
    assert len(caplog.messages) == 2  # created_on & author kwargs


def test_expectation_configuration_builder_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = ExpectationConfigurationBuilderConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert all(
        f"'{attr}': ['Missing data for required field.']" in str(e.value)
        for attr in ("class_name", "expectation_type")
    )


def test_rule_config_successfully_loads_with_required_args():
    data = {
        "name": "rule_1",
        "domain_builder": {"class_name": "DomainBuilder"},
        "parameter_builders": [
            {"class_name": "ParameterBuilder", "parameter_name": "my_parameter"}
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


def test_rule_config_successfully_loads_with_kwargs(caplog):
    data = {
        "name": "rule_1",
        "domain_builder": {"class_name": "DomainBuilder"},
        "parameter_builders": [
            {"class_name": "ParameterBuilder", "parameter_name": "my_parameter"}
        ],
        "expectation_configuration_builders": [
            {
                "class_name": "ExpectationConfigurationBuilder",
                "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
            }
        ],
        "created_on": "2021-12-12",
        "author": "Charles Dickens",
    }
    schema = RuleConfigSchema()

    with caplog.at_level(logging.INFO):
        config = schema.load(data)

    assert isinstance(config, RuleConfig)
    assert all(getattr(config, k) == data[k] for k in ("created_on", "author"))
    assert len(caplog.messages) == 2  # created_on & author kwargs


def test_rule_config_unsuccessfully_loads_with_missing_required_fields():
    data = {}
    schema = RuleConfigSchema()

    with pytest.raises(ValidationError) as e:
        schema.load(data)

    assert all(
        f"'{attr}': ['Missing data for required field.']" in str(e.value)
        for attr in (
            "name",
            "domain_builder",
            "parameter_builders",
            "expectation_configuration_builders",
        )
    )


def test_rule_based_profiler_config_successfully_loads_with_required_args():
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "rules": {
            "rule_1": {
                "name": "rule_1",
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "parameter_name": "my_parameter"}
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
    assert isinstance(config, RuleBasedProfilerConfig)
    assert len(config.rules) == 1 and isinstance(config.rules["rule_1"], RuleConfig)


def test_rule_based_profiler_config_successfully_loads_with_optional_args():
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "variables": {"foo": "bar"},
        "rules": {
            "rule_1": {
                "name": "rule_1",
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "parameter_name": "my_parameter"}
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
    assert isinstance(config, RuleBasedProfilerConfig)
    assert data["variables"] == config.variables


def test_rule_based_profiler_config_successfully_loads_with_kwargs(caplog):
    data = {
        "name": "my_RBP",
        "config_version": 1.0,
        "rules": {
            "rule_1": {
                "name": "rule_1",
                "domain_builder": {"class_name": "DomainBuilder"},
                "parameter_builders": [
                    {"class_name": "ParameterBuilder", "parameter_name": "my_parameter"}
                ],
                "expectation_configuration_builders": [
                    {
                        "class_name": "ExpectationConfigurationBuilder",
                        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
                    }
                ],
            },
        },
        "author": "Charles Dickens",
    }
    schema = RuleBasedProfilerConfigSchema()

    with caplog.at_level(logging.INFO):
        config = schema.load(data)

    assert isinstance(config, RuleBasedProfilerConfig)
    assert data["author"] == config.author
    assert len(caplog.messages) == 1  # author kwarg


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
