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
)


def test_domain_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "DomainBuilder",
    }
    schema = DomainBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, DomainBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


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

    assert "'class_name': ['Missing data for required field.']" in str(e.value)
    assert "'parameter_name': ['Missing data for required field.']" in str(e.value)


def test_expectation_configuration_builder_config_successfully_loads_with_required_args():
    data = {
        "class_name": "ExpectationConfigurationBuilder",
        "expectation_type": "expect_column_pair_values_A_to_be_greater_than_B",
    }
    schema = ExpectationConfigurationBuilderConfigSchema()
    config = schema.load(data)
    assert isinstance(config, ExpectationConfigurationBuilderConfig)
    assert all(getattr(config, k) == v for k, v in data.items())


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

    assert "'class_name': ['Missing data for required field.']" in str(e.value)
    assert "'expectation_type': ['Missing data for required field.']" in str(e.value)
