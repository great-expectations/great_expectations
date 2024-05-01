from __future__ import annotations

from pprint import pformat as pf

import pytest

import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.result_format import ResultFormat
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
from great_expectations.datasource.fluent.sql_datasource import _SQLAsset
from great_expectations.expectations.expectation import Expectation
from great_expectations.validator.v1_validator import Validator


@pytest.fixture
def failing_expectation() -> Expectation:
    return gxe.ExpectColumnValuesToBeInSet(
        column="event_type",
        value_set=["start", "stop"],
    )


@pytest.fixture
def passing_expectation() -> Expectation:
    return gxe.ExpectColumnValuesToBeBetween(
        column="id",
        min_value=-1,
        max_value=1000000,
    )


@pytest.fixture
def expectation_suite(
    failing_expectation: Expectation, passing_expectation: Expectation
) -> ExpectationSuite:
    suite = ExpectationSuite("test_suite")
    suite.add_expectation_configuration(failing_expectation.configuration)
    suite.add_expectation_configuration(passing_expectation.configuration)
    return suite


@pytest.fixture
def fds_data_asset(
    fds_data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
) -> DataAsset:
    datasource = fds_data_context.get_datasource(fds_data_context_datasource_name)
    assert isinstance(datasource, Datasource)
    return datasource.get_asset("trip_asset")


@pytest.fixture
def fds_data_asset_with_event_type_partitioner(
    fds_data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
) -> DataAsset:
    datasource = fds_data_context.get_datasource(fds_data_context_datasource_name)
    assert isinstance(datasource, Datasource)
    asset = datasource.get_asset("trip_asset_partition_by_event_type")
    assert isinstance(asset, _SQLAsset)
    return asset


@pytest.fixture
def batch_definition(
    fds_data_asset: _SQLAsset,
) -> BatchDefinition:
    return fds_data_asset.add_batch_definition_whole_table(name="test_batch_definition")


@pytest.fixture
def batch_definition_with_yearly_partitioner(
    fds_data_asset: _SQLAsset,
) -> BatchDefinition:
    return fds_data_asset.add_batch_definition_daily(name="test_batch_definition", column="date")


@pytest.fixture
def validator(
    fds_data_context: AbstractDataContext, batch_definition: BatchDefinition
) -> Validator:
    return Validator(
        batch_definition=batch_definition,
        batch_parameters=None,
        result_format=ResultFormat.SUMMARY,
    )


@pytest.mark.unit
def test_result_format_boolean_only(validator: Validator, failing_expectation: Expectation):
    validator.result_format = ResultFormat.BOOLEAN_ONLY
    result = validator.validate_expectation(failing_expectation)

    assert not result.success
    assert result.result == {}


@pytest.mark.unit
def test_result_format_basic(validator: Validator, failing_expectation: Expectation):
    validator.result_format = ResultFormat.BASIC
    result = validator.validate_expectation(failing_expectation)

    assert not result.success

    assert "partial_unexpected_list" in result.result
    assert "partial_unexpected_counts" not in result.result
    assert "unexpected_list" not in result.result


@pytest.mark.unit
def test_result_format_summary(validator: Validator, failing_expectation: Expectation):
    validator.result_format = ResultFormat.SUMMARY
    result = validator.validate_expectation(failing_expectation)

    assert not result.success

    assert "partial_unexpected_list" in result.result
    assert "partial_unexpected_counts" in result.result
    assert "unexpected_list" not in result.result


@pytest.mark.unit
def test_result_format_complete(validator: Validator, failing_expectation: Expectation):
    validator.result_format = ResultFormat.COMPLETE
    result = validator.validate_expectation(failing_expectation)

    assert not result.success

    assert "partial_unexpected_list" in result.result
    assert "partial_unexpected_counts" in result.result
    assert "unexpected_list" in result.result


@pytest.mark.unit
def test_validate_expectation_success(validator: Validator, passing_expectation: Expectation):
    result = validator.validate_expectation(passing_expectation)

    assert result.success


@pytest.mark.unit
def test_validate_expectation_failure(validator: Validator, failing_expectation: Expectation):
    result = validator.validate_expectation(failing_expectation)

    assert not result.success


@pytest.mark.unit
def test_validate_expectation_with_batch_asset_options(
    fds_data_context: AbstractDataContext,
    batch_definition_with_yearly_partitioner,
):
    YEAR = 2020
    MONTH = 1
    DAY = 1
    EXPECTED_ROW_COUNT = 4
    validator = Validator(
        batch_definition=batch_definition_with_yearly_partitioner,
        batch_parameters={"year": YEAR, "month": MONTH, "day": DAY},
    )

    result = validator.validate_expectation(
        gxe.ExpectTableRowCountToEqual(value=EXPECTED_ROW_COUNT)
    )
    print(f"Result dict ->\n{pf(result)}")
    assert result.success


@pytest.mark.unit
def test_validate_expectation_suite(validator: Validator, expectation_suite: ExpectationSuite):
    result = validator.validate_expectation_suite(expectation_suite)

    assert not result.success
    assert not result.results[0].success
    assert result.results[1].success
    assert result.statistics == {
        "evaluated_expectations": 2,
        "successful_expectations": 1,
        "unsuccessful_expectations": 1,
        "success_percent": 50.0,
    }


@pytest.mark.parametrize(
    ["parameter", "expected"],
    [
        (["start", "stop", "continue"], True),
        (["start", "stop"], False),
    ],
)
@pytest.mark.unit
def test_validate_expectation_suite_suite_parameters(
    validator: Validator,
    parameter: list[str],
    expected: bool,
):
    suite = ExpectationSuite("test_suite")
    expectation = gxe.ExpectColumnValuesToBeInSet(
        column="event_type",
        value_set={"$PARAMETER": "my_parameter"},
    )
    suite.add_expectation_configuration(expectation.configuration)
    result = validator.validate_expectation_suite(suite, {"my_parameter": parameter})

    assert result.success == expected
