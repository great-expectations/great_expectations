from __future__ import annotations

from copy import copy
from pprint import pformat as pf
from unittest import mock

import pytest

import great_expectations.expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.partitioners import PartitionerColumnValue
from great_expectations.core.result_format import ResultFormat
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.interfaces import DataAsset, Datasource
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
    datasource = fds_data_context.data_sources.get(fds_data_context_datasource_name)
    assert isinstance(datasource, Datasource)
    return datasource.get_asset("trip_asset")


@pytest.fixture
def fds_data_asset_with_event_type_partitioner(
    fds_data_context: AbstractDataContext,
    fds_data_context_datasource_name: str,
) -> DataAsset:
    datasource = fds_data_context.data_sources.get(fds_data_context_datasource_name)
    assert isinstance(datasource, Datasource)
    return datasource.get_asset("trip_asset_partition_by_event_type")


@pytest.fixture
def batch_definition(
    fds_data_asset: DataAsset,
) -> BatchDefinition:
    batch_definition = BatchDefinition[None](name="test_batch_definition")
    batch_definition.set_data_asset(fds_data_asset)
    return batch_definition


@pytest.fixture
def batch_definition_with_event_type_partitioner(
    fds_data_asset_with_event_type_partitioner: DataAsset,
) -> BatchDefinition:
    partitioner = PartitionerColumnValue(column_name="event_type")
    batch_definition = BatchDefinition(name="test_batch_definition", partitioner=partitioner)
    batch_definition.set_data_asset(fds_data_asset_with_event_type_partitioner)
    return batch_definition


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
def test_v1_validator_doesnt_mutate_result_format(
    validator: Validator, expectation_suite: ExpectationSuite
):
    """This test verifies a bugfix where the legacy Validator mutates a ResultFormat
    dict provided by the user.
    """
    result_format_dict = {
        "result_format": "COMPLETE",
    }
    backup_result_format_dict = copy(result_format_dict)
    validator.result_format = result_format_dict
    validator.validate_expectation_suite(expectation_suite=expectation_suite)
    assert result_format_dict == backup_result_format_dict


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
    batch_definition_with_event_type_partitioner: BatchDefinition,
):
    desired_event_type = "start"
    validator = Validator(
        batch_definition=batch_definition_with_event_type_partitioner,
        batch_parameters={"event_type": desired_event_type},
    )

    result = validator.validate_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="event_type",
            value_set=[desired_event_type],
        )
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


@pytest.mark.unit
def test_non_cloud_validate_does_not_render_results(
    validator: Validator,
    empty_data_context: AbstractDataContext,
):
    suite = empty_data_context.suites.add(
        ExpectationSuite(
            name="test_suite",
            expectations=[
                gxe.ExpectColumnValuesToBeInSet(
                    column="event_type",
                    value_set=["start"],
                )
            ],
        )
    )
    result = validator.validate_expectation_suite(suite)

    assert len(result.results) == 1
    assert not result.results[0].rendered_content


@mock.patch(
    "great_expectations.data_context.data_context.context_factory.project_manager.is_using_cloud",
)
@pytest.mark.unit
def test_cloud_validate_renders_results_when_appropriate(
    mock_is_using_cloud,
    validator: Validator,
    empty_data_context: AbstractDataContext,
):
    mock_is_using_cloud.return_value = True
    suite = empty_data_context.suites.add(
        ExpectationSuite(
            name="test_suite",
            expectations=[
                gxe.ExpectColumnValuesToBeInSet(
                    column="event_type",
                    value_set=["start"],
                )
            ],
        )
    )
    result = validator.validate_expectation_suite(suite)

    assert len(result.results) == 1
    assert result.results[0].rendered_content
