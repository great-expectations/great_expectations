import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource
from great_expectations.expectations.core.expect_column_values_to_be_between import (
    ExpectColumnValuesToBeBetween,
)
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.validator.v1_validator import ResultFormat, Validator


@pytest.fixture
def failing_expectation() -> Expectation:
    return ExpectColumnValuesToBeInSet(
        ExpectationConfiguration(
            "expect_column_values_to_be_in_set",
            kwargs={
                "column": "event_type",
                "value_set": ["start", "stop"],
            },
        )
    )


@pytest.fixture
def passing_expectation() -> Expectation:
    return ExpectColumnValuesToBeBetween(
        ExpectationConfiguration(
            "expect_column_values_to_be_between",
            kwargs={
                "column": "id",
                "min_value": -1,
                "max_value": 1000000,
            },
        )
    )


@pytest.fixture
def expectation_suite(
    failing_expectation: Expectation, passing_expectation: Expectation
) -> ExpectationSuite:
    suite = ExpectationSuite("test_suite")
    suite.add_expectation(failing_expectation.configuration)
    suite.add_expectation(passing_expectation.configuration)
    return suite


@pytest.fixture
def batch_config(fds_data_context: AbstractDataContext) -> BatchConfig:
    datasource = fds_data_context.get_datasource("sqlite_datasource")
    assert isinstance(datasource, SqliteDatasource)
    return BatchConfig(
        data_asset=datasource.get_asset("trip_asset"),
    )


@pytest.fixture
def batch_config_with_event_type_splitter(
    fds_data_context: AbstractDataContext,
) -> BatchConfig:
    datasource = fds_data_context.get_datasource("sqlite_datasource")
    assert isinstance(datasource, SqliteDatasource)
    return BatchConfig(
        data_asset=datasource.get_asset("trip_asset_split_by_event_type"),
    )


@pytest.fixture
def validator(
    fds_data_context: AbstractDataContext, batch_config: BatchConfig
) -> Validator:
    return Validator(
        context=fds_data_context,
        batch_config=batch_config,
        batch_request_options=None,
        result_format=ResultFormat.SUMMARY,
    )


@pytest.mark.unit
def test_result_format_boolean_only(
    validator: Validator, failing_expectation: Expectation
):
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
def test_validate_expectation_success(
    validator: Validator, passing_expectation: Expectation
):
    result = validator.validate_expectation(passing_expectation)

    assert result.success


@pytest.mark.unit
def test_validate_expectation_failure(
    validator: Validator, failing_expectation: Expectation
):
    result = validator.validate_expectation(failing_expectation)

    assert not result.success


@pytest.mark.unit
def test_validate_expectation_with_batch_asset_options(
    fds_data_context: AbstractDataContext,
    batch_config_with_event_type_splitter: BatchConfig,
):
    desired_event_type = "start"
    validator = Validator(
        context=fds_data_context,
        batch_config=batch_config_with_event_type_splitter,
        batch_request_options={"event_type": desired_event_type},
    )

    result = validator.validate_expectation(
        ExpectColumnValuesToBeInSet(
            ExpectationConfiguration(
                "expect_column_values_to_be_in_set",
                kwargs={
                    "column": "event_type",
                    "value_set": [desired_event_type],
                },
            )
        )
    )

    assert result.success


@pytest.mark.unit
def test_validate_expectation_suite(
    validator: Validator, expectation_suite: ExpectationSuite
):
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
