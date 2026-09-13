from __future__ import annotations

import json

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.batch import BatchMarkers, LegacyBatchDefinition
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResultMeta,
)
from great_expectations.core.id_dict import BatchSpec, IDDict
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metadata_types import FailureSeverity


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description():
    # arrange
    evr = ExpectationValidationResult(
        success=False,
        expectation_config=gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=6,
            notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",  # noqa: E501 # FIXME CoP
        ).configuration,
        result={
            "element_count": 100000,
            "unexpected_count": 1,
            "unexpected_percent": 0.001,
            "partial_unexpected_list": [7.0],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.001,
            "unexpected_percent_nonmissing": 0.001,
            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
            "partial_unexpected_index_list": [48422],
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": "expect_column_values_to_be_between",
            "success": False,
            "kwargs": {"column": "passenger_count", "min_value": 0.0, "max_value": 6.0},
            "result": {
                "element_count": 100000,
                "unexpected_count": 1,
                "unexpected_percent": 0.001,
                "partial_unexpected_list": [7.0],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.001,
                "unexpected_percent_nonmissing": 0.001,
                "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                "partial_unexpected_index_list": [48422],
            },
        },
        indent=4,
    )


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description_with_null_values():
    # It's unclear if an ExpectationValidationResult can ever be valid without an Expectation
    # or a result, but since it's typed that way we test it
    # arrange
    evr = ExpectationValidationResult(
        success=True,
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": None,
            "success": True,
            "kwargs": None,
            "result": {},
        },
        indent=4,
    )


@pytest.mark.unit
def test_expectation_validation_result_describe_returns_expected_description_with_exception():
    # arrange
    evr = ExpectationValidationResult(
        success=False,
        expectation_config=gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=6,
            notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",  # noqa: E501 # FIXME CoP
        ).configuration,
        result={
            "element_count": 100000,
            "unexpected_count": 1,
            "unexpected_percent": 0.001,
            "partial_unexpected_list": [7.0],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.001,
            "unexpected_percent_nonmissing": 0.001,
            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
            "partial_unexpected_index_list": [48422],
        },
        exception_info={
            "raised_exception": True,
            "exception_traceback": "Traceback (most recent call last): something went wrong",
            "exception_message": "Helpful message here",
        },
    )
    # act
    description = evr.describe()
    # assert
    assert description == json.dumps(
        {
            "expectation_type": "expect_column_values_to_be_between",
            "success": False,
            "kwargs": {"column": "passenger_count", "min_value": 0.0, "max_value": 6.0},
            "result": {
                "element_count": 100000,
                "unexpected_count": 1,
                "unexpected_percent": 0.001,
                "partial_unexpected_list": [7.0],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.001,
                "unexpected_percent_nonmissing": 0.001,
                "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                "partial_unexpected_index_list": [48422],
            },
            "exception_info": {
                "raised_exception": True,
                "exception_traceback": "Traceback (most recent call last): something went wrong",
                "exception_message": "Helpful message here",
            },
        },
        indent=4,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "validation_result_url",
    [
        "https://app.greatexpectations.io/organizations/my-org/data-assets/6f6d390b-a52b-41d1-b5c0-a1d57a6b4618/validations/expectation-suites/a0af0eb5-90ab-4219-ab60-482eee0a8b32/results/e77ce5e4-b71b-4f86-9c3b-f82385aab660",
        None,
    ],
)
def test_expectation_suite_validation_result_returns_expected_shape(
    validation_result_url: str | None,
):
    # arrange
    svr = ExpectationSuiteValidationResult(
        success=True,
        statistics={
            "evaluated_expectations": 2,
            "successful_expectations": 2,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
        suite_name="empty_suite",
        results=[
            ExpectationValidationResult(
                meta={},
                success=True,
                exception_info={
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                result={
                    "element_count": 100000,
                    "unexpected_count": 1,
                    "unexpected_percent": 0.001,
                    "partial_unexpected_list": [7.0],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.001,
                    "unexpected_percent_nonmissing": 0.001,
                    "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                    "partial_unexpected_index_list": [48422],
                },
                expectation_config=ExpectationConfiguration(
                    meta={},
                    notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",  # noqa: E501 # FIXME CoP
                    id="9f76d0b5-9d99-4ed9-a269-339b35e60490",
                    kwargs={
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "mostly": 0.95,
                        "column": "passenger_count",
                        "min_value": 0.0,
                        "max_value": 6.0,
                    },
                    type="expect_column_values_to_be_between",
                ),
            ),
            ExpectationValidationResult(
                meta={},
                success=True,
                exception_info={
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                result={
                    "element_count": 100000,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "partial_unexpected_counts": [],
                    "partial_unexpected_index_list": [],
                },
                expectation_config=ExpectationConfiguration(
                    meta={},
                    id="19c0e80c-d676-4b01-a4a3-2a568552d368",
                    kwargs={
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "column": "trip_distance",
                    },
                    type="expect_column_values_to_not_be_null",
                ),
            ),
        ],
        result_url=validation_result_url,
    )
    # act
    description = svr.describe()
    # assert
    assert description == json.dumps(
        {
            "success": True,
            "statistics": {
                "evaluated_expectations": 2,
                "successful_expectations": 2,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "success": True,
                    "kwargs": {
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "mostly": 0.95,
                        "column": "passenger_count",
                        "min_value": 0.0,
                        "max_value": 6.0,
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 1,
                        "unexpected_percent": 0.001,
                        "partial_unexpected_list": [7.0],
                        "missing_count": 0,
                        "missing_percent": 0.0,
                        "unexpected_percent_total": 0.001,
                        "unexpected_percent_nonmissing": 0.001,
                        "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                        "partial_unexpected_index_list": [48422],
                    },
                },
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": True,
                    "kwargs": {
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                        "column": "trip_distance",
                    },
                    "result": {
                        "element_count": 100000,
                        "unexpected_count": 0,
                        "unexpected_percent": 0.0,
                        "partial_unexpected_list": [],
                        "partial_unexpected_counts": [],
                        "partial_unexpected_index_list": [],
                    },
                },
            ],
            "result_url": validation_result_url,
        },
        indent=4,
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "validation_result_url",
    [
        "https://app.greatexpectations.io/organizations/my-org/data-assets/6f6d390b-a52b-41d1-b5c0-a1d57a6b4618/validations/expectation-suites/a0af0eb5-90ab-4219-ab60-482eee0a8b32/results/e77ce5e4-b71b-4f86-9c3b-f82385aab660",
        None,
    ],
)
def test_expectation_suite_validation_asset_name_access(
    validation_result_url: str | None,
):
    # arrange
    svr = ExpectationSuiteValidationResult(
        meta=ExpectationSuiteValidationResultMeta(
            active_batch_definition=LegacyBatchDefinition(
                batch_identifiers=IDDict({}),
                data_asset_name="taxi_data_1.csv",
                data_connector_name="default_inferred_data_connector_name",
                datasource_name="pandas",
            ),
            batch_markers=BatchMarkers(
                {
                    "ge_load_time": "20220727T154327.630107Z",
                    "pandas_data_fingerprint": "c4f929e6d4fab001fedc9e075bf4b612",
                }
            ),
            batch_spec=BatchSpec({"path": "../data/taxi_data_1.csv"}),
            checkpoint_name="single_validation_checkpoint",
            expectation_suite_name="taxi_suite_1",
            great_expectations_version="0.15.15",
            run_id=RunIdentifier(
                run_name="20220727-114327-my-run-name-template",
                run_time="2022-07-27T11:43:27.625252+00:00",
            ),
            validation_time="20220727T154327.701100Z",
            batch_parameters=None,
            checkpoint_id=None,
            validation_id=None,
        ),
        success=True,
        statistics={
            "evaluated_expectations": 2,
            "successful_expectations": 2,
            "unsuccessful_expectations": 0,
            "success_percent": 100.0,
        },
        suite_name="empty_suite",
        results=[
            ExpectationValidationResult(
                meta={},
                success=True,
                exception_info={
                    "raised_exception": False,
                    "exception_traceback": None,
                    "exception_message": None,
                },
                result={
                    "element_count": 100000,
                    "unexpected_count": 1,
                    "unexpected_percent": 0.001,
                    "partial_unexpected_list": [7.0],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.001,
                    "unexpected_percent_nonmissing": 0.001,
                    "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
                    "partial_unexpected_index_list": [48422],
                },
                expectation_config=ExpectationConfiguration(
                    meta={},
                    notes="Test notes",
                    id="9f76d0b5-9d99-4ed9-a269-339b35e60490",
                    kwargs={
                        "batch_id": "default_pandas_datasource-#ephemeral_pandas_asset",
                    },
                    type="expect_column_values_to_be_between",
                ),
            ),
        ],
        result_url=validation_result_url,
    )

    assert svr.asset_name == "taxi_data_1.csv"


@pytest.mark.unit
def test_render_updates_rendered_content():
    evr = ExpectationValidationResult(
        success=False,
        expectation_config=gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count",
            min_value=0,
            max_value=6,
            notes="Per the TLC data dictionary, this is a driver-submitted value (historically between 0 to 6)",  # noqa: E501 # FIXME CoP
        ).configuration,
        result={
            "element_count": 100000,
            "unexpected_count": 1,
            "unexpected_percent": 0.001,
            "partial_unexpected_list": [7.0],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.001,
            "unexpected_percent_nonmissing": 0.001,
            "partial_unexpected_counts": [{"value": 7.0, "count": 1}],
            "partial_unexpected_index_list": [48422],
        },
    )

    assert evr.rendered_content is None

    evr.render()

    assert evr.rendered_content is not None


@pytest.mark.unit
@pytest.mark.parametrize(
    "severity_enum,expected_string",
    [
        (FailureSeverity.WARNING, "warning"),
        (FailureSeverity.CRITICAL, "critical"),
        (FailureSeverity.INFO, "info"),
    ],
)
def test_expectation_validation_result_with_severity_enum_serializes_properly(
    severity_enum, expected_string
):
    """Test that expectation validation results with severity enums serialize without errors."""

    # Create an expectation config with severity enum at top level
    config = ExpectationConfiguration(
        type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 0, "max_value": 100},
        severity=severity_enum,
    )

    evr = ExpectationValidationResult(
        success=True,
        expectation_config=config,
        result={
            "observed_value": 50,
            "element_count": 1000,
            "missing_count": 0,
            "missing_percent": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )

    # Test that the validation result serializes without errors and severity is correct
    json_dict = evr.to_json_dict()
    # Verify the severity was converted to the expected string value
    assert json_dict["expectation_config"]["severity"] == expected_string


class TestSerialization:
    @pytest.mark.unit
    def test_expectation_validation_results_serializes(self) -> None:
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=gxe.ExpectColumnDistinctValuesToEqualSet(
                column="passenger_count",
                value_set=[1, 2],
            ).configuration,
            result={
                "details": {
                    "observed_value": pd.Series({"a": 1, "b": 2, "c": 4}),
                }
            },
        )

        # Ensure the results are serializable.
        as_dict = evr.describe_dict()
        from_describe_dict = json.dumps(as_dict, indent=4)
        from_describe = evr.describe()

        assert from_describe_dict == from_describe
        assert as_dict["result"]["details"]["observed_value"] == [
            {"index": "a", "value": 1},
            {"index": "b", "value": 2},
            {"index": "c", "value": 4},
        ]

    @pytest.mark.unit
    def test_expectation_suite_validation_results_serializes(self) -> None:
        svr = ExpectationSuiteValidationResult(
            success=True,
            statistics={
                "evaluated_expectations": 2,
                "successful_expectations": 2,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            suite_name="whatever",
            results=[
                ExpectationValidationResult(
                    success=True,
                    expectation_config=gxe.ExpectColumnDistinctValuesToEqualSet(
                        column="passenger_count",
                        value_set=[1, 2],
                    ).configuration,
                    result={
                        "details": {
                            "observed_value": pd.Series({"a": 1, "b": 2, "c": 4}),
                        }
                    },
                )
            ],
        )

        # Ensure the results are serializable.
        as_dict = svr.describe_dict()
        from_describe_dict = json.dumps(as_dict, indent=4)
        from_describe = svr.describe()

        assert from_describe_dict == from_describe
        assert as_dict["expectations"][0]["result"]["details"]["observed_value"] == [
            {"index": "a", "value": 1},
            {"index": "b", "value": 2},
            {"index": "c", "value": 4},
        ]


class TestExpectationValidationResultHash:
    @pytest.mark.unit
    def test_hash_consistency_with_equality(self):
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        result1 = ExpectationValidationResult(
            success=True,
            expectation_config=config1,
            result={"observed_value": 100},
            meta={"test": "value"},
            exception_info={"raised_exception": False},
        )

        result2 = ExpectationValidationResult(
            success=True,
            expectation_config=config2,
            result={"observed_value": 100},
            meta={"test": "value"},
            exception_info={"raised_exception": False},
        )

        assert result1 == result2
        assert hash(result1) == hash(result2)

    @pytest.mark.unit
    def test_hash_different_for_different_success(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        result1 = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result2 = ExpectationValidationResult(
            success=False, expectation_config=config, result={"observed_value": 100}
        )

        assert result1 != result2
        assert hash(result1) != hash(result2)

    @pytest.mark.unit
    def test_hash_different_for_different_results(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        result1 = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result2 = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 200}
        )

        assert result1 != result2
        assert hash(result1) != hash(result2)

    @pytest.mark.unit
    def test_hash_stable_across_runs(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        result = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result={"observed_value": 100},
            meta={"test": "value"},
            exception_info={"raised_exception": False},
        )

        hash1 = hash(result)
        hash2 = hash(result)
        hash3 = hash(result)

        assert hash1 == hash2 == hash3


class TestExpectationSuiteValidationResultHash:
    @pytest.mark.unit
    def test_hash_consistency_with_equality(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        evr = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result1 = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[evr],
            suite_parameters={"param": "value"},
            statistics={"evaluated_expectations": 1},
            meta={"test": "value"},
        )

        result2 = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[evr],
            suite_parameters={"param": "value"},
            statistics={"evaluated_expectations": 1},
            meta={"test": "value"},
        )

        assert result1 == result2
        assert hash(result1) == hash(result2)

    @pytest.mark.unit
    def test_hash_different_for_different_success(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        evr = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result1 = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[evr],
            suite_parameters={"param": "value"},
        )

        result2 = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=False,
            results=[evr],
            suite_parameters={"param": "value"},
        )

        assert result1 != result2
        assert hash(result1) != hash(result2)

    @pytest.mark.unit
    def test_hash_stable_across_runs(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )

        evr = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[evr],
            suite_parameters={"param": "value"},
            statistics={"evaluated_expectations": 1},
            meta={"test": "value"},
        )

        hash1 = hash(result)
        hash2 = hash(result)
        hash3 = hash(result)

        assert hash1 == hash2 == hash3


class TestExpectationProperty:
    @pytest.mark.unit
    def test_returns_typed_expectation_from_config(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "foo"},
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
        )
        expectation = evr.expectation
        assert isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
        assert expectation.column == "foo"

    @pytest.mark.unit
    def test_works_for_unexpected_rows_expectation(self):
        config = ExpectationConfiguration(
            type="unexpected_rows_expectation",
            kwargs={"unexpected_rows_query": "SELECT * FROM {batch} WHERE col > 5"},
        )
        evr = ExpectationValidationResult(
            success=False,
            expectation_config=config,
        )
        expectation = evr.expectation
        assert isinstance(expectation, gxe.UnexpectedRowsExpectation)
        assert expectation.unexpected_rows_query == "SELECT * FROM {batch} WHERE col > 5"


class TestBatchParametersProperty:
    @pytest.mark.unit
    def test_returns_batch_parameters_from_meta(self):
        params = {"year": 2026, "month": 3}
        esvr = ExpectationSuiteValidationResult(
            success=True,
            results=[],
            suite_name="test_suite",
            meta={"batch_parameters": params},
        )
        assert esvr.batch_parameters == params

    @pytest.mark.unit
    def test_returns_none_when_missing(self):
        esvr = ExpectationSuiteValidationResult(
            success=True,
            results=[],
            suite_name="test_suite",
            meta={},
        )
        assert esvr.batch_parameters is None


class TestGetMaxSeverityFailure:
    @pytest.mark.unit
    def test_get_max_severity_failure_no_results(self):
        """Test that None is returned when there are no results."""
        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[],
        )

        assert result.get_max_severity_failure() is None

    @pytest.mark.unit
    def test_get_max_severity_failure_no_failures(self):
        """Test that None is returned when all expectations pass."""
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            severity="critical",
        )

        evr = ExpectationValidationResult(
            success=True, expectation_config=config, result={"observed_value": 100}
        )

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=True,
            results=[evr],
        )

        assert result.get_max_severity_failure() is None

    @pytest.mark.unit
    def test_get_max_severity_failure_multiple_failures(self):
        """Test that the highest severity is returned among multiple failures."""

        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            severity="info",
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_be_between",
            kwargs={
                "column": "test_column",
                "min_value": 0,
                "max_value": 100,
            },
            severity="warning",
        )
        config3 = ExpectationConfiguration(
            type="expect_column_values_to_be_unique",
            kwargs={"column": "test_column"},
            severity="critical",
        )

        evr1 = ExpectationValidationResult(success=False, expectation_config=config1, result={})
        evr2 = ExpectationValidationResult(success=False, expectation_config=config2, result={})
        evr3 = ExpectationValidationResult(success=False, expectation_config=config3, result={})

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=False,
            results=[evr1, evr2, evr3],
        )

        assert result.get_max_severity_failure() == FailureSeverity.CRITICAL

    @pytest.mark.unit
    def test_get_max_severity_failure_mixed_success_failure(self):
        """Test that only failed expectations are considered."""

        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            severity="critical",
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_be_between",
            kwargs={
                "column": "test_column",
                "min_value": 0,
                "max_value": 100,
            },
            severity="warning",
        )

        evr1 = ExpectationValidationResult(success=True, expectation_config=config1, result={})
        evr2 = ExpectationValidationResult(success=False, expectation_config=config2, result={})

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=False,
            results=[evr1, evr2],
        )

        assert result.get_max_severity_failure() == FailureSeverity.WARNING

    @pytest.mark.unit
    def test_failure_severity_enum_semantic_ordering(self):
        """Test that FailureSeverity enum values sort semantically."""
        from great_expectations.expectations.metadata_types import FailureSeverity

        # Test that the enum values sort in semantic order (info < warning < critical)
        # This should NOT depend on lexicographical order of the string values
        severity_values = [FailureSeverity.CRITICAL, FailureSeverity.WARNING, FailureSeverity.INFO]

        # Sort the values - they should now be in the correct semantic order
        sorted_severities = sorted(severity_values)

        # Verify the semantic order: info < warning < critical
        assert sorted_severities[0] == FailureSeverity.INFO, (
            f"Expected INFO first, got {sorted_severities[0]}"
        )
        assert sorted_severities[1] == FailureSeverity.WARNING, (
            f"Expected WARNING second, got {sorted_severities[1]}"
        )
        assert sorted_severities[2] == FailureSeverity.CRITICAL, (
            f"Expected CRITICAL third, got {sorted_severities[2]}"
        )

        # Test individual comparisons - these should work semantically, not lexicographically
        assert FailureSeverity.INFO < FailureSeverity.WARNING, (
            "INFO should be less than WARNING semantically"
        )
        assert FailureSeverity.WARNING < FailureSeverity.CRITICAL, (
            "WARNING should be less than CRITICAL semantically"
        )
        assert FailureSeverity.INFO < FailureSeverity.CRITICAL, (
            "INFO should be less than CRITICAL semantically"
        )

        # Test that the string values can be in any order - the semantic ordering should still work
        # Even though "critical" < "info" < "warning" lexicographically
        assert FailureSeverity.CRITICAL > FailureSeverity.INFO, (
            "CRITICAL should be greater than INFO semantically"
        )
        assert FailureSeverity.CRITICAL > FailureSeverity.WARNING, (
            "CRITICAL should be greater than WARNING semantically"
        )
        assert FailureSeverity.WARNING > FailureSeverity.INFO, (
            "WARNING should be greater than INFO semantically"
        )

    @pytest.mark.unit
    def test_get_max_severity_failure_invalid_severity_skipped(self, caplog):
        """Test that expectations with invalid severity are skipped."""
        import logging

        # Create valid configurations first, then mock returning an invalid severity to
        # work around ValueError that is raised when attempting to set an invalid
        # severity in the constructor
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            severity="critical",  # Start with valid severity
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_be_between",
            kwargs={
                "column": "test_column",
                "min_value": 0,
                "max_value": 100,
            },
            severity="warning",
        )

        # Mock the get method to return invalid severity for testing
        original_get = config1.get

        def mock_get_invalid(key, default=None):
            if key == "severity":
                return "invalid_severity"
            return original_get(key, default)

        config1.get = mock_get_invalid

        evr1 = ExpectationValidationResult(success=False, expectation_config=config1, result={})
        evr2 = ExpectationValidationResult(success=False, expectation_config=config2, result={})

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=False,
            results=[evr1, evr2],
        )

        # Capture log messages BEFORE calling the method
        caplog.set_level(
            logging.ERROR, logger="great_expectations.core.expectation_validation_result"
        )

        # Now call the method that should generate the log
        assert result.get_max_severity_failure() == FailureSeverity.WARNING

        # Verify that an error was logged about invalid severity
        assert any(
            "Invalid severity value 'invalid_severity'" in record.message
            for record in caplog.records
        )
        assert any(
            "expect_column_values_to_not_be_null" in record.message for record in caplog.records
        )

    @pytest.mark.unit
    def test_get_max_severity_failure_all_invalid_severities(self):
        """Test that None is returned when all failures have invalid severity."""

        # Create valid configurations first, then mock returning an invalid severity to
        # work around ValueError that is raised when attempting to set an invalid
        # severity in the constructor
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            severity="critical",
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_be_between",
            kwargs={
                "column": "test_column",
                "min_value": 0,
                "max_value": 100,
            },
            severity="warning",
        )

        evr1 = ExpectationValidationResult(success=False, expectation_config=config1, result={})
        evr2 = ExpectationValidationResult(success=False, expectation_config=config2, result={})

        result = ExpectationSuiteValidationResult(
            suite_name="test_suite",
            success=False,
            results=[evr1, evr2],
        )

        # Mock the get method to return invalid severity for testing
        original_get1 = config1.get

        def mock_get_invalid1(key, default=None):
            if key == "severity":
                return "invalid_severity_1"
            return original_get1(key, default)

        config1.get = mock_get_invalid1

        original_get2 = config2.get

        def mock_get_invalid2(key, default=None):
            if key == "severity":
                return "invalid_severity_2"
            return original_get2(key, default)

        config2.get = mock_get_invalid2

        # Test that the method returns None when all severities are invalid
        assert result.get_max_severity_failure() is None
