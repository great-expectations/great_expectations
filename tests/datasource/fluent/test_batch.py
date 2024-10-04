from __future__ import annotations

import pathlib
from typing import Tuple

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch, Datasource
from great_expectations.expectations.expectation import Expectation

DATASOURCE_NAME = "my_pandas"
ASSET_NAME = "my_csv"


@pytest.fixture
def pandas_setup(csv_path: pathlib.Path) -> Tuple[AbstractDataContext, Batch]:
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(DATASOURCE_NAME)
    filepath = (
        csv_path
        / "ten_trips_from_each_month"
        / "yellow_tripdata_sample_10_trips_from_each_month.csv"
    )
    asset = source.add_csv_asset(ASSET_NAME, filepath_or_buffer=filepath)
    batch = asset.get_batch(asset.build_batch_request())
    return context, batch


@pytest.mark.filesystem
def test_batch_validate_expectation(pandas_setup: Tuple[AbstractDataContext, Batch]):
    _, batch = pandas_setup

    # Make Expectation
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        column="vendor_id",
        mostly=0.95,  # type: ignore[arg-type] # TODO: Fix in CORE-412
    )
    # Validate
    result = batch.validate(expectation)
    # Asserts on result
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_suite(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup

    # Make Expectation Suite
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(
            column="vendor_id",
            mostly=0.95,  # type: ignore[arg-type] # TODO: Fix in CORE-412
        )
    )
    # Validate
    result = batch.validate(suite)
    # Asserts on result
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_with_expectation_params(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    _, batch = pandas_setup

    expectation = gx.expectations.ExpectColumnMaxToBeBetween(
        column="passenger_count",
        min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
        max_value={"$PARAMETER": "expect_passenger_max_to_be_below"},
    )
    result = batch.validate(
        expectation,
        expectation_parameters={
            "expect_passenger_max_to_be_above": 1,
            "expect_passenger_max_to_be_below": 10,
        },
    )
    # Asserts on result
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_suite_with_expectation_params(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup

    # Make Expectation Suite
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    suite.add_expectation(
        gx.expectations.ExpectColumnMaxToBeBetween(
            column="passenger_count",
            min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
            max_value={"$PARAMETER": "expect_passenger_max_to_be_below"},
        )
    )
    # Validate
    result = batch.validate(
        suite,
        expectation_parameters={
            "expect_passenger_max_to_be_above": 1,
            "expect_passenger_max_to_be_below": 10,
        },
    )
    # Asserts on result
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_with_updated_expectation(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    _, batch = pandas_setup

    # Make Expectation
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        column="vendor_id",
    )
    # Validate
    result = batch.validate(expectation)
    # Asserts on result
    assert result.success is False
    # Update expectation and validate
    expectation.mostly = 0.95  # type: ignore[assignment] # TODO: Fix in CORE-412
    result = batch.validate(expectation)
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_suite_with_updated_expectation(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup

    # Make Expectation Suite
    suite = context.suites.add(ExpectationSuite(name="my_suite"))
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"))
    # Validate
    result = batch.validate(suite)
    # Asserts on result
    assert result.success is False
    # Update suite and validate
    assert len(suite.expectations) == 1

    expectation = suite.expectations[0]
    assert isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    expectation.mostly = 0.95  # type: ignore[assignment] # TODO: Fix in CORE-412

    expectation.save()
    assert isinstance(suite.expectations[0], gxe.ExpectColumnValuesToNotBeNull)
    assert suite.expectations[0].mostly == 0.95

    result = batch.validate(suite)
    assert result.success is True


class TestBatchValidateExpectation:
    @pytest.fixture
    def expectation(self) -> Expectation:
        return gxe.ExpectColumnValuesToNotBeNull(column="vendor_id", mostly=0.95)  # type: ignore[arg-type] # TODO: Fix in CORE-412

    @pytest.mark.filesystem
    def test_boolean_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        expectation: Expectation,
    ):
        _, batch = pandas_setup
        result = batch.validate(expectation, result_format="BOOLEAN_ONLY")

        assert result.success is True
        assert len(result.result) == 0

    @pytest.mark.filesystem
    def test_summary_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        expectation: Expectation,
    ):
        _, batch = pandas_setup
        summary_result = batch.validate(expectation, result_format="SUMMARY")

        assert summary_result.success is True
        assert len(summary_result.result) > 0

    @pytest.mark.filesystem
    def test_complete_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        expectation: Expectation,
    ):
        _, batch = pandas_setup
        result = batch.validate(expectation, result_format="COMPLETE")

        assert result.success is True
        assert "unexpected_index_list" in result.result


class TestBatchValidateExpectationSuite:
    @pytest.fixture
    def suite(self) -> ExpectationSuite:
        return gx.ExpectationSuite(
            name="my-suite",
            expectations=[gxe.ExpectColumnValuesToNotBeNull(column="vendor_id", mostly=0.95)],  # type: ignore[arg-type] # TODO: Fix in CORE-412
        )

    @pytest.mark.filesystem
    def test_boolean_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        suite: ExpectationSuite,
    ):
        _, batch = pandas_setup
        result = batch.validate(suite, result_format="BOOLEAN_ONLY")

        assert result.success is True
        assert len(result.results[0].result) == 0

    @pytest.mark.filesystem
    def test_summary_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        suite: ExpectationSuite,
    ):
        _, batch = pandas_setup
        summary_result = batch.validate(suite, result_format="SUMMARY")

        assert summary_result.success is True
        assert len(summary_result.results[0].result) > 0

    @pytest.mark.filesystem
    def test_complete_validation_result(
        self,
        pandas_setup: Tuple[AbstractDataContext, Batch],
        suite: ExpectationSuite,
    ):
        _, batch = pandas_setup
        result = batch.validate(suite, result_format="COMPLETE")

        assert result.success is True
        assert "unexpected_index_list" in result.results[0].result


@pytest.mark.filesystem
def test_batch_validate_expectation_does_not_persist_a_batch_definition(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup
    datasource = context.data_sources.get(DATASOURCE_NAME)
    assert isinstance(datasource, Datasource)
    asset = datasource.get_asset(ASSET_NAME)

    expectation = gxe.ExpectColumnValuesToNotBeNull(
        column="vendor_id",
        mostly=0.95,  # type: ignore[arg-type] # TODO: Fix in CORE-412
    )
    result = batch.validate(expectation)

    assert result.success
    assert len(asset.batch_definitions) == 0


@pytest.mark.filesystem
def test_batch_validate_expectation_suite_does_not_persist_a_batch_definition(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup
    datasource = context.data_sources.get(DATASOURCE_NAME)
    assert isinstance(datasource, Datasource)
    asset = datasource.get_asset(ASSET_NAME)

    suite = ExpectationSuite(
        "suite",
        expectations=[
            gxe.ExpectColumnValuesToNotBeNull(
                column="vendor_id",
                mostly=0.95,  # type: ignore[arg-type] # TODO: Fix in CORE-412
            )
        ],
    )
    result = batch.validate(suite)

    assert result.success
    assert len(asset.batch_definitions) == 0
