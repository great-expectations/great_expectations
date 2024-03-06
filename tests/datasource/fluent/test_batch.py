from __future__ import annotations

import pathlib
from typing import Tuple

import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch


@pytest.fixture
def pandas_setup(csv_path: pathlib.Path) -> Tuple[AbstractDataContext, Batch]:
    context = gx.get_context()
    source = context.sources.add_pandas("my_pandas")
    filepath = (
        csv_path
        / "ten_trips_from_each_month"
        / "yellow_tripdata_sample_10_trips_from_each_month.csv"
    )
    asset = source.add_csv_asset("my_csv", filepath_or_buffer=filepath)
    batch = asset.get_batch_list_from_batch_request(asset.build_batch_request())[0]
    return context, batch


@pytest.mark.filesystem
def test_batch_validate_expectation(pandas_setup: Tuple[AbstractDataContext, Batch]):
    _, batch = pandas_setup

    # Make Expectation
    expectation = gxe.ExpectColumnValuesToNotBeNull(
        column="vendor_id",
        mostly=0.95,
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
    suite = context.add_expectation_suite("my_suite")
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(
            column="vendor_id",
            mostly=0.95,
        )
    )
    # Validate
    result = batch.validate(suite)
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
    expectation.mostly = 0.95
    result = batch.validate(expectation)
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_expectation_suite_with_updated_expectation(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup

    # Make Expectation Suite
    suite = context.add_expectation_suite("my_suite")
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"))
    # Validate
    result = batch.validate(suite)
    # Asserts on result
    assert result.success is False
    # Update suite and validate
    assert len(suite.expectations) == 1

    expectation = suite.expectations[0]
    assert isinstance(expectation, gxe.ExpectColumnValuesToNotBeNull)
    expectation.mostly = 0.95

    expectation.save()
    assert isinstance(suite.expectations[0], gxe.ExpectColumnValuesToNotBeNull)
    assert suite.expectations[0].mostly == 0.95

    result = batch.validate(suite)
    assert result.success is True


@pytest.mark.filesystem
def test_batch_validate_change_expectation_result_format(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    _, batch = pandas_setup

    # "SUMMARY"" is the default result format
    assert batch.result_format == "SUMMARY"
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="vendor_id", mostly=0.95)
    summary_result = batch.validate(expectation)
    # Summary result succeeds and .result has non-empty summary
    assert summary_result.success is True
    assert len(summary_result.result) > 0
    batch.result_format = "BOOLEAN_ONLY"
    boolean_result = batch.validate(expectation)
    # Boolean result succeeds but .result is empty
    assert boolean_result.success is True
    assert len(boolean_result.result) == 0


@pytest.mark.filesystem
def test_batch_validate_change_expectation_suite_result_format(
    pandas_setup: Tuple[AbstractDataContext, Batch],
):
    context, batch = pandas_setup

    # "SUMMARY"" is the default result format
    assert batch.result_format == "SUMMARY"
    # Make Expectation Suite
    suite = context.add_expectation_suite("my_suite")
    suite.add_expectation(
        gxe.ExpectColumnValuesToNotBeNull(
            column="vendor_id",
            mostly=0.95,
        )
    )
    # Validate
    summary_result = batch.validate(suite)
    # Summary result succeeds and .results[0].result has non-empty summary
    assert summary_result.success is True
    assert len(summary_result.results) == 1
    assert len(summary_result.results[0].result) > 0
    batch.result_format = "BOOLEAN_ONLY"
    boolean_result = batch.validate(suite)
    # Boolean result succeeds but .results[0].result is empty
    assert boolean_result.success is True
    assert len(boolean_result.results) == 1
    assert len(boolean_result.results[0].result) == 0
