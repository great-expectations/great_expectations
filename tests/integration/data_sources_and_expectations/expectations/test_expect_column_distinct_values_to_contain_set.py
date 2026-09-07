from datetime import datetime

import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
)

COL_NAME = "my_col"

ONES_AND_TWOS = pd.DataFrame({COL_NAME: [1, 2, 2, 2]})


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=ONES_AND_TWOS)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": None,
        "missing_count": 0,
        "partial_missing_list": [],
    }


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({COL_NAME: ["foo", "bar"]}),
)
def test_strings(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=["foo"])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    data=pd.DataFrame({COL_NAME: [datetime(2024, 11, 19).date(), datetime(2024, 11, 20).date()]}),  # noqa: DTZ001 # FIXME CoP
)
def test_dates(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(
        column=COL_NAME,
        value_set=[datetime(2024, 11, 19).date()],  # noqa: DTZ001 # FIXME CoP
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    data=pd.DataFrame({COL_NAME: [datetime(2024, 11, 19).date(), datetime(2024, 11, 20).date()]}),  # noqa: DTZ001 # FIXME CoP
)
def test_dates_with_str_value_set(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(
        column=COL_NAME,
        value_set=[str(datetime(2024, 11, 19).date())],  # noqa: DTZ001 # FIXME CoP
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: [1, 2, None]})
)
def test_ignores_nulls(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: [1, 2, None]})
)
def test_data_is_superset(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ONES_AND_TWOS
)
def test_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2, 3])
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ONES_AND_TWOS
)
def test_boolean_only_result_format_excludes_fields(batch_for_datasource: Batch) -> None:
    """BOOLEAN_ONLY result format should not include missing_count or partial_missing_list."""
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2, 3])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.BOOLEAN_ONLY)
    assert not result.success
    assert result.result == {}


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ONES_AND_TWOS
)
def test_summary_result_format_includes_fields(batch_for_datasource: Batch) -> None:
    """SUMMARY result format should include missing_count and partial_missing_list."""
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2, 3])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.SUMMARY)
    assert not result.success
    assert "missing_count" in result.result
    assert "partial_missing_list" in result.result
    assert result.result["missing_count"] == 1
    assert result.result["partial_missing_list"] == [3]


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ONES_AND_TWOS
)
def test_partial_unexpected_count_zero_excludes_partial_lists(batch_for_datasource: Batch) -> None:
    """Setting partial_unexpected_count=0 should exclude partial_missing_list but keep count."""
    expectation = gxe.ExpectColumnDistinctValuesToContainSet(column=COL_NAME, value_set=[1, 2, 3])
    result = batch_for_datasource.validate(
        expectation,
        result_format={"result_format": "SUMMARY", "partial_unexpected_count": 0},
    )
    assert not result.success
    assert "missing_count" in result.result
    assert "partial_missing_list" not in result.result
