from datetime import datetime

import pandas as pd
import pytest

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
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=[1, 2])
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "observed_value": None,
        "unexpected_count": 0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "partial_missing_list": [],
    }


@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=pd.DataFrame({COL_NAME: ["foo", "bar"]}),
)
def test_strings(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(
        column=COL_NAME, value_set=["foo", "bar"]
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    data=pd.DataFrame({COL_NAME: [datetime(2024, 11, 19).date(), datetime(2024, 11, 20).date()]}),  # noqa: DTZ001 # FIXME CoP
)
def test_dates(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(
        column=COL_NAME,
        value_set=[datetime(2024, 11, 19).date(), datetime(2024, 11, 20).date()],  # noqa: DTZ001 # FIXME CoP
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    data=pd.DataFrame({COL_NAME: [datetime(2024, 11, 19).date(), datetime(2024, 11, 20).date()]}),  # noqa: DTZ001 # FIXME CoP
)
def test_dates_with_str_value_set(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(
        column=COL_NAME,
        value_set=[str(datetime(2024, 11, 19).date()), str(datetime(2024, 11, 20).date())],  # noqa: DTZ001 # FIXME CoP
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=pd.DataFrame({COL_NAME: [1, 2, None]})
)
def test_ignores_nulls(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=[1, 2])
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize("value_set", [[1], [1, 4], [1, 2, 3]])
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=ONES_AND_TWOS
)
def test_fails_if_data_is_not_equal(batch_for_datasource: Batch, value_set: list[int]) -> None:
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=value_set)
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=pd.DataFrame(
        {
            COL_NAME: pd.to_datetime(
                [datetime(2025, 9, 1), datetime(2025, 9, 2), datetime(2025, 9, 3)]  # noqa: DTZ001 # FIXME CoP
            ),
        }
    ),
)
def test_datetime64_ns_with_str_value_set(batch_for_datasource: Batch) -> None:
    """Test that datetime64[ns] columns work with string-formatted datetime value_set."""
    value_set = [
        d.strftime("%Y-%m-%dT%H:%M:%S")
        for d in pd.date_range(
            start=datetime(2025, 9, 1),  # noqa: DTZ001 # FIXME CoP
            end=datetime(2025, 9, 3),  # noqa: DTZ001 # FIXME CoP
            freq="1D",
        )
    ]
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=value_set)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=pd.DataFrame(
        {
            COL_NAME: pd.to_datetime(
                [datetime(2025, 9, 1), datetime(2025, 9, 2), datetime(2025, 9, 3)]  # noqa: DTZ001 # FIXME CoP
            ),
        }
    ),
)
def test_datetime64_ns_with_datetime_value_set(batch_for_datasource: Batch) -> None:
    """Test that datetime64[ns] columns work with datetime objects in value_set."""
    value_set = [
        datetime(2025, 9, 1),  # noqa: DTZ001 # FIXME CoP
        datetime(2025, 9, 2),  # noqa: DTZ001 # FIXME CoP
        datetime(2025, 9, 3),  # noqa: DTZ001 # FIXME CoP
    ]
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=value_set)
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES,
    data=pd.DataFrame(
        {
            COL_NAME: pd.to_datetime(
                [datetime(2025, 9, 1), datetime(2025, 9, 2), datetime(2025, 9, 3)]  # noqa: DTZ001 # FIXME CoP
            ),
        }
    ),
)
def test_datetime64_ns_with_pd_timestamp_value_set(batch_for_datasource: Batch) -> None:
    """Test that datetime64[ns] columns work with pd.Timestamp objects in value_set."""
    value_set = pd.date_range(
        start=datetime(2025, 9, 1),  # noqa: DTZ001 # FIXME CoP
        end=datetime(2025, 9, 3),  # noqa: DTZ001 # FIXME CoP
        freq="1D",
    ).tolist()
    expectation = gxe.ExpectColumnDistinctValuesToEqualSet(column=COL_NAME, value_set=value_set)
    result = batch_for_datasource.validate(expectation)
    assert result.success
