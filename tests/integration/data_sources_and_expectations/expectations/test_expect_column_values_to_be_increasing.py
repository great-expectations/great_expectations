import datetime as dt
from typing import Sequence

import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    PandasDataFrameDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig

COLUMN = "event_at"

# column_values.increasing and column_values.decreasing register a pandas and a Spark
# provider only, and Spark's parallel read gives these row-order-dependent metrics no
# ordering guarantee, so pandas is the engine these cases can assert against.
DATA_SOURCES: Sequence[DataSourceTestConfig] = [PandasDataFrameDatasourceTestConfig()]

ASCENDING_DATETIMES = pd.DataFrame(
    {COLUMN: pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])}
)
DESCENDING_DATETIMES = pd.DataFrame(
    {COLUMN: pd.to_datetime(["2024-01-03", "2024-01-02", "2024-01-01"])}
)
# The middle row goes backwards, so it is the one value that breaks the order.
ASCENDING_DATETIMES_WITH_ONE_STEP_BACK = pd.DataFrame(
    {COLUMN: pd.to_datetime(["2024-01-01", "2024-01-03", "2024-01-02"])}
)
# A repeated timestamp is in order for the default comparison and out of order for
# strictly=True.
ASCENDING_DATETIMES_WITH_A_TIE = pd.DataFrame(
    {COLUMN: pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02"])}
)
DESCENDING_DATETIMES_WITH_A_TIE = pd.DataFrame(
    {COLUMN: pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-01"])}
)
# Sub-day steps in a timezone-aware column, so the comparison sees hours, not just dates.
ASCENDING_TZ_AWARE_DATETIMES = pd.DataFrame(
    {
        COLUMN: pd.to_datetime(
            ["2024-01-01 09:00", "2024-01-01 10:00", "2024-01-01 11:00"]
        ).tz_localize("America/New_York")
    }
)
# datetime.date values, as read from a DATE column, are held in an object column. pandas
# infers that column's diff as a timedelta64 series, which these cases depend on.
ASCENDING_DATES = pd.DataFrame(
    {COLUMN: pd.Series([dt.date(2024, 1, 1), dt.date(2024, 1, 2), dt.date(2024, 1, 3)])}
)
ASCENDING_DATES_WITH_ONE_STEP_BACK = pd.DataFrame(
    {COLUMN: pd.Series([dt.date(2024, 1, 1), dt.date(2024, 1, 3), dt.date(2024, 1, 2)])}
)


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATETIMES,
)
def test_increasing_datetime_column_passes(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN))

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATETIMES,
)
def test_strictly_increasing_datetime_column_passes(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN, strictly=True)
    )

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATETIMES_WITH_ONE_STEP_BACK,
)
def test_datetime_column_that_goes_backwards_fails(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN),
        result_format=ResultFormat.COMPLETE,
    )

    assert not result.success
    assert result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATETIMES_WITH_A_TIE,
)
def test_repeated_datetime_fails_only_when_strictly_increasing(
    batch_for_datasource: Batch,
) -> None:
    assert batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN)
    ).success

    strict_result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN, strictly=True),
        result_format=ResultFormat.COMPLETE,
    )

    assert not strict_result.success
    assert strict_result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=DESCENDING_DATETIMES,
)
def test_decreasing_datetime_column_passes(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN))

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=DESCENDING_DATETIMES,
)
def test_strictly_decreasing_datetime_column_passes(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN, strictly=True)
    )

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATETIMES,
)
def test_ascending_datetime_column_is_not_decreasing(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN),
        result_format=ResultFormat.COMPLETE,
    )

    assert not result.success
    assert result.result["unexpected_count"] == 2


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=DESCENDING_DATETIMES_WITH_A_TIE,
)
def test_repeated_datetime_fails_only_when_strictly_decreasing(
    batch_for_datasource: Batch,
) -> None:
    assert batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN)
    ).success

    strict_result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN, strictly=True),
        result_format=ResultFormat.COMPLETE,
    )

    assert not strict_result.success
    assert strict_result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_TZ_AWARE_DATETIMES,
)
def test_strictly_increasing_tz_aware_datetime_column_passes(
    batch_for_datasource: Batch,
) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN, strictly=True)
    )

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_TZ_AWARE_DATETIMES,
)
def test_ascending_tz_aware_datetime_column_is_not_decreasing(
    batch_for_datasource: Batch,
) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN),
        result_format=ResultFormat.COMPLETE,
    )

    assert not result.success
    assert result.result["unexpected_count"] == 2


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATES,
)
def test_increasing_date_column_passes(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN))

    assert result.success
    assert result.result["unexpected_count"] == 0


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATES_WITH_ONE_STEP_BACK,
)
def test_date_column_that_goes_backwards_fails(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeIncreasing(column=COLUMN),
        result_format=ResultFormat.COMPLETE,
    )

    assert not result.success
    assert result.result["unexpected_count"] == 1


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES,
    data=ASCENDING_DATES,
)
def test_ascending_date_column_is_not_decreasing(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToBeDecreasing(column=COLUMN),
        result_format=ResultFormat.COMPLETE,
    )

    assert not result.success
    assert result.result["unexpected_count"] == 2
