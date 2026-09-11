import dataclasses
import math
from typing import List, Mapping, Type

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    DataSourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)

NUM_COL = "all_numbers"
ALL_THE_SAME = "all_zeros"
MISSING_COL = "one_missing"

DATA = pd.DataFrame(
    {
        NUM_COL: [1, 1, 3],
        ALL_THE_SAME: [1, 1, 1],
        MISSING_COL: [None, -1, 1],
    },
    dtype="object",
)


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete_results(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, min_value=1.15, max_value=1.5)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {"observed_value": pytest.approx(1.1547005383792517)}


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=0, max_value=0),
            id="all_the_same",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=MISSING_COL, min_value=1.414, max_value=1.415),
            id="missing_values",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, min_value=1),
            id="no_max",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL, max_value=1.5),
            id="no_min",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(
                column=NUM_COL, min_value=1, max_value=1.5, strict_min=True, strict_max=True
            ),
            id="strict_bounds",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=NUM_COL),
            id="vacuous_truth",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnStdevToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=1, max_value=2),
            id="bad_range",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, min_value=0, strict_min=True),
            id="strict_min",
        ),
        pytest.param(
            gxe.ExpectColumnStdevToBeBetween(column=ALL_THE_SAME, max_value=0, strict_max=True),
            id="strict_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnStdevToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_strict_min_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_stdev_to_be_between"
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column=NUM_COL,
        min_value=1,
        max_value=1.5,
        strict_min={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@pytest.mark.parametrize(
    "suite_param_value,expected_result",
    [
        pytest.param(True, True, id="success"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success_with_suite_param_strict_max_(
    batch_for_datasource: Batch, suite_param_value: bool, expected_result: bool
) -> None:
    suite_param_key = "test_expect_column_stdev_to_be_between"
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column=NUM_COL,
        min_value=1,
        max_value=1.5,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


SINGLE_VALUE_COL = "single_value"
ALL_NULL_COL = "all_null"
TWO_VALUE_COL = "two_values"

UNDER_TWO_VALUES = pd.DataFrame(
    {
        SINGLE_VALUE_COL: pd.Series([5.0, None], dtype="float64"),
        ALL_NULL_COL: pd.Series([None, None], dtype="float64"),
        TWO_VALUE_COL: pd.Series([5.0, 7.0], dtype="float64"),
    }
)

TWO_VALUE_STDEV = math.sqrt(2.0)
"""The sample standard deviation of TWO_VALUE_COL: mean 6.0, so sqrt(((5-6)**2 + (7-6)**2) / 1)."""

NO_ROWS = pd.DataFrame({SINGLE_VALUE_COL: pd.Series([], dtype="float64")})

try:
    from great_expectations.compatibility.pyspark import types as PYSPARK_TYPES

    UNDER_TWO_VALUES_SPARK_TYPES: Mapping[str, Type] = {
        SINGLE_VALUE_COL: PYSPARK_TYPES.DoubleType,
        ALL_NULL_COL: PYSPARK_TYPES.DoubleType,
        TWO_VALUE_COL: PYSPARK_TYPES.DoubleType,
    }
    NO_ROWS_SPARK_TYPES: Mapping[str, Type] = {SINGLE_VALUE_COL: PYSPARK_TYPES.DoubleType}
except ModuleNotFoundError:
    UNDER_TWO_VALUES_SPARK_TYPES = {}
    NO_ROWS_SPARK_TYPES = {}


def _with_spark_column_types(column_types: Mapping[str, Type]) -> List[DataSourceTestConfig]:
    """ALL_DATA_SOURCES, with the Spark entry given an explicit schema for these frames.

    Spark is in scope for these cases: `stddev_samp` returns NULL for every one of them. What it
    cannot do is *infer* their schemas, because the fixture round-trips the frame through CSV --
    an all-null column has no inferable type and an empty frame has no inferable schema. The
    fixture already takes `column_types` for exactly this, so the Spark entry gets a schema
    rather than being subtracted from the list; subtracting it would read as "Spark was never
    considered" to anything deriving supported-backend claims from this suite.
    """
    return [
        dataclasses.replace(data_source, column_types=column_types)
        if isinstance(data_source, SparkFilesystemCsvDatasourceTestConfig)
        else data_source
        for data_source in ALL_DATA_SOURCES
    ]


UNDER_TWO_VALUES_DATA_SOURCES = _with_spark_column_types(UNDER_TWO_VALUES_SPARK_TYPES)
NO_ROWS_DATA_SOURCES = _with_spark_column_types(NO_ROWS_SPARK_TYPES)


@pytest.mark.parametrize("column", [SINGLE_VALUE_COL, ALL_NULL_COL])
@parameterize_batch_for_data_sources(
    data_source_configs=UNDER_TWO_VALUES_DATA_SOURCES, data=UNDER_TWO_VALUES
)
def test_stdev_of_column_with_under_two_values(batch_for_datasource: Batch, column: str) -> None:
    """A sample standard deviation is undefined for n < 2: report it, do not raise."""
    expectation = gxe.ExpectColumnStdevToBeBetween(column=column, min_value=0, max_value=10)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {"observed_value": None}


@parameterize_batch_for_data_sources(
    data_source_configs=UNDER_TWO_VALUES_DATA_SOURCES, data=UNDER_TWO_VALUES
)
def test_stdev_at_exactly_two_values(batch_for_datasource: Batch) -> None:
    """Two non-null values is the smallest defined sample: compute it, do not report None.

    This is the boundary the guard is written against, from the defined side. Widening it to
    `<= MIN_ROWS_FOR_SAMPLE_STDEV` would make this column report `None`.
    """
    expectation = gxe.ExpectColumnStdevToBeBetween(column=TWO_VALUE_COL, min_value=0, max_value=10)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.result["observed_value"] == pytest.approx(TWO_VALUE_STDEV)


@parameterize_batch_for_data_sources(
    data_source_configs=UNDER_TWO_VALUES_DATA_SOURCES, data=UNDER_TWO_VALUES
)
def test_stdev_undefined_and_defined_in_one_suite(batch_for_datasource: Batch) -> None:
    """A guarded column and a defined one resolve together in a single validation run.

    Metrics for one batch are resolved as a bundle: the guarded branch and a real aggregate end
    up in the same query. Neither result may be disturbed by the other -- an undefined column
    must not make its sibling report `None`, and a defined one must not mask the guard. That is
    the shape a suite covering several columns of one table actually takes.
    """
    suite = ExpectationSuite(
        name="stdev_bundle",
        expectations=[
            gxe.ExpectColumnStdevToBeBetween(column=ALL_NULL_COL, min_value=0, max_value=10),
            gxe.ExpectColumnStdevToBeBetween(column=TWO_VALUE_COL, min_value=0, max_value=10),
        ],
    )
    suite_result = batch_for_datasource.validate(suite, result_format=ResultFormat.COMPLETE)

    results_by_column = {
        result.expectation_config["kwargs"]["column"]: result
        for result in suite_result.results
        if result.expectation_config is not None
    }
    assert set(results_by_column) == {ALL_NULL_COL, TWO_VALUE_COL}

    undefined = results_by_column[ALL_NULL_COL]
    assert not undefined.success
    assert undefined.result["observed_value"] is None

    defined = results_by_column[TWO_VALUE_COL]
    assert defined.success
    assert defined.result["observed_value"] == pytest.approx(TWO_VALUE_STDEV)


@parameterize_batch_for_data_sources(data_source_configs=NO_ROWS_DATA_SOURCES, data=NO_ROWS)
def test_stdev_of_empty_table(batch_for_datasource: Batch) -> None:
    """An empty table has no non-null values, so the standard deviation is undefined."""
    expectation = gxe.ExpectColumnStdevToBeBetween(
        column=SINGLE_VALUE_COL, min_value=0, max_value=10
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert not result.success
    assert result.to_json_dict()["result"] == {"observed_value": None}
