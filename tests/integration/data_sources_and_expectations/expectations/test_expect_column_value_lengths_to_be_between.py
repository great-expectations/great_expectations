import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
)

COL_NAME = "my_strings"

DATA = pd.DataFrame({COL_NAME: ["AA", "AAA", None]}, dtype="object")


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_success_complete(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=2, max_value=3)
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=2),
            id="no_max",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, max_value=3),
            id="no_min",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(
                column=COL_NAME, min_value=1, max_value=4, strict_min=True, strict_max=True
            ),
            id="strict_bounds",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToBeBetween
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=0, max_value=1),
            id="range_too_low",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=6, max_value=8),
            id="range_too_high",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=2, strict_min=True),
            id="strict_min",
        ),
        pytest.param(
            gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, max_value=3, strict_max=True),
            id="strict_max",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch, expectation: gxe.ExpectColumnValueLengthsToBeBetween
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
    suite_param_key = "test_expect_column_value_lengths_to_be_between"
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(
        column=COL_NAME,
        min_value=1,
        max_value=4,
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
    suite_param_key = "test_expect_column_value_lengths_to_be_between"
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(
        column=COL_NAME,
        min_value=1,
        max_value=4,
        strict_max={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: suite_param_value}
    )
    assert result.success == expected_result


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueLengthsToBeBetween with pandas."""
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=0, max_value=1)
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # For pandas data sources, unexpected_rows should be directly usable
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)

    # Convert directly to DataFrame for pandas data sources
    unexpected_rows_df = unexpected_rows_data

    # Should contain 2 rows where COL_NAME has length outside range [0,1]
    # ("AA" has length 2, "AAA" has length 3)
    assert len(unexpected_rows_df) == 2

    # The unexpected rows should have values "AA" and "AAA" in COL_NAME
    unexpected_values = sorted(unexpected_rows_df[COL_NAME].tolist())
    assert unexpected_values == ["AA", "AAA"]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueLengthsToBeBetween with SQL."""
    expectation = gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=0, max_value=1)
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 2 rows where COL_NAME has length outside range [0,1]
    assert len(unexpected_rows_data) == 2

    # Check that both "AA" and "AAA" appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "AA" in unexpected_rows_str
    assert "AAA" in unexpected_rows_str


# ---------------------------------------------------------------------------
# Community issue #11393: Spark and SQLAlchemy engines ignore strict_min / strict_max
# https://github.com/great-expectations/great_expectations/issues/11393
# ---------------------------------------------------------------------------

STRICT_BOUNDS_DATA = pd.DataFrame({COL_NAME: ["aa", "bbb", "cccc"]}, dtype="object")

STRICT_BOUNDS_PARAMS = [
    pytest.param(
        gxe.ExpectColumnValueLengthsToBeBetween(
            column=COL_NAME,
            min_value=2,
            max_value=4,
            strict_min=True,
            strict_max=True,
        ),
        id="both_strict",
    ),
    pytest.param(
        gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, min_value=2, strict_min=True),
        id="strict_min_only",
    ),
    pytest.param(
        gxe.ExpectColumnValueLengthsToBeBetween(column=COL_NAME, max_value=4, strict_max=True),
        id="strict_max_only",
    ),
]


@pytest.mark.parametrize("expectation", STRICT_BOUNDS_PARAMS)
@parameterize_batch_for_data_sources(
    data_source_configs=[SparkFilesystemCsvDatasourceTestConfig()],
    data=STRICT_BOUNDS_DATA,
)
def test_spark_strict_bounds_respected_issue_11393(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueLengthsToBeBetween,
) -> None:
    """Regression test for issue #11393: Spark engine must honor strict_min/strict_max
    for value lengths. Data lengths are [2, 3, 4]; each parametrized case uses a
    strict bound that excludes a boundary value, so validation must fail.
    """
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize("expectation", STRICT_BOUNDS_PARAMS)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()],
    data=STRICT_BOUNDS_DATA,
)
def test_sql_strict_bounds_respected_issue_11393(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueLengthsToBeBetween,
) -> None:
    """Regression test for issue #11393: SQLAlchemy engine must honor strict_min/strict_max
    for value lengths. Data lengths are [2, 3, 4]; each parametrized case uses a
    strict bound that excludes a boundary value, so validation must fail.
    """
    result = batch_for_datasource.validate(expectation)
    assert not result.success
