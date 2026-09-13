from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
    NON_SQL_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    SQL_DATA_SOURCES,
    PostgreSQLDatasourceTestConfig,
)

BASIC_COL = "basic"
DISTRIBUTION_WITH_OUTLIER = "with_outlier"
MOSTLY_ZERO_DISTRIBUTION = "mostly_zero"
DISTRIBUTION_WITH_NULLS = "lotta_nulls"
CONSTANT_COL = "constant"
CONSTANT_COL_WITH_NULLS = "constant_with_nulls"
SINGLE_NON_NULL_VALUE = "single_non_null_value"
INFINITY_COL = "with_infinity"

DATA = pd.DataFrame(
    {
        BASIC_COL: [1, 1, 1, 3, 3],
        DISTRIBUTION_WITH_OUTLIER: [-1000000, -1, 0, 1, 1],
        MOSTLY_ZERO_DISTRIBUTION: [1, 0, 0, 0, 0],
        DISTRIBUTION_WITH_NULLS: [-1, 0, 1, None, None],
        CONSTANT_COL: [5, 5, 5, 5, 5],
        CONSTANT_COL_WITH_NULLS: [5, 5, 5, None, None],
        # One non-null value, so the sample standard deviation is undefined rather than
        # zero. This is the input that reaches the guards' `is None` limb on SQL.
        SINGLE_NON_NULL_VALUE: [5, None, None, None, None],
    },
    dtype="object",
)

# Deliberately separate frames rather than more columns on DATA.
#
# NUMERIC_DTYPE_DATA exists because DATA is built with dtype="object", and the dtype
# decides which path a constant column takes on pandas: object raises ZeroDivisionError
# from (column - mean) / 0.0, while a numeric dtype yields NaN. Only the first is covered
# by the tests above.
NUMERIC_DTYPE_DATA = pd.DataFrame({CONSTANT_COL: [5.0, 5.0, 5.0, 5.0, 5.0]})

# INFINITY_DATA is separate because not every backend can store a floating-point infinity,
# and a column here would break every other test on those.
INFINITY_DATA = pd.DataFrame({INFINITY_COL: [1.0, 2.0, 3.0, float("inf")]})


def _assert_no_metric_exceptions(result: ExpectationValidationResult) -> None:
    """Fail loudly if any metric raised instead of producing a value."""
    exception_info = result.exception_info or {}
    if "raised_exception" in exception_info:
        assert not exception_info["raised_exception"], exception_info
    else:
        for info in exception_info.values():
            assert not (info or {}).get("raised_exception"), info


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete__non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=BASIC_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SQL_DATA_SOURCES, data=DATA)
def test_success_complete__sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=BASIC_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 5,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "partial_unexpected_counts": [],
        "unexpected_list": [],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=True
            ),
            id="basic_successful_test",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=True, mostly=0.8
            ),
            id="successful_test_with_mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True, mostly=0.6
            ),
            id="outlier_test_with_mostly",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=1.96, double_sided=False
            ),
            id="single_sided_test",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueZScoresToBeLessThan,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=BASIC_COL, threshold=0.1, double_sided=True
            ),
            id="threshold_too_low",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
            ),
            id="extreme_outlier",
        ),
        pytest.param(
            gxe.ExpectColumnValueZScoresToBeLessThan(
                column=MOSTLY_ZERO_DISTRIBUTION, threshold=1, double_sided=True, mostly=0.9
            ),
            id="mostly_requirement_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValueZScoresToBeLessThan,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueZScoresToBeLessThan."""
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
    )
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # Convert to DataFrame for easier comparison
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)
    unexpected_rows_df = unexpected_rows_data

    # Should contain 1 row with the extreme outlier (-1000000 has high z-score)
    assert len(unexpected_rows_df) == 1
    assert list(unexpected_rows_df.index) == [0]

    # The unexpected row should have the outlier value
    assert unexpected_rows_df.loc[0, DISTRIBUTION_WITH_OUTLIER] == -1000000


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValueZScoresToBeLessThan with SQL."""
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=DISTRIBUTION_WITH_OUTLIER, threshold=1, double_sided=True
    )
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

    # Should contain 1 row with the extreme outlier (-1000000 has high z-score)
    assert len(unexpected_rows_data) == 1

    # Check that "-1000000" appears in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "-1000000" in unexpected_rows_str


@pytest.mark.parametrize(
    "column",
    [
        pytest.param(CONSTANT_COL, id="constant_column"),
        pytest.param(CONSTANT_COL_WITH_NULLS, id="constant_column_with_nulls"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_zero_standard_deviation_is_consistent_across_data_sources(
    batch_for_datasource: Batch,
    column: str,
) -> None:
    """A column with no variance must produce the same verdict on every backend.

    The z-score of a constant column divides by a standard deviation of zero, and the
    backends disagree about what that means: Postgres and SQL Server raise; SQLite, MySQL
    and Spark return NULL, with Spark raising instead under
    ``spark.sql.ansi.enabled=true``; and pandas either produces NaN or raises
    ZeroDivisionError depending on the column's dtype. Left to the backend, the same data
    yielded a raised exception on some data sources, a silent pass on others, and every
    row flagged as an outlier on pandas.

    A constant column has no outliers, so the expectation succeeds with nothing
    unexpected -- and, critically, does so identically everywhere.
    """
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=column, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    _assert_no_metric_exceptions(result)
    assert result.success
    assert result.result["unexpected_count"] == 0
    assert result.result["unexpected_list"] == []


@parameterize_batch_for_data_sources(data_source_configs=ALL_DATA_SOURCES, data=DATA)
def test_undefined_standard_deviation_is_consistent_across_data_sources(
    batch_for_datasource: Batch,
) -> None:
    """Undefined variance must reach the same verdict as zero variance.

    The sample standard deviation of a single value is undefined, not zero, so this input
    misses the `== 0` arm of the guards that the constant-column test covers and exercises
    the dialect-dependent half instead. Postgres, Spark and BigQuery return NULL from
    `stddev_samp` here, which arrives as None; pandas gets NaN from `Series.std()` and
    resolves it in `_pandas_condition` rather than in the guard.

    A column that cannot have outliers must not report any, whichever of those paths runs.
    """
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=SINGLE_NON_NULL_VALUE, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    _assert_no_metric_exceptions(result)
    assert result.success
    assert result.result["unexpected_count"] == 0
    assert result.result["unexpected_list"] == []


@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=NUMERIC_DTYPE_DATA
)
def test_zero_standard_deviation_on_a_numeric_dtype_column(batch_for_datasource: Batch) -> None:
    """A constant column reaches the same verdict whatever its pandas dtype.

    The two dtypes take different paths: on an object-dtype column
    (column - mean) / 0.0 raises ZeroDivisionError, and on a numeric one it yields NaN
    that the isna() limb in _pandas_condition accepts. Every column in DATA is object
    dtype, so without this the numeric path is untested.
    """
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=CONSTANT_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    _assert_no_metric_exceptions(result)
    assert result.success
    assert result.result["unexpected_count"] == 0


# pandas only. The SQL engines cannot reach this metric at all with an infinite value:
# `column.mean` raises decimal.InvalidOperation on PostgreSQL, from convert_decimal_to_float
# handling a Decimal("Infinity"). That divergence is upstream of the z-score metric, in the
# aggregate metrics, and is not something this change reaches.
@pytest.mark.filterwarnings("ignore:invalid value encountered in subtract:RuntimeWarning")
@parameterize_batch_for_data_sources(
    data_source_configs=JUST_PANDAS_DATA_SOURCES, data=INFINITY_DATA
)
def test_infinite_value_has_no_defined_z_score(batch_for_datasource: Batch) -> None:
    """An infinite value makes the variance undefined, so no row can be an outlier.

    Series.std() returns NaN for a column containing inf -- not zero, and not None -- so
    this input clears the guard in _pandas_function entirely and is resolved by the isna()
    limb in _pandas_condition instead. Before this fix every row was reported as an
    outlier, because NaN compares False against any threshold.

    The warning filter is needed because numpy emits "invalid value encountered in
    subtract" computing the variance, and this suite promotes warnings to errors, which
    would otherwise fail inside column.standard_deviation before the z-score metric ran.
    """
    expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
        column=INFINITY_COL, threshold=1.96, double_sided=True
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    _assert_no_metric_exceptions(result)
    assert result.success
    assert result.result["unexpected_count"] == 0
