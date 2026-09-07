import subprocess
import sys
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent import SQLDatasource
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.expectations.row_conditions import Column
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
    NON_SQL_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    SQL_DATA_SOURCES,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    SqliteDatasourceTestConfig,
)

UNIQUE_INTS = "unique_integers"
DUPLICATE_INTS = "duplicate_integers"
UNIQUE_STRINGS = "unique_strings"
DUPLICATE_STRINGS = "duplicate_strings"
UNIQUE_WITH_NULL = "unique_with_null"
DUPLICATE_WITH_NULL = "duplicate_with_null"

DATA = pd.DataFrame(
    {
        UNIQUE_INTS: [1, 2, 3, 4],
        DUPLICATE_INTS: [1, 2, 3, 3],
        UNIQUE_STRINGS: ["a", "b", "c", "d"],
        DUPLICATE_STRINGS: ["a", "b", "c", "c"],
        UNIQUE_WITH_NULL: [1, 2, None, None],
        DUPLICATE_WITH_NULL: [1, 1, None, None],
    },
    dtype="object",
)

SUPPORTED_SQL_DATASOURCES = [
    ds
    for ds in SQL_DATA_SOURCES
    if not isinstance(ds, MySQLDatasourceTestConfig)  # why don't we support MySQL?
]


@parameterize_batch_for_data_sources(data_source_configs=NON_SQL_DATA_SOURCES, data=DATA)
def test_success_complete_non_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeUnique(
        column=UNIQUE_INTS,
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_SQL_DATASOURCES, data=DATA)
def test_success_complete_sql(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToBeUnique(
        column=UNIQUE_INTS,
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    assert result.success
    assert result.to_json_dict()["result"] == {
        "element_count": 4,
        "unexpected_count": 0,
        "unexpected_percent": 0.0,
        "partial_unexpected_list": [],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 0.0,
        "unexpected_percent_nonmissing": 0.0,
        "unexpected_index_query": ANY,
        "partial_unexpected_counts": [],
        "unexpected_list": [],
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=UNIQUE_INTS,
            ),
            id="unique_integers",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=UNIQUE_STRINGS,
            ),
            id="unique_strings",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=UNIQUE_WITH_NULL,
            ),
            id="unique_with_null",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_INTS, mostly=0.5),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeUnique,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=DUPLICATE_INTS,
            ),
            id="duplicate_integers",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=DUPLICATE_STRINGS,
            ),
            id="duplicate_strings",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(
                column=DUPLICATE_WITH_NULL,
            ),
            id="duplicate_nulls",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_INTS, mostly=0.7),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToBeUnique,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeUnique with pandas data sources."""
    expectation = gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_INTS)
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

    # Should contain 2 rows where DUPLICATE_INTS has duplicate value 3
    # (both occurrences are flagged)
    assert len(unexpected_rows_df) == 2

    # Both unexpected rows should have value 3 in DUPLICATE_INTS
    assert all(unexpected_rows_df[DUPLICATE_INTS] == 3)

    # Other columns should have their original values
    unexpected_strings = sorted(unexpected_rows_df[DUPLICATE_STRINGS].tolist())
    assert unexpected_strings == ["c", "c"]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToBeUnique with SQL data sources."""
    expectation = gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_INTS)
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

    # Should contain 2 rows where DUPLICATE_INTS has duplicate value 3
    # (both occurrences are flagged)
    assert len(unexpected_rows_data) == 2

    # Check that the duplicate values appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "3" in unexpected_rows_str
    assert "c" in unexpected_rows_str


# The tests below pin down result_format behaviors of ExpectColumnValuesToBeUnique on
# SQL backends that are easy to break when the underlying `column_values.unique` metric
# implementation changes: composition with row_condition, the executability of
# unexpected_index_query, and the shared output shape of exclude_unexpected_values.

ROW_ID = "row_id"
FILTER_FLAG = "filter_flag"
DUPLICATE_VALUES = "duplicate_values"

RESULT_FORMAT_GUARD_DATA = pd.DataFrame(
    {
        ROW_ID: [1, 2, 3, 4, 5],
        FILTER_FLAG: [0, 1, 1, 1, 1],
        DUPLICATE_VALUES: [200, 200, 300, 300, 400],
    }
)

RESULT_FORMAT_GUARD_DATA_SOURCES = [
    PostgreSQLDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


def _assert_no_metric_exceptions(result: ExpectationValidationResult) -> None:
    """Fail loudly if any metric raised instead of producing a value."""
    exception_info = result.exception_info or {}
    if "raised_exception" in exception_info:
        assert not exception_info["raised_exception"], exception_info
    else:
        for info in exception_info.values():
            assert not (info or {}).get("raised_exception"), info


@parameterize_batch_for_data_sources(
    data_source_configs=RESULT_FORMAT_GUARD_DATA_SOURCES, data=RESULT_FORMAT_GUARD_DATA
)
def test_complete_with_row_condition_and_unexpected_index_column_names_sql(
    batch_for_datasource: Batch,
) -> None:
    """Row-retrieval result_format options must compose with row_condition.

    The value 200 is duplicated only when the filtered-out row is included, so correct
    results prove both that the row_condition was applied and that the index list was
    hydrated without error.
    """
    expectation = gxe.ExpectColumnValuesToBeUnique(
        column=DUPLICATE_VALUES,
        row_condition=Column(FILTER_FLAG) > 0,
        condition_parser="great_expectations",
    )
    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ROW_ID],
        },
    )

    _assert_no_metric_exceptions(result)
    assert not result.success
    assert result.result["unexpected_count"] == 2
    unexpected_index_list = sorted(
        result.result["unexpected_index_list"], key=lambda entry: entry[ROW_ID]
    )
    assert unexpected_index_list == [
        {ROW_ID: 3, DUPLICATE_VALUES: 300},
        {ROW_ID: 4, DUPLICATE_VALUES: 300},
    ]


@parameterize_batch_for_data_sources(
    data_source_configs=RESULT_FORMAT_GUARD_DATA_SOURCES, data=RESULT_FORMAT_GUARD_DATA
)
def test_include_unexpected_rows_with_row_condition_sql(
    batch_for_datasource: Batch,
) -> None:
    """include_unexpected_rows must compose with row_condition on SQL backends."""
    expectation = gxe.ExpectColumnValuesToBeUnique(
        column=DUPLICATE_VALUES,
        row_condition=Column(FILTER_FLAG) > 0,
        condition_parser="great_expectations",
    )
    result = batch_for_datasource.validate(
        expectation,
        result_format={"result_format": "SUMMARY", "include_unexpected_rows": True},
    )

    _assert_no_metric_exceptions(result)
    assert not result.success
    unexpected_rows = sorted(result.result["unexpected_rows"], key=lambda row: row[ROW_ID])
    assert unexpected_rows == [
        {ROW_ID: 3, FILTER_FLAG: 1, DUPLICATE_VALUES: 300},
        {ROW_ID: 4, FILTER_FLAG: 1, DUPLICATE_VALUES: 300},
    ]


@parameterize_batch_for_data_sources(
    data_source_configs=RESULT_FORMAT_GUARD_DATA_SOURCES, data=RESULT_FORMAT_GUARD_DATA
)
def test_unexpected_index_query_is_executable_sql(
    batch_for_datasource: Batch,
) -> None:
    """The unexpected_index_query surfaced to users must run against the source database
    and return exactly the unexpected rows.
    """
    expectation = gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_VALUES)
    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ROW_ID],
        },
    )

    _assert_no_metric_exceptions(result)
    assert not result.success
    unexpected_index_query = result.result["unexpected_index_query"]
    assert unexpected_index_query

    datasource = batch_for_datasource.datasource
    assert isinstance(datasource, SQLDatasource)
    with datasource.get_engine().connect() as connection:
        query_results = connection.execute(sa.text(unexpected_index_query.rstrip(";"))).fetchall()

    # Query selects the index column(s) followed by the expectation's column.
    assert sorted(tuple(row) for row in query_results) == [
        (1, 200),
        (2, 200),
        (3, 300),
        (4, 300),
    ]


@parameterize_batch_for_data_sources(
    data_source_configs=RESULT_FORMAT_GUARD_DATA_SOURCES, data=RESULT_FORMAT_GUARD_DATA
)
def test_exclude_unexpected_values_returns_columnar_index_list_sql(
    batch_for_datasource: Batch,
) -> None:
    """With exclude_unexpected_values=True, all SQL map expectations return a single
    columnar entry ({index_column: [values, ...]}) rather than one dict per row.
    ExpectColumnValuesToBeUnique must match that shared shape.
    """
    expectation = gxe.ExpectColumnValuesToBeUnique(column=DUPLICATE_VALUES)
    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ROW_ID],
            "exclude_unexpected_values": True,
        },
    )

    _assert_no_metric_exceptions(result)
    assert not result.success
    unexpected_index_list = result.result["unexpected_index_list"]
    assert len(unexpected_index_list) == 1
    assert sorted(unexpected_index_list[0][ROW_ID]) == [1, 2, 3, 4]
    assert DUPLICATE_VALUES not in unexpected_index_list[0]


# The SQL implementation adds a helper count column to an intermediate projection.
# Nothing stops a user column from carrying the same name, so the data below is shaped
# to collide with it deliberately.
COUNT_LABEL_COLLISION_COLUMN = "_num_rows"

COUNT_LABEL_COLLISION_DATA = pd.DataFrame(
    {
        ROW_ID: [1, 2, 3],
        COUNT_LABEL_COLLISION_COLUMN: [10, 10, 20],
    }
)


@parameterize_batch_for_data_sources(
    data_source_configs=RESULT_FORMAT_GUARD_DATA_SOURCES, data=COUNT_LABEL_COLLISION_DATA
)
def test_column_named_like_internal_count_label_sql(
    batch_for_datasource: Batch,
) -> None:
    """A source column may share a name with a helper column the implementation adds.

    The duplicate check must compare against its own count column, never against a
    same-named source column that happens to sit in the same projection. When the two
    are confused the comparison silently runs against user data: no error is raised,
    but the summary fields report the wrong rows and disagree with the index list,
    which is built by a separate code path.
    """
    expectation = gxe.ExpectColumnValuesToBeUnique(column=COUNT_LABEL_COLLISION_COLUMN)
    result = batch_for_datasource.validate(
        expectation,
        result_format={
            "result_format": "COMPLETE",
            "unexpected_index_column_names": [ROW_ID],
        },
    )

    _assert_no_metric_exceptions(result)
    assert not result.success
    # Only the value 10 repeats, so both of its rows are unexpected and 20 is not.
    assert result.result["unexpected_count"] == 2
    assert sorted(result.result["unexpected_list"]) == [10, 10]
    assert sorted(entry[ROW_ID] for entry in result.result["unexpected_index_list"]) == [1, 2]


@pytest.mark.timeout(30)  # the subprocess pays full library import cost
@pytest.mark.unit
def test_import_does_not_emit_metric_reregistration_warnings() -> None:
    """Importing great_expectations must not warn about metric providers being
    overwritten. Such warnings indicate a metric (e.g. column_values.unique) is
    registered more than once with different providers, and they surface on stderr for
    every user of the library.
    """
    completed = subprocess.run(
        [sys.executable, "-c", "import great_expectations"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "overwriting metric_provider" not in completed.stderr
    assert "is being registered with different metric_provider" not in completed.stderr
