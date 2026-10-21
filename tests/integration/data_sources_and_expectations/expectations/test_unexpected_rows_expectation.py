from datetime import datetime, timezone
from typing import Literal, Sequence
from uuid import uuid4

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.constants import MAX_RESULT_RECORDS
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    DataSourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    # SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.base import Mapping

# pandas not currently supported by this Expecatation
ALL_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SparkFilesystemCsvDatasourceTestConfig(),
    # SqliteDatasourceTestConfig(),  # fix me
]


# spark and big query not currently supported with extra_data, so we can't test JOIN
# pandas not currently supported by this Expecatation
EXTRA_DATA_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    # SqliteDatasourceTestConfig(),  # fix me
]

# pandas and spark not currently supporting partitioners
_PARTITIONER_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    # SqliteDatasourceTestConfig(),  # fix me
]


# spark and big query not currently supported with extra_data, so we can't test JOIN
# pandas and spark not currently supporting partitioners
PARTITIONER_AND_EXTRA_DATA_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    DatabricksDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    # SqliteDatasourceTestConfig(),  # fix me
]

# NOTE: SQL Server historically required the TOP expression in nested queries that use ORDER BY,
#       so tests were grouped by this requirement.  Since GX-2551, ORDER BY is automatically
#       stripped for the COUNT(*) path, so SQL Server can handle ORDER BY without TOP transparently.
#       The existing TOP/non-TOP test split is kept for backwards-compatibility coverage.
# strings correspond to `label` property on TestConfig instances
DATA_SOURCE_TYPES_THAT_REQUIRE_TOP_EXPRESSION = {
    SQLServerDatasourceTestConfig().label,
}

DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION: Sequence[DataSourceTestConfig] = [
    ds
    for ds in ALL_SUPPORTED_DATA_SOURCES
    if ds.label in DATA_SOURCE_TYPES_THAT_REQUIRE_TOP_EXPRESSION
]

DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION: Sequence[DataSourceTestConfig] = [
    ds
    for ds in ALL_SUPPORTED_DATA_SOURCES
    if ds.label not in DATA_SOURCE_TYPES_THAT_REQUIRE_TOP_EXPRESSION
]

PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION: Sequence[DataSourceTestConfig] = [
    ds
    for ds in _PARTITIONER_SUPPORTED_DATA_SOURCES
    if ds.label in DATA_SOURCE_TYPES_THAT_REQUIRE_TOP_EXPRESSION
]

PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION: Sequence[
    DataSourceTestConfig
] = [
    ds
    for ds in _PARTITIONER_SUPPORTED_DATA_SOURCES
    if ds.label not in DATA_SOURCE_TYPES_THAT_REQUIRE_TOP_EXPRESSION
]

TABLE_1 = pd.DataFrame(
    {
        "entity_id": [1, 2],
        "created_at": [
            datetime(year=2024, month=12, day=1, tzinfo=timezone.utc).date(),
            datetime(year=2024, month=11, day=30, tzinfo=timezone.utc).date(),
        ],
        "quantity": [1, 2],
        "temperature": [75, 92],
        "color": ["red", "red"],
    }
)

TABLE_2 = pd.DataFrame(
    {
        "entity_id": [1, 2],
        "created_at": [
            datetime(year=2024, month=12, day=1, tzinfo=timezone.utc).date(),
            datetime(year=2024, month=11, day=30, tzinfo=timezone.utc).date(),
        ],
        "total_quantity": [1, 2],
    }
)

DATE_COLUMN = "created_at"

_SUCCESS_QUERIES = [
    "SELECT * FROM {batch} WHERE quantity > 2",
    "SELECT * FROM {batch} WHERE quantity > 2 AND temperature > 91",
    "SELECT * FROM {batch} WHERE quantity > 2 OR temperature > 92",
    "SELECT color FROM {batch} GROUP BY color HAVING SUM(quantity) > 3",
]
SUCCESS_QUERIES_WITH_TOP_EXPRESSION = _SUCCESS_QUERIES + [
    "SELECT TOP 10 * FROM {batch} WHERE quantity > 2 ORDER BY quantity DESC",
]
SUCCESS_QUERIES_WITHOUT_TOP_EXPRESSION = _SUCCESS_QUERIES + [
    "SELECT * FROM {batch} WHERE quantity > 2 ORDER BY quantity DESC",
]

JOIN_SUCCESS_QUERIES = [
    """
     SELECT t1.entity_id, t1.quantity, t2.total_quantity
     FROM {batch} t1
     JOIN table_2 t2 ON t1.entity_id = t2.entity_id
     WHERE t1.quantity <> t2.total_quantity
    """,
    """
     SELECT t1.*, t2.record_count FROM
     (SELECT * FROM {batch} AS batch) AS t1
     JOIN
     (SELECT entity_id, SUM(total_quantity) as total_quantity, COUNT(*) as record_count
      FROM table_2 GROUP BY entity_id) AS t2
     ON t1.entity_id = t2.entity_id
     WHERE t1.quantity <> t2.total_quantity
    """,
]

_FAILURE_QUERIES = [
    "SELECT * FROM {batch}",
    "SELECT * FROM {batch} WHERE quantity > 0",
    "SELECT * FROM {batch} WHERE quantity > 0 AND temperature > 74",
    "SELECT * FROM {batch} WHERE quantity > 0 OR temperature > 92",
    "SELECT color FROM {batch} GROUP BY color HAVING SUM(quantity) > 0",
]
FAIULRE_QUERIES_WITH_TOP_EXPRESSION = _FAILURE_QUERIES + [
    "SELECT TOP 10 * FROM {batch} WHERE quantity > 0 ORDER BY quantity DESC",
]
FAIULRE_QUERIES_WITHOUT_TOP_EXPRESSION = _FAILURE_QUERIES + [
    "SELECT * FROM {batch} WHERE quantity > 0 ORDER BY quantity DESC",
]

JOIN_FAILURE_QUERIES = [
    """
     SELECT t1.entity_id, t1.quantity, t2.total_quantity
     FROM {batch} t1
     JOIN table_2 t2 ON t1.entity_id = t2.entity_id
     WHERE t1.quantity = t2.total_quantity
    """,
    """
     SELECT t1.*, t2.record_count FROM
     (SELECT * FROM {batch} AS batch) AS t1
     JOIN
     (SELECT entity_id, SUM(total_quantity) as total_quantity, COUNT(*) as record_count
      FROM table_2 GROUP BY entity_id) AS t2
     ON t1.entity_id = t2.entity_id
     WHERE t1.quantity = t2.total_quantity
    """,
]


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITHOUT_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_success(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to succeed",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITH_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_success_with_top_expression(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to succeed",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=EXTRA_DATA_SUPPORTED_DATA_SOURCES,
    data=TABLE_1,
    extra_data={"table_2": TABLE_2},
)
def test_unexpected_rows_expectation_join_keyword_success(
    batch_for_datasource: Batch,
    fully_qualified_extra_table_names_for_datasource: Mapping[str, str],
) -> None:
    for join_success_query in JOIN_SUCCESS_QUERIES:
        unexpected_rows_query = join_success_query.replace(
            "table_2",
            fully_qualified_extra_table_names_for_datasource["table_2"],
        )
        expectation = gxe.UnexpectedRowsExpectation(
            description="Expect query with JOIN keyword to succeed",
            unexpected_rows_query=unexpected_rows_query,
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success
        assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=[MySQLDatasourceTestConfig(), SQLServerDatasourceTestConfig()],
    data=pd.DataFrame({"status": ["active", "active"]}),
)
def test_unexpected_rows_expectation_substring_join_without_join_keyword(
    batch_for_datasource: Batch,
) -> None:
    """A query with no JOIN at all, where "join" only appears inside a string literal,
    must be substituted and validated the same way any other non-join query is."""
    expectation = gxe.UnexpectedRowsExpectation(
        description="No JOIN keyword anywhere in this query; 'joined' is a string literal",
        unexpected_rows_query="SELECT * FROM {batch} WHERE status = 'joined'",
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=[MySQLDatasourceTestConfig()],
    data=pd.DataFrame({"status": ["active", "active"]}),
)
def test_unexpected_rows_expectation_backslash_escaped_join_without_join_keyword(
    batch_for_datasource: Batch,
) -> None:
    """MySQL treats a backslash as an escape, so this is one string literal containing "join"."""
    expectation = gxe.UnexpectedRowsExpectation(
        description="No JOIN keyword in this query; 'join' is inside an escaped literal",
        unexpected_rows_query="SELECT * FROM {batch} WHERE status = 'it\\'s a join'",
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", FAIULRE_QUERIES_WITHOUT_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_failure(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to fail",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success is False
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", FAIULRE_QUERIES_WITH_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_failure_with_top_expression(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to fail",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success is False
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=EXTRA_DATA_SUPPORTED_DATA_SOURCES,
    data=TABLE_1,
    extra_data={"table_2": TABLE_2},
)
def test_unexpected_rows_expectation_join_keyword_failure(
    batch_for_datasource: Batch,
    fully_qualified_extra_table_names_for_datasource: Mapping[str, str],
) -> None:
    for join_failure_query in JOIN_FAILURE_QUERIES:
        unexpected_rows_query = join_failure_query.replace(
            "table_2", fully_qualified_extra_table_names_for_datasource["table_2"]
        )
        expectation = gxe.UnexpectedRowsExpectation(
            description="Expect query with JOIN keyword to fail",
            unexpected_rows_query=unexpected_rows_query,
        )
        result = batch_for_datasource.validate(expectation)
        assert result.success is False
        assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITHOUT_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_partitioner_success(
    asset_for_datasource,
    unexpected_rows_query,
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name="my-batch-def", column=DATE_COLUMN
    ).get_batch()
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword and paritioner defined to succeed",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITH_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_partitioner_success_with_top_expression(
    asset_for_datasource,
    unexpected_rows_query,
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name="my-batch-def", column=DATE_COLUMN
    ).get_batch()
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword and paritioner defined to succeed",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_AND_EXTRA_DATA_SUPPORTED_DATA_SOURCES,
    data=TABLE_1,
    extra_data={"table_2": TABLE_2},
)
def test_unexpected_rows_expectation_join_keyword_partitioner_success(
    asset_for_datasource,
    fully_qualified_extra_table_names_for_datasource: Mapping[str, str],
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name="my-batch-def", column=DATE_COLUMN
    ).get_batch()
    for join_success_query in JOIN_SUCCESS_QUERIES:
        unexpected_rows_query = join_success_query.replace(
            "table_2", fully_qualified_extra_table_names_for_datasource["table_2"]
        )
        expectation = gxe.UnexpectedRowsExpectation(
            description="Expect query with JOIN keyword and paritioner defined to succeed",
            unexpected_rows_query=unexpected_rows_query,
        )
        result = batch.validate(expectation)
        assert result.success
        assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", FAIULRE_QUERIES_WITHOUT_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_partitioner_failure(
    asset_for_datasource,
    unexpected_rows_query,
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name=str(uuid4()), column=DATE_COLUMN
    ).get_batch()
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword and partitioner defined to fail",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch.validate(expectation)
    assert result.success is False
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_SUPPORTED_DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", FAIULRE_QUERIES_WITH_TOP_EXPRESSION)
def test_unexpected_rows_expectation_batch_keyword_partitioner_failure_with_top_expression(
    asset_for_datasource,
    unexpected_rows_query,
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name=str(uuid4()), column=DATE_COLUMN
    ).get_batch()
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword and partitioner defined to fail",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch.validate(expectation)
    assert result.success is False
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=PARTITIONER_AND_EXTRA_DATA_SUPPORTED_DATA_SOURCES,
    data=TABLE_1,
    extra_data={"table_2": TABLE_2},
)
def test_unexpected_rows_expectation_join_keyword_partitioner_failure(
    asset_for_datasource,
    fully_qualified_extra_table_names_for_datasource: Mapping[str, str],
) -> None:
    batch = asset_for_datasource.add_batch_definition_monthly(
        name=str(uuid4()), column=DATE_COLUMN
    ).get_batch()
    for join_failure_query in JOIN_FAILURE_QUERIES:
        unexpected_rows_query = join_failure_query.replace(
            "table_2", fully_qualified_extra_table_names_for_datasource["table_2"]
        )
        expectation = gxe.UnexpectedRowsExpectation(
            description="Expect query with JOIN keyword and paritioner defined to fail",
            unexpected_rows_query=unexpected_rows_query,
        )
        result = batch.validate(expectation)
        assert result.success is False
        assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=TABLE_1,
)
def test_success_result_format(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE entity_id = 123"
        ),
        result_format="COMPLETE",
    )

    assert result.success
    assert result.result == {
        "observed_value": 0,
        "details": {
            "unexpected_rows": [],
        },
    }


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=TABLE_1,
)
def test_fail_result_format(batch_for_datasource: Batch) -> None:
    result = batch_for_datasource.validate(
        gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE entity_id = 2"
        ),
        result_format="COMPLETE",
    )

    assert not result.success
    assert result.result == {
        "observed_value": 1,
        "details": {
            "unexpected_rows": [
                {
                    "entity_id": 2,
                    "created_at": datetime(year=2024, month=11, day=30, tzinfo=timezone.utc).date(),
                    "quantity": 2,
                    "temperature": 92,
                    "color": "red",
                }
            ],
        },
    }


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=TABLE_1,
)
@pytest.mark.parametrize(
    "result_format,expected_keys",
    [
        pytest.param("BOOLEAN_ONLY", {"success"}, id="boolean_only"),
        pytest.param("BASIC", {"success", "result"}, id="basic"),
        pytest.param("SUMMARY", {"success", "result"}, id="summary"),
        pytest.param("COMPLETE", {"success", "result"}, id="complete"),
    ],
)
def test_result_format_controls_details_visibility(
    batch_for_datasource: Batch,
    result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
    expected_keys: set[str],
) -> None:
    """Test that unexpected_rows are only visible with COMPLETE result format."""
    result = batch_for_datasource.validate(
        gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE entity_id = 2"
        ),
        result_format=result_format,
    )

    # Verify top-level keys
    assert set(result.to_json_dict().keys()) >= expected_keys

    if result_format == "BOOLEAN_ONLY":
        assert result.result == {}
    elif result_format == "COMPLETE":
        assert "details" in result.result
        assert "unexpected_rows" in result.result["details"]
        assert "observed_value" in result.result
    else:
        # BASIC and SUMMARY should not have details
        assert "details" not in result.result
        assert "observed_value" in result.result


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_DO_NOT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITHOUT_TOP_EXPRESSION)
def test_success_with_suite_param_other_table_name(
    batch_for_datasource: Batch, unexpected_rows_query
) -> None:
    suite_param_key = "test_unexpected_rows_expectation"
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to succeed",
        unexpected_rows_query={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: unexpected_rows_query}
    )
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SUCCESS_QUERIES_WITH_TOP_EXPRESSION)
def test_success_with_suite_param_other_table_name_with_top_expression(
    batch_for_datasource: Batch, unexpected_rows_query
) -> None:
    suite_param_key = "test_unexpected_rows_expectation"
    expectation = gxe.UnexpectedRowsExpectation(
        description="Expect query with {batch} keyword to succeed",
        unexpected_rows_query={"$PARAMETER": suite_param_key},
        result_format=ResultFormat.SUMMARY,
    )
    result = batch_for_datasource.validate(
        expectation, expectation_parameters={suite_param_key: unexpected_rows_query}
    )
    assert result.success


SQL_SERVER_ORDER_BY_WITHOUT_TOP_SUCCESS_QUERIES = [
    "SELECT * FROM {batch} WHERE quantity > 2 ORDER BY quantity DESC",
    "SELECT * FROM {batch} WHERE quantity > 2 ORDER BY quantity ASC, temperature DESC",
]

SQL_SERVER_ORDER_BY_WITHOUT_TOP_FAILURE_QUERIES = [
    "SELECT * FROM {batch} WHERE quantity > 0 ORDER BY quantity DESC",
]


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SQL_SERVER_ORDER_BY_WITHOUT_TOP_SUCCESS_QUERIES)
def test_sql_server_order_by_without_top_works_transparently_success(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="ORDER BY without TOP should work on SQL Server",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success
    assert result.exception_info.get("raised_exception") is False


@parameterize_batch_for_data_sources(
    data_source_configs=DATA_SOURCES_THAT_REQUIRE_TOP_EXPRESSION,
    data=TABLE_1,
)
@pytest.mark.parametrize("unexpected_rows_query", SQL_SERVER_ORDER_BY_WITHOUT_TOP_FAILURE_QUERIES)
def test_sql_server_order_by_without_top_works_transparently_failure(
    batch_for_datasource,
    unexpected_rows_query,
) -> None:
    expectation = gxe.UnexpectedRowsExpectation(
        description="ORDER BY without TOP should work on SQL Server (failing case)",
        unexpected_rows_query=unexpected_rows_query,
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success is False
    assert result.exception_info.get("raised_exception") is False


GET_UNEXPECTED_ROWS_DATA = pd.DataFrame(
    {"id": list(range(300)), "value": [i % 2 for i in range(300)]}
)


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()],
    data=GET_UNEXPECTED_ROWS_DATA,
)
def test_get_unexpected_rows_returns_all_rows(
    asset_for_datasource,
    _batch_setup_for_datasource,
) -> None:
    """get_unexpected_rows() returns >200 rows, bypassing MAX_RESULT_RECORDS."""
    context = _batch_setup_for_datasource.context
    batch_definition = asset_for_datasource.add_batch_definition_whole_table(
        name="get_unexpected_rows_bd"
    )
    suite = context.suites.add(ExpectationSuite(name="get_unexpected_rows_suite"))
    expectation = gxe.UnexpectedRowsExpectation(
        unexpected_rows_query="SELECT * FROM {batch} WHERE id > 0"
    )
    suite.add_expectation(expectation)

    vd = context.validation_definitions.add(
        ValidationDefinition(
            name="get_unexpected_rows_vd",
            data=batch_definition,
            suite=suite,
        )
    )

    result = vd.run()
    assert not result.success

    evr = result.results[0]
    rows = vd.get_unexpected_rows(evr.expectation)
    assert len(rows) == 299
    assert len(rows) > MAX_RESULT_RECORDS
