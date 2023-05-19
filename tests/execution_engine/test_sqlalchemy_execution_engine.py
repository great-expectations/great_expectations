import logging
import os
from typing import Dict, Tuple, cast

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypes,
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)

# Function to test for spark dataframe equality
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
)
from great_expectations.self_check.util import build_sa_engine
from great_expectations.util import get_sqlalchemy_domain_data
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator
from tests.expectations.test_util import get_table_columns_metric
from tests.test_utils import get_sqlite_table_names, get_sqlite_temp_table_names

try:
    sqlalchemy = pytest.importorskip("sqlalchemy")
except ImportError:
    sqlalchemy = None

pytestmark = pytest.mark.sqlalchemy_version_compatibility


def test_instantiation_via_connection_string(sa, test_db_connection_string):
    my_execution_engine = SqlAlchemyExecutionEngine(
        connection_string=test_db_connection_string
    )
    assert my_execution_engine.connection_string == test_db_connection_string
    assert my_execution_engine.credentials is None
    assert my_execution_engine.url is None

    my_execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            table_name="table_1",
            schema_name="main",
            sampling_method="_sample_using_limit",
            sampling_kwargs={"n": 5},
        )
    )


def test_instantiation_via_url(sa):
    db_file = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    my_execution_engine = SqlAlchemyExecutionEngine(url="sqlite:///" + db_file)
    assert my_execution_engine.connection_string is None
    assert my_execution_engine.credentials is None
    assert my_execution_engine.url[-36:] == "test_cases_for_sql_data_connector.db"

    my_execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            table_name="table_partitioned_by_date_column__A",
            sampling_method="_sample_using_limit",
            sampling_kwargs={"n": 5},
        )
    )


@pytest.mark.integration
def test_instantiation_via_url_and_retrieve_data_with_other_dialect(sa):
    """Ensure that we can still retrieve data when the dialect is not recognized."""

    # 1. Create engine with sqlite db
    db_file = file_relative_path(
        __file__,
        os.path.join(  # noqa: PTH118
            "..", "test_sets", "test_cases_for_sql_data_connector.db"
        ),
    )
    my_execution_engine = SqlAlchemyExecutionEngine(url="sqlite:///" + db_file)
    assert my_execution_engine.connection_string is None
    assert my_execution_engine.credentials is None
    assert my_execution_engine.url[-36:] == "test_cases_for_sql_data_connector.db"

    # 2. Change dialect to one not listed in GXSqlDialect
    my_execution_engine.engine.dialect.name = "other_dialect"

    # 3. Get data
    num_rows_in_sample: int = 10
    batch_data, _ = my_execution_engine.get_batch_data_and_markers(
        batch_spec=SqlAlchemyDatasourceBatchSpec(
            table_name="table_partitioned_by_date_column__A",
            sampling_method="_sample_using_limit",
            sampling_kwargs={"n": num_rows_in_sample},
        )
    )

    # 4. Assert dialect and data are as expected

    assert batch_data.dialect == GXSqlDialect.OTHER

    my_execution_engine.load_batch_data("__", batch_data)
    validator = Validator(my_execution_engine)
    assert len(validator.head(fetch_all=True)) == num_rows_in_sample


def test_instantiation_via_credentials(sa, test_backends, test_df):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    my_execution_engine = SqlAlchemyExecutionEngine(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
            "port": "5432",
            "database": "test_ci",
        }
    )
    assert my_execution_engine.connection_string is None
    assert my_execution_engine.credentials == {
        "username": "postgres",
        "password": "",
        "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "port": "5432",
        "database": "test_ci",
    }
    assert my_execution_engine.url is None

    # Note Abe 20201116: Let's add an actual test of get_batch_data_and_markers, which will require setting up test
    # fixtures
    # my_execution_engine.get_batch_data_and_markers(batch_spec=BatchSpec(
    #     table_name="main.table_1",
    #     sampling_method="_sample_using_limit",
    #     sampling_kwargs={
    #         "n": 5
    #     }
    # ))


def test_instantiation_error_states(sa, test_db_connection_string):
    with pytest.raises(gx_exceptions.InvalidConfigError):
        SqlAlchemyExecutionEngine()


# Testing batching of aggregate metrics
def test_sa_batch_aggregate_metrics(caplog, sa):
    import datetime

    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    aggregate_fn_metric_1 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric_1.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    aggregate_fn_metric_2 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric_2.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    aggregate_fn_metric_3 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric_3.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    aggregate_fn_metric_4 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric_4.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            aggregate_fn_metric_1,
            aggregate_fn_metric_2,
            aggregate_fn_metric_3,
            aggregate_fn_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_1.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric_1,
        "table.columns": table_columns_metric,
    }
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_2.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric_2,
        "table.columns": table_columns_metric,
    }
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_3.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric_3,
        "table.columns": table_columns_metric,
    }
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric_4,
        "table.columns": table_columns_metric,
    }
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    start = datetime.datetime.now()
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)
    end = datetime.datetime.now()
    print("t1")
    print(end - start)
    assert results[desired_metric_1.id] == 3
    assert results[desired_metric_2.id] == 1
    assert results[desired_metric_3.id] == 4
    assert results[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SqlAlchemyExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


def test_get_domain_records_with_column_domain(sa):
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("b")<5',
            "condition_parser": "great_expectations__experimental__",
        }
    )
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    expected_column_df = df.iloc[:3]
    engine = build_sa_engine(expected_column_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_column_domain_and_filter_conditions(sa):
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("b")<5',
            "condition_parser": "great_expectations__experimental__",
            "filter_conditions": [
                RowCondition(
                    condition='col("b").notnull()',
                    condition_type=RowConditionParserType.GE,
                )
            ],
        }
    )
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    expected_column_df = df.iloc[:3]
    engine = build_sa_engine(expected_column_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_different_column_domain_and_filter_conditions(sa):
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("a")<2',
            "condition_parser": "great_expectations__experimental__",
            "filter_conditions": [
                RowCondition(
                    condition='col("b").notnull()',
                    condition_type=RowConditionParserType.GE,
                )
            ],
        }
    )
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    expected_column_df = df.iloc[:1]
    engine = build_sa_engine(expected_column_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_column_domain_and_filter_conditions_raises_error_on_multiple_conditions(
    sa,
):
    df = pd.DataFrame(
        {"a": [1, 2, 3, 4, 5], "b": [2, 3, 4, 5, None], "c": [1, 2, 3, 4, None]}
    )
    engine = build_sa_engine(df, sa)
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        engine.get_domain_records(
            domain_kwargs={
                "column": "a",
                "row_condition": 'col("a")<2',
                "condition_parser": "great_expectations__experimental__",
                "filter_conditions": [
                    RowCondition(
                        condition='col("b").notnull()',
                        condition_type=RowConditionParserType.GE,
                    ),
                    RowCondition(
                        condition='col("c").notnull()',
                        condition_type=RowConditionParserType.GE,
                    ),
                ],
            }
        )


def test_get_domain_records_with_column_pair_domain(sa):
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "both_values_are_missing",
        }
    )
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    expected_column_pair_df = pd.DataFrame(
        {"a": [2, 3, 4, 6], "b": [3.0, 4.0, 5.0, 6.0], "c": [2.0, 3.0, 4.0, None]}
    )
    engine = build_sa_engine(expected_column_pair_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"

    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "either_value_is_missing",
        }
    )
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    expected_column_pair_df = pd.DataFrame(
        {"a": [2, 3, 4], "b": [3, 4, 5], "c": [2, 3, 4]}
    )
    engine = build_sa_engine(expected_column_pair_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"

    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_A": "b",
            "column_B": "c",
            "row_condition": 'col("a")<6',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "neither",
        }
    )
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    expected_column_pair_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5],
            "b": [2.0, 3.0, 4.0, 5.0, None],
            "c": [1.0, 2.0, 3.0, 4.0, 5.0],
        }
    )
    engine = build_sa_engine(expected_column_pair_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"


def test_get_domain_records_with_multicolumn_domain(sa):
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["a", "c"],
            "row_condition": 'col("b")>2',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "all_values_are_missing",
        }
    )
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    expected_multicolumn_df = pd.DataFrame(
        {"a": [2, 3, 4, 5], "b": [3, 4, 5, 7], "c": [2, 3, 4, 6]}, index=[0, 1, 2, 4]
    )
    engine = build_sa_engine(expected_multicolumn_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"

    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, None, 6],
            "c": [1, 2, 3, 4, 5, None],
        }
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "row_condition": 'col("a")<5',
            "condition_parser": "great_expectations__experimental__",
            "ignore_row_if": "any_value_is_missing",
        }
    )
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    expected_multicolumn_df = pd.DataFrame(
        {"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [1, 2, 3, 4]}, index=[0, 1, 2, 3]
    )
    engine = build_sa_engine(expected_multicolumn_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"

    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        }
    )
    engine = build_sa_engine(df, sa)
    data = engine.get_domain_records(
        domain_kwargs={
            "column_list": ["b", "c"],
            "ignore_row_if": "never",
        }
    )
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    expected_multicolumn_df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4, None, 5],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [1, 2, 3, 4, None, 6],
        },
        index=[0, 1, 2, 3, 4, 5],
    )
    engine = build_sa_engine(expected_multicolumn_df, sa)
    expected_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()

    assert (
        domain_data == expected_data
    ), "Data does not match after getting full access compute domain"


# Ensuring functionality of compute_domain when no domain kwargs are given
def test_get_compute_domain_with_no_domain_kwargs(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="table"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - column_pair
def test_get_compute_domain_with_column_pair(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    # Fetching data, compute_domain_kwargs, accessor_kwargs
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="column_pair"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert (
        "column_A" not in compute_kwargs.keys()
        and "column_B" not in compute_kwargs.keys()
    ), "domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]}),
        sa,
    )

    # Obtaining compute domain
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_list": ["a", "b", "c"]}, domain_type="multicolumn"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_list": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    # Loading batch data
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(
        sa.select(sa.text("*")).select_from(data)
    ).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "a",
            "row_condition": 'col("b") > 24',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="column",
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*"))
        .select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
        .where(sa.column("b") > 24)
    ).fetchall()
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    # Obtaining data from computation
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="column",
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(sa.text("*"))
        .select_from(
            cast(SqlAlchemyBatchData, engine.batch_manager.active_batch_data).selectable
        )
        .where(sa.column("b") == 2)
    ).fetchall()
    domain_data = engine.engine.execute(get_sqlalchemy_domain_data(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "b"}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_nonexistent_condition_parser(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}), sa
    )

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
            domain_type=MetricDomainTypes.TABLE,
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.does_not_exist",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )

    # Ensuring a metric provider error is raised if metric does not exist
    with pytest.raises(gx_exceptions.MetricProviderError) as e:
        # noinspection PyUnusedLocal
        engine.resolve_metrics(
            metrics_to_resolve=(
                desired_metric_1,
                desired_metric_2,
                desired_metric_3,
                desired_metric_4,
            )
        )
        print(e)


def test_resolve_metric_bundle_with_compute_domain_kwargs_json_serialization(sa):
    """
    Insures that even when "compute_domain_kwargs" has multiple keys, it will be JSON-serialized for "IDDict.to_id()".
    """
    engine = build_sa_engine(
        pd.DataFrame(
            {
                "names": [
                    "Ada Lovelace",
                    "Alan Kay",
                    "Donald Knuth",
                    "Edsger Dijkstra",
                    "Guido van Rossum",
                    "John McCarthy",
                    "Marvin Minsky",
                    "Ray Ozzie",
                ]
            }
        ),
        sa,
        batch_id="1234",
    )

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
            "batch_id": "1234",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    try:
        results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))
    except gx_exceptions.MetricProviderError as e:
        assert False, str(e)

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.max",
        metric_domain_kwargs={
            "batch_id": "1234",
        },
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }

    try:
        results = engine.resolve_metrics(
            metrics_to_resolve=(desired_metric,), metrics=results
        )
        assert results == {desired_metric.id: 16}
    except gx_exceptions.MetricProviderError as e:
        assert False, str(e)


def test_get_batch_data_and_markers_using_query(sqlite_view_engine, test_df):
    my_execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_view_engine
    )
    add_dataframe_to_db(df=test_df, name="test_table_0", con=my_execution_engine.engine)

    query: str = "SELECT * FROM test_table_0"
    batch_spec = RuntimeQueryBatchSpec(
        query=query,
    )
    batch_data, batch_markers = my_execution_engine.get_batch_data_and_markers(
        batch_spec=batch_spec
    )

    assert batch_spec.query == query
    assert len(get_sqlite_temp_table_names(sqlite_view_engine)) == 2
    assert batch_markers.get("ge_load_time") is not None


def test_sa_batch_unexpected_condition_temp_table(caplog, sa):
    def validate_tmp_tables(execution_engine):
        temp_tables = [
            name
            for name in get_sqlite_temp_table_names(execution_engine)
            if name.startswith("ge_temp_")
        ]
        tables = [
            name
            for name in get_sqlite_table_names(execution_engine)
            if name.startswith("ge_temp_")
        ]
        assert len(temp_tables) == 0
        assert len(tables) == 0

    execution_engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(engine=execution_engine)
    metrics.update(results)

    validate_tmp_tables(execution_engine=execution_engine)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = execution_engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,), metrics=metrics
    )
    metrics.update(results)

    validate_tmp_tables(execution_engine=execution_engine)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
    }
    # noinspection PyUnusedLocal
    results = execution_engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )

    validate_tmp_tables(execution_engine=execution_engine)


class TestExecuteQuery:
    """What does this test and why?

    The `SQLAlchemyExecutionEngine.execute_query()` method exists to encapsulate
    the creation and management of connections. Here we check that it works
    as intended.
    """

    def test_execute_query(self, sa):
        execution_engine: SqlAlchemyExecutionEngine = build_sa_engine(
            pd.DataFrame({"a": [1, 2], "b": [4, 4]}), sa
        )
        data = execution_engine.get_domain_records(
            domain_kwargs={
                "column": "a",
            }
        )
        result = execution_engine.execute_query(
            sa.select(sa.text("*")).select_from(data)
        )
        expected = [(1, 4), (2, 4)]
        assert [r for r in result] == expected


class TestConnectionPersistence:
    """What does this test and why?

    sqlite/mssql temp tables only persist within a connection, so we need to keep the connection alive.
    These tests ensure that we use the existing connection if one is available.
    """

    def test_same_connection_used_sqlite(self, sa):
        """What does this test and why?

        We want to make sure that the same connection is used for subsequent queries.
        """
        execution_engine: SqlAlchemyExecutionEngine = build_sa_engine(
            pd.DataFrame({"a": [1, 2], "b": [4, 4]}), sa
        )

        assert execution_engine.dialect_name == GXSqlDialect.SQLITE
        assert execution_engine._connection is None

        data = execution_engine.get_domain_records(
            domain_kwargs={
                "column": "a",
            }
        )
        query = sa.select(sa.text("*")).select_from(data)
        execution_engine.execute_query(query)

        assert execution_engine._connection is not None
        connection_id = id(execution_engine._connection)

        execution_engine.execute_query(query)

        assert execution_engine._connection is not None
        assert connection_id == id(execution_engine._connection)

    def test_same_connection_used_from_static_pool(self, sa):
        """What does this test and why?

        We want to make sure that the same connection is used for subsequent queries.
        """
        execution_engine: SqlAlchemyExecutionEngine = build_sa_engine(
            pd.DataFrame({"a": [1, 2], "b": [4, 4]}), sa
        )

        assert execution_engine.dialect_name == GXSqlDialect.SQLITE

        pre_query_connection = execution_engine.get_connection()

        data = execution_engine.get_domain_records(
            domain_kwargs={
                "column": "a",
            }
        )
        query = sa.select(sa.text("*")).select_from(data)
        execution_engine.execute_query(query)

        post_query_connection = execution_engine.get_connection()

        assert pre_query_connection == post_query_connection

    def test_connection_closed_sqlite(self, sa):
        """What does this test and why?

        We want to make sure that the connection is closed when the execution
        engine is deleted.
        """
        execution_engine: SqlAlchemyExecutionEngine = build_sa_engine(
            pd.DataFrame({"a": [1, 2], "b": [4, 4]}), sa
        )

        assert execution_engine.dialect_name == GXSqlDialect.SQLITE
        assert execution_engine._connection is None

        data = execution_engine.get_domain_records(
            domain_kwargs={
                "column": "a",
            }
        )
        query = sa.select(sa.text("*")).select_from(data)
        execution_engine.execute_query(query)

        assert execution_engine._connection is not None

        connection = execution_engine._connection
        execution_engine.__del__()
        assert connection.closed
