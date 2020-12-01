import logging
import os

import pandas as pd
import pytest

from great_expectations.core.batch import Batch, BatchSpec
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.exceptions import InvalidConfigError
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics import (
    ColumnMean,
    ColumnStandardDeviation,
    ColumnValuesInSet,
    ColumnValuesZScore,
)
from great_expectations.validator.validation_graph import MetricConfiguration

# Function to test for spark dataframe equality
from tests.test_utils import _build_sa_engine


def test_instantiation_via_connection_string(sa, test_db_connection_string):
    my_execution_engine = SqlAlchemyExecutionEngine(
        connection_string=test_db_connection_string
    )
    assert my_execution_engine.connection_string == test_db_connection_string
    assert my_execution_engine.credentials == None
    assert my_execution_engine.url == None

    my_execution_engine.get_batch_data_and_markers(
        BatchSpec(
            table_name="main.table_1",
            sampling_method="_sample_using_limit",
            sampling_kwargs={"n": 5},
        )
    )


def test_instantiation_via_url(sa):
    db_file = file_relative_path(
        __file__,
        os.path.join("..", "test_sets", "test_cases_for_sql_data_connector.db"),
    )
    my_execution_engine = SqlAlchemyExecutionEngine(url="sqlite:///" + db_file)
    assert my_execution_engine.connection_string == None
    assert my_execution_engine.credentials == None
    assert my_execution_engine.url[-36:] == "test_cases_for_sql_data_connector.db"

    my_execution_engine.get_batch_data_and_markers(
        BatchSpec(
            table_name="table_partitioned_by_date_column__A",
            sampling_method="_sample_using_limit",
            sampling_kwargs={"n": 5},
        )
    )


def test_instantiation_via_credentials(sa, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("test_database_store_backend_get_url_for_key requires postgresql")

    my_execution_engine = SqlAlchemyExecutionEngine(
        credentials={
            "drivername": "postgresql",
            "username": "postgres",
            "password": "",
            "host": "localhost",
            "port": "5432",
            "database": "test_ci",
        }
    )
    assert my_execution_engine.connection_string == None
    assert my_execution_engine.credentials == {
        "username": "postgres",
        "password": "",
        "host": "localhost",
        "port": "5432",
        "database": "test_ci",
    }
    assert my_execution_engine.url == None

    # Note Abe 20201116: Let's add an actual test of get_batch_data_and_markers, which will require setting up test
    # fixtures
    # my_execution_engine.get_batch_data_and_markers(BatchSpec(
    #     table_name="main.table_1",
    #     sampling_method="_sample_using_limit",
    #     sampling_kwargs={
    #         "n": 5
    #     }
    # ))


def test_instantiation_error_states(sa, test_db_connection_string):
    with pytest.raises(InvalidConfigError):
        SqlAlchemyExecutionEngine()


# Testing batching of aggregate metrics
def test_sa_batch_aggregate_metrics(caplog, sa):
    import datetime

    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        )
    )
    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_1},
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_2},
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_3},
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_4},
    )
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    start = datetime.datetime.now()
    res = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    end = datetime.datetime.now()
    print("t1")
    print(end - start)
    assert res[desired_metric_1.id] == 3
    assert res[desired_metric_2.id] == 1
    assert res[desired_metric_3.id] == 4
    assert res[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SqlAlchemyExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


# Ensuring functionality of compute_domain when no domain kwargs are given
def test_get_compute_domain_with_no_domain_kwargs(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="table"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(["*"]).select_from(engine.active_batch_data.selectable)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - column_pair
def test_get_compute_domain_with_column_pair(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    # Fetching data, compute_domain_kwargs, accessor_kwargs
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="column_pair"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(["*"]).select_from(engine.active_batch_data.selectable)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

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

    # Building new engine so that values still found
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    data2, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="identity"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select([sa.column("a"), sa.column("b")]).select_from(
            engine.active_batch_data.selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data2)).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn(sa):
    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]})
    )

    # Obtaining compute domain
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="multicolumn"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(["*"]).select_from(engine.active_batch_data.selectable)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"

    # Checking for identity
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="identity"
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select([sa.column("a"), sa.column("b"), sa.column("c")]).select_from(
            engine.active_batch_data.selectable
        )
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    # Loading batch data
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.COLUMN
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(["*"]).select_from(engine.active_batch_data.selectable)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"

    # Testing for identity
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    # Loading batch data
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type=MetricDomainTypes.IDENTITY
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select([sa.column("a")]).select_from(engine.active_batch_data.selectable)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {"column": "a"}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "row_condition": 'col("b") > 24',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="identity",
    )

    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    raw_data = engine.engine.execute(
        sa.select(["*"])
        .select_from(engine.active_batch_data.selectable)
        .where(sa.column("b") > 24)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

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
        sa.select(["*"])
        .select_from(engine.active_batch_data.selectable)
        .where(sa.column("b") == 2)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert raw_data == domain_data, "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "b"}, "Accessor kwargs have been modified"

    # Should react differently for domain type identity
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="identity",
    )

    # Ensuring data has been properly queried
    # Seeing if raw data is the same as the data after condition has been applied - checking post computation data
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    raw_data = engine.engine.execute(
        sa.select(["*"])
        .select_from(engine.active_batch_data.selectable)
        .where(sa.column("b") == 2)
    ).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_nonexistent_condition_parser(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
            domain_type=MetricDomainTypes.TABLE,
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric(sa):
    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.does_not_exist",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )

    # Ensuring a metric provider error is raised if metric does not exist
    with pytest.raises(MetricProviderError) as e:
        res = engine.resolve_metrics(
            metrics_to_resolve=(
                desired_metric_1,
                desired_metric_2,
                desired_metric_3,
                desired_metric_4,
            )
        )
        print(e)
