import pytest

from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.expectations.metrics import (
    MapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.column_map_condition_auxilliary_methods import (
    _sqlalchemy_column_map_condition_values,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture
def execution_engine_with_mini_taxi_loaded(sa, mini_taxi_df):
    sqlite_engine = sa.create_engine("sqlite://")
    dataframe = mini_taxi_df
    add_dataframe_to_db(
        df=dataframe,
        name="test_table",
        con=sqlite_engine,
        index=False,
    )
    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlite_engine
    )
    return execution_engine


@pytest.fixture
def execution_engine_with_mini_taxi_table_name(
    sa, execution_engine_with_mini_taxi_loaded
):
    execution_engine = execution_engine_with_mini_taxi_loaded
    # BatchData created with `table_name`
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        table_name="test_table",
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def execution_engine_with_mini_taxi_query(sa, execution_engine_with_mini_taxi_loaded):
    execution_engine = execution_engine_with_mini_taxi_loaded
    # BatchData created with query
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        query="SELECT * FROM test_table",
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def execution_engine_with_mini_taxi_selectable(
    sa, execution_engine_with_mini_taxi_loaded
):
    execution_engine = execution_engine_with_mini_taxi_loaded
    # BatchData created with Selectable
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=sa.select(sa.text("*")).select_from(sa.table("test_table")),
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.mark.parametrize(
    "execution_engine_fixture_name",
    [
        # fixture name passed as string
        ("execution_engine_with_mini_taxi_table_name"),
        ("execution_engine_with_mini_taxi_query"),
        ("execution_engine_with_mini_taxi_selectable"),
    ],
)
def test_column_map_condition_values_with_able(
    execution_engine_fixture_name, request, sa
):
    execution_engine = request.getfixturevalue(execution_engine_fixture_name)
    metric_domain_kwargs = {
        "batch_id": "__",
        "table": "test_table",
        "column": "total_amount",
        "row_condition": 'col("pk_1")!=0',
        "condition_parser": "great_expectations__experimental__",
    }

    metric_value_kwargs = {
        "min_value": 0.01,
        "max_value": 100.0,
        "strict_min": False,
        "strict_max": False,
        "parse_strings_as_datetimes": False,
        "allow_cross_type_comparisons": None,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    table_columns_metric, table_column_metrics_results = get_table_columns_metric(
        execution_engine=execution_engine
    )
    desired_metric = MetricConfiguration(
        metric_name="column_values.between.condition",
        metric_domain_kwargs={"column": "total_amount"},
        metric_value_kwargs={"min_value": 0.0, "max_value": 10.0},
    )
    desired_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = execution_engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    metrics = dict()
    metrics = {
        "unexpected_condition": results[desired_metric.id],
        "table.columns": table_column_metrics_results[table_columns_metric.id],
    }
    mp = MapMetricProvider()
    res = _sqlalchemy_column_map_condition_values(
        cls=mp,
        execution_engine=execution_engine,
        metric_domain_kwargs=metric_domain_kwargs,
        metric_value_kwargs=metric_value_kwargs,
        metrics=metrics,
    )
    # one value is out of the range
    assert res == [14.8]
