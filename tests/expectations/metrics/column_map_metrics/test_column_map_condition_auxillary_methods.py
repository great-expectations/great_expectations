from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytest

from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.expectations.metrics import (
    MapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.column_map_condition_auxilliary_methods import (
    _spark_column_map_condition_values,
    _sqlalchemy_column_map_condition_values,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture
def mini_taxi_df() -> pd.DataFrame:
    """
    Returns: pandas dataframe that contains a small selection of columns and rows from taxi_data, for unittesting.
    """
    df = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4],
            "vendor_id": [1, 1, 1, 1, 1],
            "pickup_datetime": [
                "2019-01-15 3:36:12",
                "2019-01-25 18:20:32",
                "2019-01-05 6:47:31",
                "2019-01-09 15:08:02",
                "2019-01-25 18:49:51",
            ],
            "dropoff_datetime": [
                "2019-01-15 3:42:19",
                "2019-01-25 18:26:55",
                "2019-01-05 6:52:19",
                "2019-01-09 15:20:17",
                "2019-01-25 18:56:44",
            ],
            "trip_distance": [1, 0.8, 1.1, 2.5, 0.8],
            "tip_amount": [1.95, 1.55, 0, 3, 1.65],
            "total_amount": [9.75, 9.35, 6.8, 14.8, 9.95],
        }
    )
    return df


@pytest.fixture
def sql_execution_engine_with_mini_taxi_loaded(sa, mini_taxi_df):
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
def spark_execution_engine_with_mini_taxi_loaded(spark_session, mini_taxi_df):
    conf: List[tuple] = spark_session.sparkContext.getConf().getAll()
    spark_config: Dict[str, Any] = dict(conf)

    pandas_df = mini_taxi_df
    spark_df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in pandas_df.to_records(index=False)
        ],
        pandas_df.columns.tolist(),
    )
    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(
        spark_config=spark_config,
    )
    execution_engine.load_batch_data(batch_id="1234", batch_data=spark_df)
    return execution_engine


@pytest.fixture
def sql_execution_engine_with_mini_taxi_table_name(
    sa, sql_execution_engine_with_mini_taxi_loaded
):
    execution_engine = sql_execution_engine_with_mini_taxi_loaded
    # BatchData created with `table_name`
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        table_name="test_table",
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def sql_execution_engine_with_mini_taxi_query(
    sa, sql_execution_engine_with_mini_taxi_loaded
):
    execution_engine = sql_execution_engine_with_mini_taxi_loaded
    # BatchData created with query
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        query="SELECT * FROM test_table",
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


@pytest.fixture
def sql_execution_engine_with_mini_taxi_selectable(
    sa, sql_execution_engine_with_mini_taxi_loaded
):
    execution_engine = sql_execution_engine_with_mini_taxi_loaded
    # BatchData created with Selectable
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine,
        selectable=sa.select(sa.text("*")).select_from(sa.table("test_table")),
    )
    execution_engine.load_batch_data("__", batch_data)
    return execution_engine


# We calculate the column_values.between.condition with min value 0.0 and max value 10.0.
# when row_condition is col("pk_1")!=0 _sqlalchemy_column_map_condition_values() method will return [14.8] because it will run against all rows of mini_taxi_df and find the total_amount values that out of range (0 < x < 10.0).
# when row_condition is col("pk_1")==0 _sqlalchemy_column_map_condition_values() method will return [] because it will only run against a single row of mini_taxi_df (where pk_1==0), and that total_amount value is within our range (9.75).
@pytest.mark.sqlite
@pytest.mark.parametrize(
    "execution_engine_fixture_name",
    (
        "sql_execution_engine_with_mini_taxi_table_name",
        "sql_execution_engine_with_mini_taxi_query",
        "sql_execution_engine_with_mini_taxi_selectable",
    ),
)
@pytest.mark.parametrize(
    "metric_domain_kwargs, expected_result",
    [
        (
            {
                "column": "total_amount",
                "row_condition": 'col("pk_1")==0',
                "condition_parser": "great_expectations__experimental__",
            },
            [],
        ),
        (
            {
                "column": "total_amount",
                "row_condition": 'col("pk_1")!=0',
                "condition_parser": "great_expectations__experimental__",
            },
            [14.8],
        ),
    ],
)
def test_sqlalchemy_column_map_condition_values(
    execution_engine_fixture_name, metric_domain_kwargs, expected_result, request, sa
):
    execution_engine = request.getfixturevalue(execution_engine_fixture_name)
    metric_value_kwargs = {
        "min_value": 0,
        "max_value": 10.0,
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

    desired_metric = MetricConfiguration(
        metric_name="column_values.between.condition",
        metric_domain_kwargs=metric_domain_kwargs,
        metric_value_kwargs=metric_value_kwargs,
    )

    # table.columns metric has to be calculated and loaded first, because it is a dependency of the `column_values.between.condition` metric.
    table_columns_metric, table_column_metrics_results = get_table_columns_metric(
        execution_engine=execution_engine
    )
    desired_metric.metric_dependencies = {"table.columns": table_columns_metric}

    results = execution_engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
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
    # one value is out of range with row condition
    assert res == expected_result


@pytest.mark.spark
@pytest.mark.parametrize(
    "execution_engine_fixture_name, metric_domain_kwargs, expected_result",
    [
        (
            "spark_execution_engine_with_mini_taxi_loaded",
            {
                "column": "total_amount",
                "row_condition": 'col("pk_1")==0',
                "condition_parser": "great_expectations__experimental__",
            },
            [],
        ),
        (
            "spark_execution_engine_with_mini_taxi_loaded",
            {
                "column": "total_amount",
                "row_condition": 'col("pk_1")>=0',
                "condition_parser": "great_expectations__experimental__",
            },
            [14.8],
        ),
    ],
)
def test_spark_column_map_condition_values(
    execution_engine_fixture_name,
    metric_domain_kwargs,
    expected_result,
    request,
    spark_session,
):
    execution_engine = request.getfixturevalue(execution_engine_fixture_name)
    metric_value_kwargs = {
        "min_value": 0,
        "max_value": 10.0,
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

    desired_metric = MetricConfiguration(
        metric_name="column_values.between.condition",
        metric_domain_kwargs=metric_domain_kwargs,
        metric_value_kwargs=metric_value_kwargs,
    )

    # table.columns metric has to be calculated and loaded first, because it is a dependency of the `column_values.between.condition` metric.
    table_columns_metric, table_column_metrics_results = get_table_columns_metric(
        execution_engine=execution_engine
    )
    desired_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = execution_engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    metrics = {
        "unexpected_condition": results[desired_metric.id],
        "table.columns": table_column_metrics_results[table_columns_metric.id],
    }
    mp = MapMetricProvider()
    res = _spark_column_map_condition_values(
        cls=mp,
        execution_engine=execution_engine,
        metric_domain_kwargs=metric_domain_kwargs,
        metric_value_kwargs=metric_value_kwargs,
        metrics=metrics,
    )
    # one value is out of range with row condition
    assert res == expected_result
