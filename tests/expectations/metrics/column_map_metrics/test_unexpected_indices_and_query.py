from typing import Dict, Tuple

import pandas as pd
import pytest

from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.exceptions import MetricResolutionError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.self_check.util import (
    build_pandas_engine,
    build_sa_execution_engine,
    build_spark_engine,
)
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture
def animal_table_df() -> pd.DataFrame:
    """
    Returns: pandas dataframe that contains example data for unexpected_index_column_names metric tests
    """
    df = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )
    return df


@pytest.fixture
def metric_value_kwargs_complete() -> dict:
    """
    Test configuration for metric_value_kwargs. Contains `unexpected_index_column_names` key
    """
    return {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }


def _build_table_columns_and_unexpected(
    engine, metric_value_kwargs
) -> Tuple[MetricConfiguration, MetricConfiguration, dict]:
    """
    Helper method build the dependencies needed for unexpected_indices (ID/PK) related metrics.

    The unexpected_indices related metrics are dependent on :
        - unexpected_condition
        - table_columns

    This method takes in the engine (connected to data), and metric value kwargs dictionary to build the metrics
    and return as a tuple


    Args:
        engine (ExecutionEngine): has connection to data
        metric_value_kwargs (dict): can be

    Returns:

        Tuple with MetricConfigurations corresponding to unexpected_condition and table_columns metric, as well as metrics dict.

    """
    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    # get table_columns_metric
    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    # unexpected_condition metric
    unexpected_condition_metric: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_condition_metric,), metrics=metrics
    )

    return table_columns_metric, unexpected_condition_metric, metrics


@pytest.mark.unit
def test_pd_unexpected_index_list_metric_without_id_pk(animal_table_df):
    df: pd.DataFrame = animal_table_df
    # pandas will return default unexpected_index_list without id_pk
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    engine: PandasExecutionEngine = build_pandas_engine(df=df)
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    for key, val in results.items():
        assert val == [3, 4, 5]


@pytest.mark.unit
def test_pd_unexpected_index_list_metric_with_id_pk(
    metric_value_kwargs_complete, animal_table_df
):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = metric_value_kwargs_complete

    engine: PandasExecutionEngine = build_pandas_engine(df=df)
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)

    unexpected_index_list: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_list.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_list,), metrics=metrics
    )
    for key, val in results.items():
        assert val == [
            {"animals": "giraffe", "pk_1": 3},
            {"animals": "lion", "pk_1": 4},
            {"animals": "zebra", "pk_1": 5},
        ]


@pytest.mark.unit
def test_sa_unexpected_index_list_metric_with_id_pk(
    sa, animal_table_df, metric_value_kwargs_complete
):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = metric_value_kwargs_complete

    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df=df, sa=sa)
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)

    unexpected_index_list = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_list.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_list,), metrics=metrics
    )
    for key, val in results.items():
        assert val == [
            {"animals": "giraffe", "pk_1": 3},
            {"animals": "lion", "pk_1": 4},
            {"animals": "zebra", "pk_1": 5},
        ]


@pytest.mark.unit
def test_sa_unexpected_index_list_metric_without_id_pk(sa, animal_table_df):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df=df, sa=sa)
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)

    unexpected_index_list = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_list.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_list,), metrics=metrics
    )
    assert list(results.values())[0] is None


@pytest.mark.unit
def test_sa_unexpected_index_query_metric_with_id_pk(
    sa, animal_table_df, metric_value_kwargs_complete
):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = metric_value_kwargs_complete

    engine = build_sa_execution_engine(df=df, sa=sa)
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for key, val in results.items():
        assert (
            val == "SELECT pk_1, animals \n"
            "FROM test \n"
            "WHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'));"
        )


@pytest.mark.unit
def test_sa_unexpected_index_query_metric_without_id_pk(sa, animal_table_df):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(df=df, sa=sa)

    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for key, val in results.items():
        assert (
            val == "SELECT animals \n"
            "FROM test \n"
            "WHERE animals IS NOT NULL AND (animals NOT IN ('cat', 'fish', 'dog'));"
        )


@pytest.mark.integration
def test_spark_unexpected_index_list_metric_with_id_pk(
    spark_session, animal_table_df, metric_value_kwargs_complete
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session, df=animal_table_df, batch_id="my_id"
    )

    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)

    unexpected_index_list = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs_complete,
    )
    unexpected_index_list.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_list,), metrics=metrics
    )
    for val in results.values():
        assert val == [
            {"animals": "giraffe", "pk_1": 3},
            {"animals": "lion", "pk_1": 4},
            {"animals": "zebra", "pk_1": 5},
        ]


@pytest.mark.integration
def test_spark_unexpected_index_list_metric_without_id_pk(
    spark_session, animal_table_df
):
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session, df=animal_table_df, batch_id="my_id"
    )
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)

    unexpected_index_list = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_list.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_list,), metrics=metrics
    )
    assert list(results.values())[0] is None


@pytest.mark.integration
def test_pd_unexpected_index_query_metric_with_id_pk(
    animal_table_df, metric_value_kwargs_complete
):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = metric_value_kwargs_complete
    engine: PandasExecutionEngine = build_pandas_engine(df=df)

    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for val in results.values():
        assert val == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.integration
def test_pd_unexpected_index_query_metric_without_id_pk(
    animal_table_df,
):
    df: pd.DataFrame = animal_table_df
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }
    engine: PandasExecutionEngine = build_pandas_engine(df=df)

    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for val in results.values():
        assert val == "df.filter(items=[3, 4, 5], axis=0)"


@pytest.mark.integration
def test_spark_unexpected_index_query_metric_with_id_pk(
    spark_session, animal_table_df, metric_value_kwargs_complete
):
    metric_value_kwargs: dict = metric_value_kwargs_complete

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session, df=animal_table_df, batch_id="my_id"
    )
    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs_complete)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for val in results.values():
        assert (
            val
            == "df.filter(F.expr((animals IS NOT NULL) AND (NOT (animals IN (cat, fish, dog)))))"
        )


@pytest.mark.integration
def test_spark_unexpected_index_query_metric_without_id_pk(
    spark_session, animal_table_df, metric_value_kwargs_complete
):
    metric_value_kwargs: dict = {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session, df=animal_table_df, batch_id="my_id"
    )

    (
        table_columns_metric,
        unexpected_columns_metric,
        metrics,
    ) = _build_table_columns_and_unexpected(engine, metric_value_kwargs)
    unexpected_index_query: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_index_query.metric_dependencies = {
        "unexpected_condition": unexpected_columns_metric,
        "table.columns": table_columns_metric,
    }
    results: Dict[Tuple[str, str, str], MetricValue] = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_index_query,), metrics=metrics
    )
    for val in results.values():
        assert (
            val
            == "df.filter(F.expr((animals IS NOT NULL) AND (NOT (animals IN (cat, fish, dog)))))"
        )
