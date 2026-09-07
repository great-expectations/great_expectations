import copy
import datetime
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, Union

import numpy as np
import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import pyspark, sqlalchemy
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core.batch import Batch
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypes,
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.self_check.util import (
    build_pandas_engine,
    build_sa_execution_engine,
    build_spark_engine,
)
from great_expectations.util import isclose
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,
    MetricConfigurationID,
)
from tests.expectations.test_util import get_table_columns_metric

if TYPE_CHECKING:
    from great_expectations.validator.computed_metric import MetricValue


@pytest.mark.unit
def test_metric_loads_pd():
    assert get_metric_provider("column.max", PandasExecutionEngine()) is not None


@pytest.mark.big
def test_basic_metric_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 3}


@pytest.mark.unit
@pytest.mark.parametrize(
    "build_engine,dataframe,expected_result",
    [
        [
            build_pandas_engine,
            pd.DataFrame({"a": [1, 2, 3, None]}),
            6,
        ],
        [
            build_pandas_engine,
            pd.DataFrame({"a": [Decimal("2.0"), Decimal("0.18781")]}),
            2.18781,
        ],
    ],
)
def test_column_sum_metric_pd(build_engine, dataframe, expected_result):
    engine = build_engine(df=dataframe)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.sum",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: expected_result}


@pytest.mark.spark
@pytest.mark.parametrize(
    "dataframe,expected_result",
    [
        [
            pd.DataFrame({"a": [1, 2, 3, None]}),
            6,
        ],
        [
            pd.DataFrame({"a": [Decimal("2.0"), Decimal("0.18781")]}),
            2.18781,
        ],
    ],
)
def test_column_sum_metric_spark(spark_session, dataframe, expected_result):
    engine = build_spark_engine(spark=spark_session, df=dataframe, batch_id="my_id")

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.sum.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column.sum",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: expected_result}


def _resolve_spark_column_sum(engine, column="a"):
    """Resolve the ``column.sum`` metric for a Spark engine and return its value."""
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.sum.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": column},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column.sum",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {"metric_partial_fn": aggregate_fn_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)
    return results[desired_metric.id]


@pytest.mark.spark
def test_column_sum_metric_spark_longtype_preserves_integer_type(spark_session):
    # A genuine LongType, no-null column must keep an *integer* observed value on
    # Spark 3, byte-identical to prior behavior. Spark 4 evaluates the Long
    # accumulator under ANSI semantics and raises ARITHMETIC_OVERFLOW on overflow
    # (Spark 3 silently wraps), so integral inputs are widened to DoubleType before
    # summing there and the value legitimately surfaces as a float.
    schema = pyspark.types.StructType(
        [pyspark.types.StructField("a", pyspark.types.LongType(), nullable=False)]
    )
    spark_df = spark_session.createDataFrame([(1,), (2,), (3,)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    observed_value = _resolve_spark_column_sum(engine)

    assert observed_value == 6
    if pyspark.pyspark and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0"):
        assert isinstance(observed_value, float)
    else:
        assert isinstance(observed_value, int) and not isinstance(observed_value, bool)


@pytest.mark.spark
def test_column_sum_metric_spark_longtype_overflow_does_not_raise(spark_session):
    # Long-overflow-prone integral input: on Spark 4 the double-widening keeps the
    # ANSI-mode aggregate from raising ARITHMETIC_OVERFLOW and yields the true
    # (non-wrapped) magnitude; Spark 3 silently wraps the Long accumulator. Either
    # way the computation must complete without raising.
    max_long = 9223372036854775807
    schema = pyspark.types.StructType(
        [pyspark.types.StructField("a", pyspark.types.LongType(), nullable=False)]
    )
    spark_df = spark_session.createDataFrame([(max_long,), (max_long,)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    observed_value = _resolve_spark_column_sum(engine)

    if pyspark.pyspark and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0"):
        # Widened to double, so no overflow and the true magnitude is preserved.
        assert observed_value == pytest.approx(float(2 * max_long))
    else:
        # Spark 3 wraps the Long accumulator rather than raising; assert it completed.
        assert observed_value is not None


@pytest.mark.big
@pytest.mark.parametrize(
    "dataframe,expected_result",
    [
        [pd.DataFrame({"a": [1, 2, 3, None]}), 2],
        [
            pd.DataFrame({"a": [Decimal("2.0"), Decimal("0.18781")]}),
            1.093905,
        ],
    ],
)
def test_column_mean_metric_pd(dataframe, expected_result):
    engine = build_pandas_engine(dataframe)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: expected_result}


@pytest.mark.spark
@pytest.mark.parametrize(
    "dataframe,expected_result",
    [
        [pd.DataFrame({"a": [1, 2, 3, None]}), 2],
        [
            pd.DataFrame({"a": [Decimal("2.0"), Decimal("0.18781")]}),
            1.093905,
        ],
    ],
)
def test_column_mean_metric_spark(spark_session, dataframe, expected_result):
    engine = build_spark_engine(spark=spark_session, df=dataframe, batch_id="my_id")

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.mean.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: expected_result}


@pytest.mark.big
@pytest.mark.parametrize(
    "build_engine,dataframe,expected_result",
    [
        [
            build_pandas_engine,
            pd.DataFrame({"a": [1, 2, 3, None]}),
            1,
        ],
        [
            build_pandas_engine,
            pd.DataFrame({"a": [Decimal("2.0"), Decimal("0.18781")]}),
            1.2814118377984496,
        ],
    ],
)
def test_column_standard_deviation_metric_pd(build_engine, dataframe, expected_result):
    engine = build_engine(df=dataframe)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: expected_result}


@pytest.mark.unit
def test_column_value_lengths_min_metric_pd():
    engine = build_pandas_engine(
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
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.min",
        metric_domain_kwargs={"column": "names"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 8}


@pytest.mark.sqlite
def test_column_quoted_name_type_sa(sa):
    engine = build_sa_execution_engine(
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
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_columns_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.columns",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    table_columns_metric_id: MetricConfigurationID = table_columns_metric.id
    batch_column_list = metrics[table_columns_metric_id]

    column_name: str
    quoted_batch_column_list = [
        sqlalchemy.quoted_name(value=str(column_name), quote=True)
        for column_name in batch_column_list
    ]

    column_name = "names"

    str_column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=batch_column_list,
    )
    assert isinstance(str_column_name, str)

    quoted_column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=quoted_batch_column_list,
    )
    assert sqlalchemy.quoted_name and isinstance(quoted_column_name, sqlalchemy.quoted_name)
    assert quoted_column_name.quote is True

    for column_name in [
        "non_existent_column",
        "?NAMES?",
        "*Names*",
    ]:
        with pytest.raises(gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError) as eee:
            _ = get_dbms_compatible_column_names(
                column_names=column_name,
                batch_columns_list=batch_column_list,
            )
        assert str(eee.value) == f'Error: The column "{column_name}" in BatchData does not exist.'


@pytest.mark.unit
def test_column_quoted_name_type_sa_handles_explicit_string_identifiers(sa):
    """
    Within SQLite, identifiers can be quoted using one of the following mechanisms:
    'keyword'		A keyword in single quotes is a string literal.
    "keyword"		A keyword in double-quotes is an identifier.
    [keyword]		A keyword enclosed in square brackets is an identifier. This is not standard SQL.
                    This quoting mechanism is used by MS Access and SQL Server and is included in SQLite for compatibility.
    `keyword`		A keyword enclosed in grave accents (ASCII code 96) is an identifier. This is not standard SQL.
                    This quoting mechanism is used by MySQL and is included in SQLite for compatibility.

    When explicit quoted identifiers are passed in, we should use them as-is.
    Explicit identifiers are used when the column contains a space or reserved word.
    """  # noqa: E501 # FIXME CoP
    engine = build_sa_execution_engine(
        pd.DataFrame(
            {
                "More Names": [
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
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_columns_metric: MetricConfiguration = MetricConfiguration(
        metric_name="table.columns",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    table_columns_metric_id: MetricConfigurationID = table_columns_metric.id
    batch_column_list = metrics[table_columns_metric_id]

    for column_name in [
        '"More Names"',
        "[More Names]",
        "`More Names`",
    ]:
        str_column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=batch_column_list,
        )
        assert isinstance(str_column_name, str)
        assert str_column_name == column_name


@pytest.mark.sqlite
def test_column_value_lengths_min_metric_sa(sa):
    engine = build_sa_execution_engine(
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
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.min",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 8}


@pytest.mark.spark
def test_column_value_lengths_min_metric_spark(spark_session):
    engine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
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
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.min",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 8}


@pytest.mark.big
def test_column_value_lengths_max_metric_pd():
    engine = build_pandas_engine(
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
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.max",
        metric_domain_kwargs={"column": "names"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 16}


@pytest.mark.sqlite
def test_column_value_lengths_max_metric_sa(sa):
    engine = build_sa_execution_engine(
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
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.max",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 16}


@pytest.mark.spark
def test_column_value_lengths_max_metric_spark(spark_session):
    engine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
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
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.length.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "names",
        },
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.length.max",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 16}


@pytest.mark.unit
def test_quantiles_metric_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 4]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.quantile_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
            "allow_relative_error": "linear",
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1.75, 2.5, 3.25]}


@pytest.mark.sqlite
def test_quantiles_metric_sa(sa):
    engine = build_sa_execution_engine(pd.DataFrame({"a": [1, 2, 3, 4]}), sa)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )

    results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
    metrics.update(results)

    table_row_count_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    table_row_count_metric.metric_dependencies = {
        "metric_partial_fn": partial_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(table_row_count_metric,), metrics=metrics)
    metrics.update(results)

    column_values_null_condition_metric = MetricConfiguration(
        metric_name=f"column_values.null.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_null_condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_null_condition_metric,), metrics=metrics
    )
    metrics.update(results)

    column_values_nonnull_count_metric = MetricConfiguration(
        metric_name=f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_nonnull_count_metric.metric_dependencies = {
        "unexpected_condition": column_values_null_condition_metric,
        "metric_partial_fn": partial_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_nonnull_count_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.quantile_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.row_count": table_row_count_metric,
        "column_values.nonnull.count": column_values_nonnull_count_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1.0, 2.0, 3.0]}


@pytest.mark.spark
def test_quantiles_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [1, 2, 3, 4]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.quantile_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "quantiles": [2.5e-1, 5.0e-1, 7.5e-1],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1.0, 2.0, 3.0]}


@pytest.mark.unit
def test_column_histogram_metric_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                ]
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": [0.0, 0.9, 1.8, 2.7, 3.6, 4.5, 5.4, 6.3, 7.2, 8.1, 9.0],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}


@pytest.mark.sqlite
def test_column_histogram_metric_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                ],
                "b": [
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ],
            }
        ),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": [0.0, 0.9, 1.8, 2.7, 3.6, 4.5, 5.4, 6.3, 7.2, 8.1, 9.0],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}

    desired_metric = MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={
            "bins": [0.0],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [10]}


@pytest.mark.spark
def test_column_histogram_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                ]
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.histogram",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": [0.0, 0.9, 1.8, 2.7, 3.6, 4.5, 5.4, 6.3, 7.2, 8.1, 9.0],
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}


@pytest.mark.big
def test_column_partition_metric_pd():
    """
    Test of "column.partition" metric for both, standard numeric column and "datetime.datetime" valued column.

    The "column.partition" metric depends on "column.max" metric and on "column.max" metric.

    For "PandasExecutionEngine", explicit values of these metrics are needed.

    For standard numerical data, test set contains 12 evenly spaced integers.
    For "datetime.datetime" data, test set contains 12 dates, starting with January 1, 2021, separated by 7 days.

    Expected partition boundaries are pre-computed algorithmically and asserted to be "close" to actual metric values.
    """  # noqa: E501 # FIXME CoP
    week_idx: int
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                ],
                "b": [
                    datetime.datetime(2021, 1, 1, 0, 0, 0) + datetime.timedelta(days=(week_idx * 7))  # noqa: DTZ001 # FIXME CoP
                    for week_idx in range(12)
                ],
            },
        ),
    )

    seconds_in_week: int = 604800

    n_bins: int = 10

    increment: Union[float, datetime.timedelta]
    idx: int
    element: Union[float, pd.Timestamp]

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    # Test using standard numeric column.

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
    )
    column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
    )
    column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = float(n_bins + 1) / n_bins
    assert all(
        isclose(operand_a=element, operand_b=(increment * idx))
        for idx, element in enumerate(results[desired_metric.id])
    )

    # Test using "datetime.datetime" column.

    metrics = {}
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
    )
    column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
    )
    column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = datetime.timedelta(seconds=(seconds_in_week * float(n_bins + 1) / n_bins))
    assert all(
        isclose(
            operand_a=element.to_pydatetime(),
            operand_b=(datetime.datetime(2021, 1, 1, 0, 0, 0) + (increment * idx)),  # noqa: DTZ001 # FIXME CoP
        )
        for idx, element in enumerate(results[desired_metric.id])
    )


@pytest.mark.sqlite
def test_column_partition_metric_sa(sa):  # noqa: PLR0915 # FIXME CoP
    """
    Test of "column.partition" metric for both, standard numeric column and "datetime.datetime" valued column.

    The "column.partition" metric depends on "column.max" metric and on "column.max" metric.

    For "SqlAlchemyExecutionEngine", explicit values of these metrics are needed, each requiring a "metric_partial_fn",
    corresponding to "column.min.aggregate_fn" metric and "column.max.aggregate_fn" metric, respectively, resolved.

    For standard numerical data, test set contains 12 evenly spaced integers.
    For "datetime.datetime" data, test set contains 12 dates, starting with January 1, 2021, separated by 7 days.

    Expected partition boundaries are pre-computed algorithmically and asserted to be "close" to actual metric values.
    """  # noqa: E501 # FIXME CoP
    week_idx: int
    engine = build_sa_execution_engine(
        pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                ],
                "b": [
                    datetime.datetime(2021, 1, 1, 0, 0, 0) + datetime.timedelta(days=(week_idx * 7))  # noqa: DTZ001 # FIXME CoP
                    for week_idx in range(12)
                ],
            },
        ),
        sa,
    )

    seconds_in_week: int = 604800

    n_bins: int = 10

    increment: Union[float, datetime.timedelta]
    idx: int
    element: Union[float, pd.Timestamp]

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    # Test using standard numeric column.

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_column_min_metric = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    partial_column_max_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            partial_column_min_metric,
            partial_column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
    )
    column_min_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_min_metric,
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
    )
    column_max_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_max_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = float(n_bins + 1) / n_bins
    assert all(
        isclose(operand_a=element, operand_b=(increment * idx))
        for idx, element in enumerate(results[desired_metric.id])
    )

    # Test using "datetime.datetime" column.

    metrics = {}
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_column_min_metric = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    partial_column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    partial_column_max_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    partial_column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            partial_column_min_metric,
            partial_column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
    )
    column_min_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_min_metric,
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
    )
    column_max_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_max_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = datetime.timedelta(seconds=(seconds_in_week * float(n_bins + 1) / n_bins))
    assert all(
        isclose(
            operand_a=element,
            operand_b=(datetime.datetime(2021, 1, 1, 0, 0, 0) + (increment * idx)),  # noqa: DTZ001 # FIXME CoP
        )
        for idx, element in enumerate(results[desired_metric.id])
    )


@pytest.mark.spark
def test_column_partition_metric_spark(spark_session):  # noqa: PLR0915 # FIXME CoP
    """
    Test of "column.partition" metric for both, standard numeric column and "datetime.datetime" valued column.

    The "column.partition" metric depends on "column.max" metric and on "column.max" metric.

    For "SparkDFExecutionEngine", explicit values of these metrics are needed, each requiring a "metric_partial_fn",
    corresponding to "column.min.aggregate_fn" metric and "column.max.aggregate_fn" metric, respectively, resolved.

    For standard numerical data, test set contains 12 evenly spaced integers.
    For "datetime.datetime" data, test set contains 12 dates, starting with January 1, 2021, separated by 7 days.

    Expected partition boundaries are pre-computed algorithmically and asserted to be "close" to actual metric values.
    """  # noqa: E501 # FIXME CoP
    week_idx: int
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                ],
                "b": [
                    datetime.datetime(2021, 1, 1, 0, 0, 0) + datetime.timedelta(days=(week_idx * 7))  # noqa: DTZ001 # FIXME CoP
                    for week_idx in range(12)
                ],
            },
        ),
        schema=pyspark.types.StructType(
            [
                pyspark.types.StructField("a", pyspark.types.IntegerType(), True),
                pyspark.types.StructField("b", pyspark.types.TimestampType(), True),
            ]
        ),
        batch_id="my_id",
    )

    seconds_in_week: int = 604800

    n_bins: int = 10

    increment: Union[float, datetime.timedelta]
    idx: int
    element: Union[float, pd.Timestamp]

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    # Test using standard numeric column.

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_column_min_metric = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    partial_column_max_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            partial_column_min_metric,
            partial_column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
    )
    column_min_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_min_metric,
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
    )
    column_max_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_max_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = float(n_bins + 1) / n_bins
    assert all(
        isclose(operand_a=element, operand_b=(increment * idx))
        for idx, element in enumerate(results[desired_metric.id])
    )

    # Test using "datetime.datetime" column.

    metrics = {}
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_column_min_metric = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    partial_column_min_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    partial_column_max_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    partial_column_max_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            partial_column_min_metric,
            partial_column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    column_min_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
    )
    column_min_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_min_metric,
        "table.columns": table_columns_metric,
    }
    column_max_metric: MetricConfiguration = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
    )
    column_max_metric.metric_dependencies = {
        "metric_partial_fn": partial_column_max_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_min_metric,
            column_max_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "column.min": column_min_metric,
        "column.max": column_max_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    increment = datetime.timedelta(seconds=(seconds_in_week * float(n_bins + 1) / n_bins))
    assert all(
        isclose(
            operand_a=element,
            operand_b=(datetime.datetime(2021, 1, 1, 0, 0, 0) + (increment * idx)),  # noqa: DTZ001 # FIXME CoP
        )
        for idx, element in enumerate(results[desired_metric.id])
    )


@pytest.mark.unit
def test_max_metric_column_exists_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 3}


@pytest.mark.unit
def test_max_metric_column_does_not_exist_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
        metrics.update(results)
    assert str(eee.value) == 'Error: The column "non_existent_column" in BatchData does not exist.'


@pytest.mark.sqlite
def test_max_metric_column_exists_sa(sa):
    engine = build_sa_execution_engine(pd.DataFrame({"a": [1, 2, 1, None]}), sa)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": partial_metric,
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 2}


@pytest.mark.sqlite
def test_max_metric_column_does_not_exist_sa(sa):
    engine = build_sa_execution_engine(pd.DataFrame({"a": [1, 2, 1, None]}), sa)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    partial_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
        metrics.update(results)
    assert 'Error: The column "non_existent_column" in BatchData does not exist.' in str(eee.value)


@pytest.mark.spark
def test_max_metric_column_exists_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [1, 2, 1]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    partial_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": partial_metric,
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 2}


@pytest.mark.spark
def test_max_metric_column_does_not_exist_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [1, 2, 1]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    partial_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
        metrics.update(results)
    assert str(eee.value) == 'Error: The column "non_existent_column" in BatchData does not exist.'


@pytest.mark.sqlite
def test_map_value_set_sa(sa):
    engine = build_sa_execution_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}), sa)

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    aggregate_partial.metric_dependencies = {
        "unexpected_condition": desired_metric,
    }

    metrics = engine.resolve_metrics(metrics_to_resolve=(aggregate_partial,), metrics=metrics)
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_partial,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert results == {desired_metric.id: 0}


@pytest.mark.sqlite
def test_map_of_type_sa(sa):
    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    add_dataframe_to_db(df=df, name="test", con=eng, index=False)
    batch_data = SqlAlchemyBatchData(
        execution_engine=eng, table_name="test", source_table_name="test"
    )
    engine = SqlAlchemyExecutionEngine(engine=eng, batch_data_dict={"my_id": batch_data})
    desired_metric = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # noinspection PyTypeChecker
    assert results[desired_metric.id][0]["name"] == "a"
    # noinspection PyTypeChecker
    assert isinstance(results[desired_metric.id][0]["type"], sa.FLOAT)


@pytest.mark.spark
def test_map_value_set_spark(spark_session, basic_spark_df_execution_engine):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 3, None]},
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs={
            "value_set": [1, 2, 3],
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs={
            "value_set": [1, 2, 3],
        },
    )
    aggregate_partial.metric_dependencies = {
        "unexpected_condition": condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_partial,), metrics=metrics)
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs={
            "value_set": [1, 2, 3],
        },
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_partial,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 0}

    # We run the same computation again, this time with None being replaced by nan instead of NULL
    # to demonstrate this behavior
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df = spark_session.createDataFrame(df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="my_id", batch_data=df)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    aggregate_partial.metric_dependencies = {
        "unexpected_condition": condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_partial,), metrics=metrics)
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_partial,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 1}


@pytest.mark.unit
def test_map_column_value_lengths_between_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": ["a", "aaa", "bcbc", "defgh", None]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.value_length.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)

    result_series, _, _ = results[desired_metric.id]

    ser_expected_lengths = pd.Series([1, 3, 4, 5])
    assert ser_expected_lengths.equals(result_series)


@pytest.mark.filterwarnings(
    "ignore:pandas.Int64Index is deprecated*:FutureWarning:tests.expectations.metrics"
)
@pytest.mark.big
def test_map_column_values_increasing_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "a": [
                    1,
                    2,
                    4,
                    5,
                    3,
                    6,
                    7,
                ]
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "strictly": True,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert list(metrics[condition_metric.id][0]) == [
        False,
        False,
        False,
        False,
        True,
        False,
        False,
    ]
    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 1}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id]["a"].index == pd.Index([4], dtype="int64")


@pytest.mark.spark
def test_map_column_values_increasing_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 3.0, 4.5, 6, None, 3],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_column_types = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={
            "include_nested": True,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(table_column_types,),
        metrics=metrics,
    )
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "strictly": True,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.column_types": table_column_types,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.increasing.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 1}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 3},
    ]


def _resolve_strictly_monotonic_unexpected_count(engine, metric_stem: str):
    """Resolve the unexpected count for a strictly increasing/decreasing metric."""
    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_column_types = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={"include_nested": True},
    )
    results = engine.resolve_metrics(metrics_to_resolve=(table_column_types,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"{metric_stem}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"strictly": True},
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.column_types": table_column_types,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"{metric_stem}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)
    return metrics[unexpected_count_metric.id]


@pytest.mark.spark
def test_map_column_values_increasing_spark_large_longs(spark_session):
    # Adjacent Long values above 2**53 that differ by 1 cannot be distinguished once
    # widened to DoubleType (their difference collapses to 0), which would wrongly flag a
    # strictly-increasing column as having an unexpected (non-increasing) row. An exact
    # wide DECIMAL preserves the true difference, so no rows are unexpected on either
    # Spark version.
    schema = pyspark.types.StructType(
        [pyspark.types.StructField("a", pyspark.types.LongType(), nullable=False)]
    )
    spark_df = spark_session.createDataFrame([(2**60,), (2**60 + 1,)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    unexpected_count = _resolve_strictly_monotonic_unexpected_count(
        engine, "column_values.increasing"
    )

    assert unexpected_count == 0


@pytest.mark.spark
def test_map_column_values_decreasing_spark_large_longs(spark_session):
    # Mirror of the increasing case: adjacent Long values above 2**53 differing by 1 must
    # keep their exact difference so a strictly-decreasing column is not wrongly flagged.
    schema = pyspark.types.StructType(
        [pyspark.types.StructField("a", pyspark.types.LongType(), nullable=False)]
    )
    spark_df = spark_session.createDataFrame([(2**60 + 1,), (2**60,)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    unexpected_count = _resolve_strictly_monotonic_unexpected_count(
        engine, "column_values.decreasing"
    )

    assert unexpected_count == 0


@pytest.mark.spark
def test_map_column_values_between_spark_string_column_string_bounds(spark_session):
    # Comparing a string column against string bounds is a valid same-family comparison
    # (e.g. ISO-date or version-string ranges) and must not be rejected as an incomparable
    # type. It stays valid under Spark 4 ANSI as well, so no rows are unexpected on either
    # Spark version.
    schema = pyspark.types.StructType(
        [pyspark.types.StructField("s", pyspark.types.StringType(), nullable=False)]
    )
    spark_df = spark_session.createDataFrame([("a",), ("b",), ("c",), ("d",)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_column_types = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={"include_nested": True},
    )
    results = engine.resolve_metrics(metrics_to_resolve=(table_column_types,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.between.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "s"},
        metric_value_kwargs={
            "min_value": "a",
            "max_value": "z",
            "strict_min": False,
            "strict_max": False,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.column_types": table_column_types,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.between.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "s"},
        metric_value_kwargs=None,
    )
    aggregate_fn_metric.metric_dependencies = {"unexpected_condition": condition_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.between.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "s"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {"metric_partial_fn": aggregate_fn_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 0


@pytest.mark.big
@pytest.mark.filterwarnings(
    "ignore:pandas.Int64Index is deprecated*:FutureWarning:tests.expectations.metrics"
)
def test_map_column_values_decreasing_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "a": [
                    7,
                    6,
                    3,
                    5,
                    4,
                    2,
                    1,
                ]
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "strictly": True,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert list(metrics[condition_metric.id][0]) == [
        False,
        False,
        False,
        True,
        False,
        False,
        False,
    ]
    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 1}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id]["a"].index == pd.Index([3], dtype="int64")


@pytest.mark.spark
def test_map_column_values_decreasing_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [3, None, 6, 4.5, 3.0, 2, 1],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    table_column_types = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={
            "include_nested": True,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(table_column_types,),
        metrics=metrics,
    )
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "strictly": True,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.column_types": table_column_types,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.decreasing.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 1}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [{"a": 6}]


@pytest.mark.big
def test_map_unique_column_exists_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 3, 4, None]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert list(metrics[condition_metric.id][0]) == [False, False, True, True, False]
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 1}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id]["a"].index == [2]
    assert metrics[unexpected_rows_metric.id]["a"].values == [3]


@pytest.mark.unit
def test_map_unique_column_does_not_exist_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert str(eee.value) == 'Error: The column "non_existent_column" in BatchData does not exist.'


@pytest.mark.sqlite
def test_map_unique_column_exists_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 3, 3, None], "b": ["foo", "bar", "baz", "qux", "fish"]}),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    count_per_value_metric = MetricConfiguration(
        metric_name=f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    count_per_value_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(count_per_value_metric,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}": count_per_value_metric,  # noqa: E501 # metric name exceeds line length
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,),
        metrics=metrics,  # metrics=aggregate_fn_metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert results[desired_metric.id] == [(3, 2)]

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == [{"a": 3, "b": "baz"}, {"a": 3, "b": "qux"}]


@pytest.mark.sqlite
def test_map_unique_column_does_not_exist_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 3, 3, None], "b": ["foo", "bar", "baz", "qux", "fish"]}),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    assert 'Error: The column "non_existent_column" in BatchData does not exist.' in str(eee.value)


@pytest.mark.sqlite
def test_map_unique_empty_query_sa(sa):
    """If the table contains zero rows then there must be zero unexpected values."""
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [], "b": []}),
        sa,
    )

    table_columns_metric: MetricConfiguration
    metrics: dict
    table_columns_metric, metrics = get_table_columns_metric(execution_engine=engine)

    count_per_value_metric = MetricConfiguration(
        metric_name=f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    count_per_value_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(count_per_value_metric,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}": count_per_value_metric,  # noqa: E501 # metric name exceeds line length
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,),
        metrics=metrics,
    )
    assert results[desired_metric.id] == 0


@pytest.mark.sqlite
def test_map_unique_unexpected_count_sql_shape_sa(sa):
    """Regression test for Redshift WLM "low_timeout" fixes.

    The single most common path on SQLAlchemy is "unexpected_count" (BASIC
    result_format). The generated SQL must:

      * scan the source table exactly once (no "col NOT IN (dup_subquery)"
        semi-join pattern, which double-scans on column-store backends),
      * carry only the target column through the window operator (the inner
        windowed subquery must NOT project arbitrary table columns, because
        that materializes every column — including JSON/SUPER fields on
        Redshift — through the partition sort and was observed to trip the
        WLM "low_timeout" rule even after the double-scan was removed).
    """
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 3, 3, None], "b": ["x", "y", "z", "z", "w"]}),
        sa,
    )

    executed_sql: list[str] = []

    @sa.event.listens_for(engine.engine, "before_cursor_execute")
    def capture_sql(conn, cursor, statement, parameters, context, executemany):
        executed_sql.append(statement)

    table_columns_metric: MetricConfiguration
    metrics: dict
    table_columns_metric, metrics = get_table_columns_metric(execution_engine=engine)

    count_per_value_metric = MetricConfiguration(
        metric_name=f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    count_per_value_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(count_per_value_metric,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}": count_per_value_metric,  # noqa: E501 # metric name exceeds line length
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    assert results[unexpected_count_metric.id] == 2

    combined = " ".join(executed_sql)
    combined_upper = combined.upper()

    # Single-pass: no semi-join double-scan.
    assert "NOT IN" not in combined_upper, (
        "Duplicate detection must not rely on a NOT IN (dup_subquery) pattern "
        "(double-scans source on column-store DBs like Redshift)."
    )

    # Window must still be used for the count path (single scan, no GROUP BY
    # collapse, framework's SUM(CASE) wrapper relies on one row per source row).
    assert "PARTITION BY" in combined_upper, (
        f"Expected windowed unique check for unexpected_count, got: {executed_sql}"
    )

    # Narrow projection: only target column "a" must appear inside the windowed
    # subquery. Column "b" must NOT be projected through the window operator —
    # that was the wide-row failure mode that kept tripping WLM "low_timeout".
    assert ", b," not in combined and ", b " not in combined and ', "b"' not in combined, (
        "Inner windowed subquery must project only the target column; carrying "
        "extra source columns through the window operator re-introduces the "
        "wide-row sort that trips Redshift WLM `low_timeout`. "
        f"Got SQL: {executed_sql}"
    )


@pytest.mark.sqlite
def test_map_unique_unexpected_rows_join_back_sa(sa):
    """The "unexpected_rows" path must hydrate full source rows by joining a
    narrow dup-keys aggregate back to the source — never by widening the
    windowed subquery to carry every source column.
    """
    engine = build_sa_execution_engine(
        pd.DataFrame(
            {
                "a": [1, 2, 3, 3, None],
                "b": ["x", "y", "z1", "z2", "w"],
                "c": [10, 20, 30, 31, 40],
            }
        ),
        sa,
    )

    executed_sql: list[str] = []

    @sa.event.listens_for(engine.engine, "before_cursor_execute")
    def capture_sql(conn, cursor, statement, parameters, context, executemany):
        executed_sql.append(statement)

    table_columns_metric: MetricConfiguration
    metrics: dict
    table_columns_metric, metrics = get_table_columns_metric(execution_engine=engine)

    count_per_value_metric = MetricConfiguration(
        metric_name=f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    count_per_value_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(count_per_value_metric,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"column_values.count_per_value.{MetricPartialFunctionTypeSuffixes.MAP.value}": count_per_value_metric,  # noqa: E501 # metric name exceeds line length
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_rows_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "COMPLETE", "partial_unexpected_count": 20}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)

    rows = results[unexpected_rows_metric.id]
    duplicated_a = sorted(r["a"] for r in rows)
    assert duplicated_a == [3, 3]

    combined_upper = " ".join(executed_sql).upper()
    assert "GROUP BY" in combined_upper and "HAVING" in combined_upper, (
        "unexpected_rows must use a narrow GROUP BY/HAVING dup-keys subquery "
        f"joined back to source; got: {executed_sql}"
    )


@pytest.mark.spark
def test_map_unique_column_exists_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 3, 3, 4, None],
                "b": [None, "foo", "bar", "baz", "qux", "fish"],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    # unique is a *window* function so does not use the aggregate_fn version of unexpected count
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == [(3, 2)]

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
        metric_domain_kwargs={
            "column": "a",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == [{"a": 3, "b": "bar"}, {"a": 3, "b": "baz"}]


@pytest.mark.spark
def test_map_unique_column_does_not_exist_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 3, 3, 4, None],
                "b": [None, "foo", "bar", "baz", "qux", "fish"],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.unique.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    with pytest.raises(gx_exceptions.MetricResolutionError) as eee:
        # noinspection PyUnusedLocal
        metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    assert str(eee.value) == 'Error: The column "non_existent_column" in BatchData does not exist.'


@pytest.mark.big
def test_z_score_under_threshold_pd():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    mean.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    stdev.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(metrics_to_resolve=desired_metrics, metrics=metrics)
    metrics.update(results)

    column_values_z_score_map_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_z_score_map_metric.metric_dependencies = {
        "column.standard_deviation": stdev,
        "column.mean": mean,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_z_score_map_metric,), metrics=metrics
    )
    metrics.update(results)
    column_values_z_score_under_threshold_condition_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    column_values_z_score_under_threshold_condition_metric.metric_dependencies = {
        f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}": column_values_z_score_map_metric,  # noqa: E501 # FIXME CoP
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_z_score_under_threshold_condition_metric,),
        metrics=metrics,
    )
    assert list(results[column_values_z_score_under_threshold_condition_metric.id][0]) == [
        False,
        False,
        False,
    ]
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": column_values_z_score_under_threshold_condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert results[desired_metric.id] == 0


@pytest.mark.spark
def test_z_score_under_threshold_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 3, None]},
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_mean_aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.mean.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_mean_aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    column_standard_deviation_aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.standard_deviation.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_standard_deviation_aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metrics = (
        column_mean_aggregate_fn_metric,
        column_standard_deviation_aggregate_fn_metric,
    )
    results = engine.resolve_metrics(metrics_to_resolve=desired_metrics, metrics=metrics)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    mean.metric_dependencies = {
        "metric_partial_fn": column_mean_aggregate_fn_metric,
    }
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    stdev.metric_dependencies = {
        "metric_partial_fn": column_standard_deviation_aggregate_fn_metric,
        "table.columns": table_columns_metric,
    }
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(metrics_to_resolve=desired_metrics, metrics=metrics)
    metrics.update(results)

    column_values_z_score_map_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_z_score_map_metric.metric_dependencies = {
        "column.standard_deviation": stdev,
        "column.mean": mean,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_z_score_map_metric,), metrics=metrics
    )
    metrics.update(results)
    condition_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    condition_metric.metric_dependencies = {
        f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}": column_values_z_score_map_metric,  # noqa: E501 # FIXME CoP
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    aggregate_fn_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,), metrics=metrics)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert results[desired_metric.id] == 0


@pytest.mark.spark
def test_z_score_under_threshold_spark_constant_column(spark_session):
    # A constant column has a standard deviation of zero. Dividing by it would yield
    # Infinity (or raise under ANSI arithmetic), so the z-score must resolve to null for
    # every row -- producing no unexpected values and no error on either Spark version.
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [5, 5, 5, 5]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_mean_aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.mean.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_mean_aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    column_standard_deviation_aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column.standard_deviation.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_standard_deviation_aggregate_fn_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            column_mean_aggregate_fn_metric,
            column_standard_deviation_aggregate_fn_metric,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    mean.metric_dependencies = {"metric_partial_fn": column_mean_aggregate_fn_metric}
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    stdev.metric_dependencies = {
        "metric_partial_fn": column_standard_deviation_aggregate_fn_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(mean, stdev), metrics=metrics)
    metrics.update(results)

    # Confirm the constant column really does have a zero standard deviation.
    assert metrics[stdev.id] == 0

    z_score_map_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    z_score_map_metric.metric_dependencies = {
        "column.standard_deviation": stdev,
        "column.mean": mean,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(z_score_map_metric,), metrics=metrics)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    condition_metric.metric_dependencies = {
        f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}": z_score_map_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    aggregate_fn_metric.metric_dependencies = {"unexpected_condition": condition_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    unexpected_count_metric.metric_dependencies = {"metric_partial_fn": aggregate_fn_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)

    assert results[unexpected_count_metric.id] == 0


@pytest.mark.unit
def test_table_metric_pd(caplog):
    df = pd.DataFrame({"a": [1, 2, 3, 3, None], "b": [1, 2, 3, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 5}
    assert (
        'Unexpected key(s) "column" found in domain_kwargs for Domain type "table"' in caplog.text
    )


@pytest.mark.big
def test_map_column_pairs_equal_metric_pd():  # noqa: PLR0915 # FIXME CoP
    engine = build_pandas_engine(
        pd.DataFrame(
            data={
                "a": [0, 1, 9, 2],
                "b": [5, 4, 3, 6],
                "c": [5, 4, 3, 6],
                "d": [7, 8, 9, 0],
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "column_pair_values.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [False, False, False, False]
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].empty
    assert len(metrics[unexpected_rows_metric.id].columns) == 4

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [True, True, False, True]
    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].equals(
        pd.DataFrame(
            data={"a": [0, 1, 2], "b": [5, 4, 6], "c": [5, 4, 6], "d": [7, 8, 0]},
            index=pd.Index([0, 1, 3]),
        )
    )
    assert len(metrics[unexpected_rows_metric.id].columns) == 4
    pd.testing.assert_index_equal(metrics[unexpected_rows_metric.id].index, pd.Index([0, 1, 3]))

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3
    assert metrics[unexpected_values_metric.id] == [(0, 7), (1, 8), (2, 0)]


@pytest.mark.sqlite
def test_table_metric_sa(sa):
    engine = build_sa_execution_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}), sa)

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 6}


@pytest.mark.sqlite
def test_map_column_pairs_equal_metric_sa(sa):  # noqa: PLR0915 # FIXME CoP
    engine = build_sa_execution_engine(
        pd.DataFrame(
            data={
                "a": [0, 1, 9, 2],
                "b": [5, 4, 3, 6],
                "c": [5, 4, 3, 6],
                "d": [7, 8, 9, 0],
            }
        ),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "column_pair_values.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert len(metrics[unexpected_rows_metric.id]) == 0

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 0, "b": 5, "c": 5, "d": 7},
        {"a": 1, "b": 4, "c": 4, "d": 8},
        {"a": 2, "b": 6, "c": 6, "d": 0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3
    assert metrics[unexpected_values_metric.id] == [(0, 7), (1, 8), (2, 0)]


@pytest.mark.spark
def test_map_column_pairs_equal_metric_spark(spark_session):  # noqa: PLR0915 # FIXME CoP
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            data={
                "a": [0, 1, 9, 2],
                "b": [5, 4, 3, 6],
                "c": [5, 4, 3, 6],
                "d": [7, 8, 9, 0],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "column_pair_values.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert len(metrics[unexpected_rows_metric.id]) == 0

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "b",
            "column_B": "c",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 0, "b": 5, "c": 5, "d": 7},
        {"a": 1, "b": 4, "c": 4, "d": 8},
        {"a": 2, "b": 6, "c": 6, "d": 0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "d",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3
    assert metrics[unexpected_values_metric.id] == [(0, 7), (1, 8), (2, 0)]


@pytest.mark.big
def test_map_column_pairs_greater_metric_pd():
    df = pd.DataFrame({"a": [2, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert (
        results[condition_metric.id][0]
        .reset_index(drop=True)
        .equals(pd.Series([False, False, False, False]))
    )

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []


@pytest.mark.sqlite
def test_map_column_pairs_greater_metric_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame(
            data={
                "a": [2, 3, 4, None, 3, None],
                "b": [1, 2, 3, None, 3, 5],
            }
        ),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []


@pytest.mark.spark
def test_map_column_pairs_greater_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            data={
                "a": [2, 3, 4, None, 3, None],
                "b": [1, 2, 3, None, 3, 5],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.a_greater_than_b.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "or_equal": True,
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []


@pytest.mark.unit
def test_map_column_pairs_in_set_metric_pd():
    df = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert (
        results[condition_metric.id][0]
        .reset_index(drop=True)
        .equals(pd.Series([True, False, False, False]))
    )


@pytest.mark.sqlite
def test_map_column_pairs_in_set_metric_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [10, 9, 3, 4, None, 3, None], "b": [1, 4, 2, 3, None, 3, 5]}),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert results[unexpected_values_metric.id] == [(10, 1), (9, 4)]

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "both_values_are_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert results[unexpected_values_metric.id] == [
        (10.0, 1.0),
        (9.0, 4.0),
        (None, 5.0),
    ]


@pytest.mark.spark
def test_map_column_pairs_in_set_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [10, 9, 3, 4, None, 3, None], "b": [1, 4, 2, 3, None, 3, 5]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert results[unexpected_values_metric.id] == [(10, 1), (9, 4)]

    condition_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "both_values_are_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_values_metric = MetricConfiguration(
        metric_name=f"column_pair_values.in_set.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
        metric_domain_kwargs={
            "column_A": "a",
            "column_B": "b",
            "ignore_row_if": "either_value_is_missing",
        },
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "result_format": {
                "result_format": "SUMMARY",
                "partial_unexpected_count": 6,
            },
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    assert results[unexpected_values_metric.id] == [
        (10.0, 1.0),
        (9.0, 4.0),
        (None, 5.0),
    ]


@pytest.mark.spark
def test_table_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1]},
        ),
        batch_id="my_id",
    )

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    results = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=results)

    assert results == {desired_metric.id: 3}


@pytest.mark.unit
def test_column_median_metric_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {"a": [1, 2, 3]},
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: 2}


@pytest.mark.unit
@pytest.mark.parametrize(
    "dataframe,median,",
    [
        pytest.param(
            pd.DataFrame({"a": [1, 2, 3]}),
            2,
        ),
        pytest.param(
            pd.DataFrame({"a": [1]}),
            1,
        ),
    ],
)
def test_column_median_metric_sa(sa, dataframe: pd.DataFrame, median: int):
    engine = build_sa_execution_engine(
        dataframe,
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )

    results = engine.resolve_metrics(metrics_to_resolve=(partial_metric,), metrics=metrics)
    metrics.update(results)

    table_row_count_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    table_row_count_metric.metric_dependencies = {
        "metric_partial_fn": partial_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(table_row_count_metric,), metrics=metrics)
    metrics.update(results)

    column_values_null_condition_metric = MetricConfiguration(
        metric_name=f"column_values.null.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_null_condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_null_condition_metric,), metrics=metrics
    )
    metrics.update(results)

    column_values_nonnull_count_metric = MetricConfiguration(
        metric_name=f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_values_nonnull_count_metric.metric_dependencies = {
        "unexpected_condition": column_values_null_condition_metric,
        "metric_partial_fn": partial_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(column_values_nonnull_count_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "table.row_count": table_row_count_metric,
        "column_values.nonnull.count": column_values_nonnull_count_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results == {desired_metric.id: median}


@pytest.mark.spark
def test_column_median_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3]},
        ),
        batch_id="my_id",
    )

    aggregate_fn_metric = MetricConfiguration(
        metric_name=f"table.row_count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    metrics = engine.resolve_metrics(metrics_to_resolve=(aggregate_fn_metric,))

    row_count = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=None,
    )
    row_count.metric_dependencies = {
        "metric_partial_fn": aggregate_fn_metric,
    }
    metrics = engine.resolve_metrics(metrics_to_resolve=(row_count,), metrics=metrics)

    desired_metric = MetricConfiguration(
        metric_name="column.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "table.row_count": row_count,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    assert results == {desired_metric.id: 2}


@pytest.mark.big
def test_value_counts_metric_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert pd.Series(index=[1, 2, 3], data=[2, 2, 2]).equals(metrics[desired_metric.id])


@pytest.mark.big
def test_value_counts_metric_sa(sa):
    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )
    desired_metric_b = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric, desired_metric_b))
    assert pd.Series(
        index=pd.Index(data=[1, 2, 3], name="value"),
        data=[2, 2, 2],
    ).equals(metrics[desired_metric.id])
    assert pd.Series(
        index=pd.Index(data=[4], name="value"),
        data=[6],
    ).equals(metrics[desired_metric_b.id])


@pytest.mark.spark
def test_value_counts_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 1, 2, 3, 3, None],
                "b": [None, None, None, None, None, None, None],
            },
        ),
        schema=pyspark.types.StructType(
            [
                pyspark.types.StructField("a", pyspark.types.FloatType(), True),
                pyspark.types.StructField("b", pyspark.types.NullType(), True),
            ]
        ),
        batch_id="my_id",
    )

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert pd.Series(index=[1.0, 2.0, 3.0, np.nan], data=[2, 2, 2, 1]).equals(
        metrics[desired_metric.id]
    )

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert pd.Series(index=[], data=[]).equals(metrics[desired_metric.id])


@pytest.mark.spark
@pytest.mark.parametrize(
    "dataframe",
    [
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3, None]}),
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3, None], "b": [1, 3, 5, 3, 4, 2, None]}),
    ],
)
def test_distinct_metric_spark(
    spark_session,
    dataframe,
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=dataframe,
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_distinct_values_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)
    assert metrics[column_distinct_values_metric.id] == {1, 2, 3}

    column_distinct_values_count_metric_partial_fn = MetricConfiguration(
        metric_name=f"column.distinct_values.count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_count_metric_partial_fn.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_metric_partial_fn,),
        metrics=metrics,
    )
    metrics.update(results)
    assert pyspark.Column and isinstance(
        metrics[column_distinct_values_count_metric_partial_fn.id][0],
        pyspark.Column,
    )

    column_distinct_values_count_metric = MetricConfiguration(
        metric_name="column.distinct_values.count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_count_metric.metric_dependencies = {
        "metric_partial_fn": column_distinct_values_count_metric_partial_fn
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_metric,), metrics=metrics
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_metric.id] == 3

    column_distinct_values_count_threshold_metric = MetricConfiguration(
        metric_name="column.distinct_values.count.under_threshold",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"threshold": 5},
    )
    column_distinct_values_count_threshold_metric.metric_dependencies = {
        "column.distinct_values.count": column_distinct_values_count_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_threshold_metric,),
        metrics=metrics,
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_threshold_metric.id] is True


@pytest.mark.big
def test_distinct_metric_sa(
    sa,
):
    engine: SqlAlchemyExecutionEngine = build_sa_execution_engine(
        pd.DataFrame(
            {
                "a": [1, 2, 1, 2, 3, 3, None],
            }
        ),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_distinct_values_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_metric,),
        metrics=metrics,
    )
    metrics.update(results)
    assert metrics[column_distinct_values_metric.id] == {1, 2, 3}

    column_distinct_values_count_metric_partial_fn = MetricConfiguration(
        metric_name=f"column.distinct_values.count.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_count_metric_partial_fn.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_metric_partial_fn,),
        metrics=metrics,
    )
    metrics.update(results)
    assert isinstance(
        metrics[column_distinct_values_count_metric_partial_fn.id][0],
        sa.sql.functions.count,
    )

    column_distinct_values_count_metric = MetricConfiguration(
        metric_name="column.distinct_values.count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_count_metric.metric_dependencies = {
        "metric_partial_fn": column_distinct_values_count_metric_partial_fn
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_metric,), metrics=metrics
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_metric.id] == 3

    column_distinct_values_count_threshold_metric = MetricConfiguration(
        metric_name="column.distinct_values.count.under_threshold",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"threshold": 5},
    )
    column_distinct_values_count_threshold_metric.metric_dependencies = {
        "column.distinct_values.count": column_distinct_values_count_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_threshold_metric,),
        metrics=metrics,
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_threshold_metric.id] is True


@pytest.mark.big
def test_distinct_metric_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}))

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    column_distinct_values_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_metric,), metrics=metrics
    )
    metrics.update(results)
    assert metrics[column_distinct_values_metric.id] == {1, 2, 3}

    column_distinct_values_count_metric = MetricConfiguration(
        metric_name="column.distinct_values.count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    column_distinct_values_count_metric.metric_dependencies = {
        "column.distinct_values": column_distinct_values_metric,
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_metric,), metrics=metrics
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_metric.id] == 3

    column_distinct_values_count_threshold_metric = MetricConfiguration(
        metric_name="column.distinct_values.count.under_threshold",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"threshold": 5},
    )
    column_distinct_values_count_threshold_metric.metric_dependencies = {
        "column.distinct_values.count": column_distinct_values_count_metric,
        "table.columns": table_columns_metric,
    }

    results = engine.resolve_metrics(
        metrics_to_resolve=(column_distinct_values_count_threshold_metric,),
        metrics=metrics,
    )
    metrics.update(results)
    assert metrics[column_distinct_values_count_threshold_metric.id] is True


@pytest.mark.big
def test_batch_aggregate_metrics_pd():
    import datetime

    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "a": [
                    "2021-01-01",
                    "2021-01-31",
                    "2021-02-28",
                    "2021-03-20",
                    "2021-02-21",
                    "2021-05-01",
                    "2021-06-18",
                ],
                "b": [
                    "2021-06-18",
                    "2021-05-01",
                    "2021-02-21",
                    "2021-03-20",
                    "2021-02-28",
                    "2021-01-31",
                    "2021-01-01",
                ],
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
    )
    desired_metric_1.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
    )
    desired_metric_2.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
    )
    desired_metric_3.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
    )
    desired_metric_4.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    start = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
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
    end = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
    print(end - start)
    assert results[desired_metric_1.id] == "2021-06-18"
    assert results[desired_metric_2.id] == "2021-01-01"
    assert results[desired_metric_3.id] == "2021-06-18"
    assert results[desired_metric_4.id] == "2021-01-01"


@pytest.mark.sqlite
def test_batch_aggregate_metrics_sa(caplog, sa):
    import datetime

    engine = build_sa_execution_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_aggregate_fn_metric_1 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_1.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_2 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_2.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_3 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_3.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_4 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_4.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_aggregate_fn_metric_1,
            desired_aggregate_fn_metric_2,
            desired_aggregate_fn_metric_3,
            desired_aggregate_fn_metric_4,
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
        "metric_partial_fn": desired_aggregate_fn_metric_1,
        "table.columns": table_columns_metric,
    }
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_2.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_2,
        "table.columns": table_columns_metric,
    }
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_3.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_3,
        "table.columns": table_columns_metric,
    }
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_4,
        "table.columns": table_columns_metric,
    }
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    start = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
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
    end = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
    print("t1")
    print(end - start)
    assert results[desired_metric_1.id] == 3
    assert results[desired_metric_2.id] == 1
    assert results[desired_metric_3.id] == 4
    assert results[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if record.message == "SqlAlchemyExecutionEngine computed 4 metrics on domain_id ()":
            found_message = True
    assert found_message


@pytest.mark.spark
def test_batch_aggregate_metrics_spark(caplog, spark_session):
    import datetime

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]},
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    desired_aggregate_fn_metric_1 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_1.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_2 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_2.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_3 = MetricConfiguration(
        metric_name=f"column.max.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_3.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_aggregate_fn_metric_4 = MetricConfiguration(
        metric_name=f"column.min.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_aggregate_fn_metric_4.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_aggregate_fn_metric_1,
            desired_aggregate_fn_metric_2,
            desired_aggregate_fn_metric_3,
            desired_aggregate_fn_metric_4,
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
        "metric_partial_fn": desired_aggregate_fn_metric_1,
    }
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric_2.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_2,
    }
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_3.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_3,
    }
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )
    desired_metric_4.metric_dependencies = {
        "metric_partial_fn": desired_aggregate_fn_metric_4,
    }
    start = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
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
    end = datetime.datetime.now()  # noqa: DTZ005 # FIXME CoP
    print(end - start)
    assert results[desired_metric_1.id] == 3
    assert results[desired_metric_2.id] == 1
    assert results[desired_metric_3.id] == 4
    assert results[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if record.message == "SparkDFExecutionEngine computed 4 metrics on domain_id ()":
            found_message = True
    assert found_message


@pytest.mark.big
def test_map_multicolumn_sum_equal_pd():  # noqa: PLR0915 # FIXME CoP
    engine = build_pandas_engine(
        pd.DataFrame(data={"a": [0, 1, 2], "b": [5, 4, 3], "c": [0, 0, 1], "d": [7, 8, 9]})
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "multicolumn_sum.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [False, False, False]
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].empty
    assert len(metrics[unexpected_rows_metric.id].columns) == 4

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [False, False, True]
    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].equals(
        pd.DataFrame(data={"a": [2], "b": [3], "c": [1], "d": [9]}, index=[2])
    )
    assert len(metrics[unexpected_rows_metric.id].columns) == 4
    pd.testing.assert_index_equal(metrics[unexpected_rows_metric.id].index, pd.Index([2]))

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 1
    assert metrics[unexpected_values_metric.id] == [{"a": 2, "b": 3, "c": 1}]


@pytest.mark.sqlite
def test_map_multicolumn_sum_equal_sa(sa):  # noqa: PLR0915 # FIXME CoP
    engine = build_sa_execution_engine(
        pd.DataFrame(data={"a": [0, 1, 2], "b": [5, 4, 3], "c": [0, 0, 1], "d": [7, 8, 9]}),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "multicolumn_sum.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).
    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert len(metrics[unexpected_rows_metric.id]) == 0

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [{"a": 2, "b": 3, "c": 1, "d": 9}]
    assert len(metrics[unexpected_rows_metric.id][0]) == 4

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 1
    assert metrics[unexpected_values_metric.id] == [{"a": 2, "b": 3, "c": 1}]


@pytest.mark.spark
def test_map_multicolumn_sum_equal_spark(spark_session):  # noqa: PLR0915 # FIXME CoP
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(data={"a": [0, 1, 2], "b": [5, 4, 3], "c": [0, 0, 1], "d": [7, 8, 9]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no unexpected rows.
    2. Fail -- one or more unexpected rows.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "multicolumn_sum.equal"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).
    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert len(metrics[unexpected_rows_metric.id]) == 0

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "sum_total": 5,
        },
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 1

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [{"a": 2, "b": 3, "c": 1, "d": 9}]
    assert len(metrics[unexpected_rows_metric.id][0]) == 4

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 1
    assert metrics[unexpected_values_metric.id] == [{"a": 2, "b": 3, "c": 1}]


def _resolve_multicolumn_sum_equal_unexpected_count(engine, column_list, sum_total):
    """Resolve the unexpected count for multicolumn_sum.equal over the given columns."""
    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=f"multicolumn_sum.equal.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column_list": column_list},
        metric_value_kwargs={"sum_total": sum_total},
    )
    condition_metric.metric_dependencies = {"table.columns": table_columns_metric}
    results = engine.resolve_metrics(metrics_to_resolve=(condition_metric,), metrics=metrics)
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=f"multicolumn_sum.equal.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column_list": column_list},
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)
    return metrics[unexpected_count_metric.id]


@pytest.mark.spark
def test_map_multicolumn_sum_equal_spark_large_longs(spark_session):
    # Summing large integral operands must not overflow the Long accumulator. Under Spark
    # 4 ANSI arithmetic an overflowing Long sum raises ARITHMETIC_OVERFLOW; widening
    # integral operands to an exact wide DECIMAL lets the equality resolve correctly. On
    # Spark 3 the original Long arithmetic wraps around instead (its historical behavior),
    # so the check is only asserted exact on Spark 4.
    schema = pyspark.types.StructType(
        [
            pyspark.types.StructField("a", pyspark.types.LongType(), nullable=False),
            pyspark.types.StructField("b", pyspark.types.LongType(), nullable=False),
        ]
    )
    spark_df = spark_session.createDataFrame([(2**62, 2**62)], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    unexpected_count = _resolve_multicolumn_sum_equal_unexpected_count(
        engine, ["a", "b"], Decimal(2**63)
    )

    if pyspark.pyspark and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0"):
        # Widened to DECIMAL: the sum equals 2**63 exactly and does not raise.
        assert unexpected_count == 0
    else:
        # Spark 3 wraps the Long accumulator rather than raising; assert it completed.
        assert isinstance(unexpected_count, int)


@pytest.mark.spark
def test_map_multicolumn_sum_equal_spark_decimal_operands(spark_session):
    # Decimal operands must be summed exactly. Widening them to DOUBLE would reintroduce
    # binary floating error (0.1 + 0.2 != 0.3), wrongly flagging the row as unexpected.
    # Leaving decimals uncast keeps the equality exact on both Spark versions.
    schema = pyspark.types.StructType(
        [
            pyspark.types.StructField("a", pyspark.types.DecimalType(2, 1), nullable=False),
            pyspark.types.StructField("b", pyspark.types.DecimalType(2, 1), nullable=False),
        ]
    )
    spark_df = spark_session.createDataFrame([(Decimal("0.1"), Decimal("0.2"))], schema=schema)
    engine = build_spark_engine(spark=spark_session, df=spark_df, batch_id="my_id")

    unexpected_count = _resolve_multicolumn_sum_equal_unexpected_count(
        engine, ["a", "b"], Decimal("0.3")
    )

    assert unexpected_count == 0


@pytest.mark.big
def test_map_compound_columns_unique_pd():  # noqa: PLR0915 # FIXME CoP
    engine = build_pandas_engine(
        pd.DataFrame(data={"a": [0, 1, 1], "b": [1, 2, 3], "c": [0, 2, 2]})
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no duplicated compound column keys.
    2. Fail -- one or more duplicated compound column keys.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "compound_columns.unique"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [False, False, False]
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].empty
    assert len(metrics[unexpected_rows_metric.id].columns) == 3

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [False, True, True]
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].equals(
        pd.DataFrame(data={"a": [1, 1], "b": [2, 3], "c": [2, 2]}, index=[1, 2])
    )
    assert len(metrics[unexpected_rows_metric.id].columns) == 3
    pd.testing.assert_index_equal(metrics[unexpected_rows_metric.id].index, pd.Index([1, 2]))

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [{"a": 1, "c": 2}, {"a": 1, "c": 2}]


@pytest.mark.sqlite
def test_map_compound_columns_unique_sa(sa):  # noqa: PLR0915 # FIXME CoP
    engine = build_sa_execution_engine(
        pd.DataFrame(data={"a": [0, 1, 1], "b": [1, 2, 3], "c": [0, 2, 2]}),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no duplicated compound column keys.
    2. Fail -- one or more duplicated compound column keys.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    prerequisite_function_metric_name: str = (
        f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}"
    )

    prerequisite_function_metric = MetricConfiguration(
        metric_name=prerequisite_function_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    prerequisite_function_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(prerequisite_function_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    metric_name: str = "compound_columns.unique"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}": prerequisite_function_metric,  # noqa: E501 # FIXME CoP
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return SQLAlchemy ColumnElement object.
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert len(metrics[unexpected_rows_metric.id]) == 0

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    prerequisite_function_metric = MetricConfiguration(
        metric_name=prerequisite_function_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    prerequisite_function_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(prerequisite_function_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        f"compound_columns.count.{MetricPartialFunctionTypeSuffixes.MAP.value}": prerequisite_function_metric,  # noqa: E501 # FIXME CoP
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return SQLAlchemy ColumnElement object.
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1, "b": 2, "c": 2},
        {"a": 1, "b": 3, "c": 2},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [{"a": 1, "c": 2}, {"a": 1, "c": 2}]


@pytest.mark.spark
def test_map_compound_columns_unique_spark(spark_session):  # noqa: PLR0915 # FIXME CoP
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(data={"a": [0, 1, 1], "b": [1, 2, 3], "c": [0, 2, 2]}),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    """
    Two tests:
    1. Pass -- no duplicated compound column keys.
    2. Fail -- one or more duplicated compound column keys.
    """

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "compound_columns.unique"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    # First, assert Pass (no unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 0

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == []

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 0
    assert metrics[unexpected_values_metric.id] == []

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    # Second, assert Fail (one or more unexpected results).

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1, "b": 2, "c": 2},
        {"a": 1, "b": 3, "c": 2},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "c"],
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [{"a": 1, "c": 2}, {"a": 1, "c": 2}]


@pytest.mark.big
def test_map_select_column_values_unique_within_record_pd():  # noqa: PLR0915 # FIXME CoP
    engine = build_pandas_engine(
        pd.DataFrame(
            data={
                "a": [1, 1, 8, 1, 4, None, None, 7],
                "b": [1, 2, 2, 2, 4, None, None, 1],
                "c": [2, 3, 7, 3, 4, None, 9, 0],
            }
        )
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "select_column_values.unique.within_record"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [
        True,
        False,
        False,
        False,
        True,
        True,
        False,
    ]
    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 8}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].equals(
        pd.DataFrame(
            data={"a": [1.0, 4.0, None], "b": [1.0, 4.0, None], "c": [2.0, 4.0, 9.0]},
            index=[0, 4, 6],
        )
    )
    assert len(metrics[unexpected_rows_metric.id].columns) == 3
    pd.testing.assert_index_equal(metrics[unexpected_rows_metric.id].index, pd.Index([0, 4, 6]))

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3

    unexpected_values = []
    for unexpected_value_dict in metrics[unexpected_values_metric.id]:
        updated_unexpected_value_dict = {
            key: "NULL" if np.isnan(value) else value
            for key, value in unexpected_value_dict.items()
        }
        unexpected_values.append(updated_unexpected_value_dict)

    assert unexpected_values == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
        {"a": "NULL", "b": "NULL", "c": 9.0},
    ]

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert list(metrics[condition_metric.id][0]) == [
        True,
        False,
        False,
        False,
        True,
        False,
    ]
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id].equals(
        pd.DataFrame(data={"a": [1.0, 4.0], "b": [1.0, 4.0], "c": [2.0, 4.0]}, index=[0, 4])
    )
    assert len(metrics[unexpected_rows_metric.id].columns) == 3
    pd.testing.assert_index_equal(metrics[unexpected_rows_metric.id].index, pd.Index([0, 4]))

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
    ]


@pytest.mark.sqlite
def test_map_select_column_values_unique_within_record_sa(sa):  # noqa: PLR0915 # FIXME CoP
    engine = build_sa_execution_engine(
        pd.DataFrame(
            data={
                "a": [1, 1, 8, 1, 4, None, None, 7],
                "b": [1, 2, 2, 2, 4, None, None, 1],
                "c": [2, 3, 7, 3, 4, None, 9, 0],
            }
        ),
        sa,
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "select_column_values.unique.within_record"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 8}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
        {"a": None, "b": None, "c": 9.0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3

    assert metrics[unexpected_values_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
        {"a": None, "b": None, "c": 9.0},
    ]

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
    ]


@pytest.mark.spark
def test_map_select_column_values_unique_within_record_spark(  # noqa: PLR0915 # 56
    spark_session,
):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            data={
                "a": [1, 1, 8, 1, 4, None, None, 7],
                "b": [1, 2, 2, 2, 4, None, None, 1],
                "c": [2, 3, 7, 3, 4, None, 9, 0],
            }
        ),
        batch_id="my_id",
    )

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    # Save original metrics for testing unexpected results.
    metrics_save: dict = copy.deepcopy(metrics)

    metric_name: str = "select_column_values.unique.within_record"
    condition_metric_name: str = (
        f"{metric_name}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"
    )
    unexpected_count_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
    )
    unexpected_rows_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}"
    )
    unexpected_values_metric_name: str = (
        f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
    )

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 3

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 8}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
        {"a": None, "b": None, "c": 9.0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "all_values_are_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 3

    unexpected_values = []
    for unexpected_value_dict in metrics[unexpected_values_metric.id]:
        updated_unexpected_value_dict = {
            key: "NULL" if value is None else value for key, value in unexpected_value_dict.items()
        }
        unexpected_values.append(updated_unexpected_value_dict)

    assert unexpected_values == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
        {"a": "NULL", "b": "NULL", "c": 9.0},
    ]

    # Restore from saved original metrics in order to start fresh on testing for unexpected results.
    metrics = copy.deepcopy(metrics_save)

    condition_metric = MetricConfiguration(
        metric_name=condition_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,),
        metrics=metrics,
    )
    metrics.update(results)

    unexpected_count_metric = MetricConfiguration(
        metric_name=unexpected_count_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs=None,
    )
    unexpected_count_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_count_metric,), metrics=metrics)
    metrics.update(results)

    # Condition metrics return "negative logic" series.
    assert metrics[unexpected_count_metric.id] == 2

    unexpected_rows_metric = MetricConfiguration(
        metric_name=unexpected_rows_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_rows_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(unexpected_rows_metric,), metrics=metrics)
    metrics.update(results)

    assert metrics[unexpected_rows_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
    ]

    unexpected_values_metric = MetricConfiguration(
        metric_name=unexpected_values_metric_name,
        metric_domain_kwargs={
            "column_list": ["a", "b", "c"],
            "ignore_row_if": "any_value_is_missing",
        },
        metric_value_kwargs={
            "result_format": {"result_format": "SUMMARY", "partial_unexpected_count": 3}
        },
    )
    unexpected_values_metric.metric_dependencies = {
        "unexpected_condition": condition_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_values_metric,), metrics=metrics
    )
    metrics.update(results)

    assert len(metrics[unexpected_values_metric.id]) == 2
    assert metrics[unexpected_values_metric.id] == [
        {"a": 1.0, "b": 1.0, "c": 2.0},
        {"a": 4.0, "b": 4.0, "c": 4.0},
    ]
