from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Tuple,
    Union,
)

import great_expectations.exceptions as gx_exceptions

if TYPE_CHECKING:
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
        SqlAlchemyExecutionEngine,
    )

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable import (
    _is_sqlalchemy_metric_selectable,
)
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark, sqlalchemy


logger = logging.getLogger(__name__)


def _pandas_column_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_values).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return list(domain_values)

    return list(domain_values[: result_format["partial_unexpected_count"]])


# TODO: <Alex>11/15/2022: Please DO_NOT_DELETE this method (even though it is not currently utilized).  Thanks.</Alex>
def _pandas_column_map_series_and_domain_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    (
        map_series,
        compute_domain_kwargs_2,
        accessor_domain_kwargs_2,
    ) = metrics["metric_partial_fn"]
    assert (
        compute_domain_kwargs == compute_domain_kwargs_2
    ), "map_series and condition must have the same compute domain"
    assert (
        accessor_domain_kwargs == accessor_domain_kwargs_2
    ), "map_series and condition must have the same accessor kwargs"
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_series_and_domain_values).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]
    map_series = map_series[boolean_mapped_unexpected_values == True]  # noqa: E712

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return (
            list(domain_values),
            list(map_series),
        )

    return (
        list(domain_values[: result_format["partial_unexpected_count"]]),
        list(map_series[: result_format["partial_unexpected_count"]]),
    )


def _pandas_column_map_condition_value_counts(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns respective value counts for distinct column values"""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_value_counts).
"""
        )

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    result_format = metric_value_kwargs["result_format"]
    value_counts = None
    try:
        value_counts = domain_values[boolean_mapped_unexpected_values].value_counts()
    except ValueError:
        try:
            value_counts = (
                domain_values[boolean_mapped_unexpected_values]
                .apply(tuple)
                .value_counts()
            )
        except ValueError:
            pass

    if not value_counts:
        raise gx_exceptions.MetricComputationError("Unable to compute value counts")

    if result_format["result_format"] == "COMPLETE":
        return value_counts

    return value_counts[result_format["partial_unexpected_count"]]


def _sqlalchemy_column_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Tuple],
    **kwargs,
):
    """
    Particularly for the purpose of finding unexpected values, returns all the metric values which do not meet an
    expected Expectation condition for ColumnMapExpectation Expectations.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_values).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    query = sa.select(sa.column(column_name).label("unexpected_values")).where(
        unexpected_condition
    )
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    elif (
        result_format["result_format"] == "COMPLETE"
        and execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
    ):
        logger.warning(
            "BigQuery imposes a limit of 10000 parameters on individual queries; "
            "if your data contains more than 10000 columns your results will be truncated."
        )
        query = query.limit(10000)  # BigQuery upper bound on query parameters

    return [
        val.unexpected_values
        for val in execution_engine.execute_query(query).fetchall()
    ]


def _sqlalchemy_column_map_condition_value_counts(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Returns value counts for all the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_value_counts).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    column: sa.Column = sa.column(column_name)

    query = (
        sa.select(column, sa.func.count(column))
        .where(unexpected_condition)
        .group_by(column)
    )
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    return execution_engine.execute_query(query).fetchall()


def _spark_column_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_spark_column_map_condition_values).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        rows = filtered.select(
            F.col(column_name).alias(column_name)
        ).collect()  # note that without the explicit alias, spark will use only the final portion of a nested column as the column name
    else:
        rows = (
            filtered.select(
                F.col(column_name).alias(column_name)
            )  # note that without the explicit alias, spark will use only the final portion of a nested column as the column name
            .limit(result_format["partial_unexpected_count"])
            .collect()
        )
    return [row[column_name] for row in rows]


def _spark_column_map_condition_value_counts(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_spark_column_map_condition_value_counts).
"""
        )

    column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]
    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    df: pyspark.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]

    value_counts = filtered.groupBy(F.col(column_name).alias(column_name)).count()
    if result_format["result_format"] == "COMPLETE":
        rows = value_counts.collect()
    else:
        rows = value_counts.collect()[: result_format["partial_unexpected_count"]]
    return rows
