from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Union,
)

import great_expectations.exceptions as gx_exceptions

if TYPE_CHECKING:
    from great_expectations.execution_engine import SparkDFExecutionEngine
    from great_expectations.expectations.metrics.import_manager import quoted_name

from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
    verify_column_names_exist,
)

if TYPE_CHECKING:
    import pyspark

logger = logging.getLogger(__name__)

def _spark_map_condition_unexpected_count_aggregate_fn(
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
    return (
        F.sum(F.when(unexpected_condition, 1).otherwise(0)),
        compute_domain_kwargs,
        accessor_domain_kwargs,
    )


def _spark_map_condition_unexpected_count_value(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    # fn_domain_kwargs maybe updated to reflect null filtering
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    return filtered.count()


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

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

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

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    df: pyspark.sql.dataframe.DataFrame = execution_engine.get_domain_records(
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


def _spark_map_condition_rows(
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
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return filtered.collect()

    return filtered.limit(result_format["partial_unexpected_count"]).collect()


def _spark_map_condition_index(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[Dict[str, Any]], None]:
    """
    Returns indices of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition", (None, None, None))

    if unexpected_condition is None:
        return None

    result_format = metric_value_kwargs["result_format"]
    if "unexpected_index_column_names" not in result_format:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df: pyspark.sql.dataframe.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    result_format = metric_value_kwargs["result_format"]
    if not result_format.get("unexpected_index_column_names"):
        raise gx_exceptions.MetricResolutionError(
            message="unexpected_indices cannot be returned without 'unexpected_index_column_names'. Please check your configuration.",
            failed_metrics=["unexpected_index_list"],
        )
    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )
    unexpected_index_list: Optional[List[Dict[str, Any]]] = []

    unexpected_index_column_names: List[str] = result_format[
        "unexpected_index_column_names"
    ]
    columns_to_keep: List[str] = [column for column in unexpected_index_column_names]
    columns_to_keep += domain_column_name_list

    # check that column name is in row
    for col_name in columns_to_keep:
        if col_name not in filtered.columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                f"Error: The unexpected_index_column '{col_name}' does not exist in Spark DataFrame. Please check your configuration and try again."
            )

    if result_format["result_format"] != "COMPLETE":
        filtered.limit(result_format["partial_unexpected_count"])

    for row in filtered.collect():
        dict_to_add: dict = {}
        for col_name in columns_to_keep:
            dict_to_add[col_name] = row[col_name]
        unexpected_index_list.append(dict_to_add)

    return unexpected_index_list


def _spark_map_condition_query(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[str, None]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Converts unexpected_condition into a string that can be rendered in DataDocs

    Output will look like:

        df.filter(F.expr( [unexpected_condition] ))

    """
    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return None

    (
        unexpected_condition,
        _,
        _,
    ) = metrics.get("unexpected_condition", (None, None, None))

    # unexpected_condition is an F.column object, meaning the str representation is wrapped in Column<> syntax.
    # like Column<'[unexpected_expression]'>
    unexpected_condition_as_string: str = str(unexpected_condition)
    unexpected_condition_filtered: str = unexpected_condition_as_string.replace(
        "Column<'(", ""
    ).replace(")'>", "")
    return f"df.filter(F.expr({unexpected_condition_filtered}))"


def _spark_column_pair_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [
        column_A_name,
        column_B_name,
    ]
    # noinspection PyPep8Naming
    column_A_name, column_B_name = get_dbms_compatible_column_names(
        column_names=column_names,
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
            [
                F.col(column_A_name).alias(column_A_name),
                F.col(column_B_name).alias(column_B_name),
            ]
        ).collect()
    else:
        rows = (
            filtered.select(
                [
                    F.col(column_A_name).alias(column_A_name),
                    F.col(column_B_name).alias(column_B_name),
                ]
            )
            .limit(result_format["partial_unexpected_count"])
            .collect()
        )

    unexpected_list = [(row[column_A_name], row[column_B_name]) for row in rows]
    return unexpected_list


def _spark_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return df.count()


def _spark_multicolumn_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    column_selector = [
        F.col(column_name).alias(column_name) for column_name in column_list
    ]

    domain_values = filtered.select(column_selector)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        domain_values = (
            domain_values.select(column_selector).toPandas().to_dict("records")
        )
    else:
        domain_values = (
            domain_values.select(column_selector)
            .limit(result_format["partial_unexpected_count"])
            .toPandas()
            .to_dict("records")
        )

    return domain_values


def _spark_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.count()
