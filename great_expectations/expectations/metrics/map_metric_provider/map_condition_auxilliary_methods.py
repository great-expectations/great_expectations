from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)

import numpy as np

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.pyspark import pyspark
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable import (  # noqa: E501
    _is_sqlalchemy_metric_selectable,
)
from great_expectations.expectations.metrics.util import (
    MAX_RESULT_RECORDS,
    compute_unexpected_pandas_indices,
    get_dbms_compatible_metric_domain_kwargs,
    get_sqlalchemy_source_table_and_schema,
    sql_statement_with_post_compile_to_string,
)
from great_expectations.util import (
    convert_to_json_serializable,  # noqa: TID251
    generate_temporary_table_name,
    get_sqlalchemy_selectable,
)

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
        SqlAlchemyExecutionEngine,
    )


logger = logging.getLogger(__name__)


def _pandas_map_condition_unexpected_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> int:
    """Returns unexpected count for MapExpectations"""
    return np.count_nonzero(metrics["unexpected_condition"][0])


def _pandas_map_condition_index(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[int], List[Dict[str, Any]]]:
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]

    accessor_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs=accessor_domain_kwargs,
        batch_columns_list=metrics["table.columns"],
    )

    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

        ###
        # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
        # currently handle filter_column_isnull differently than other map_fn / map_condition
        # cases.
        ###
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[domain_records_df[column_name].notnull()]

        domain_column_name_list.append(column_name)

    # column pair expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, sqlalchemy.quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list = accessor_domain_kwargs["column_list"]
        domain_column_name_list = column_list

    result_format = metric_value_kwargs["result_format"]
    domain_records_df = domain_records_df[boolean_mapped_unexpected_values]

    unexpected_index_list: Union[List[int], List[Dict[str, Any]]] = (
        compute_unexpected_pandas_indices(
            domain_records_df=domain_records_df,
            result_format=result_format,
            execution_engine=execution_engine,
            metrics=metrics,
            expectation_domain_column_list=domain_column_name_list,
        )
    )
    if result_format["result_format"] == "COMPLETE":
        return unexpected_index_list
    return unexpected_index_list[: result_format["partial_unexpected_count"]]


def _pandas_map_condition_query(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[str]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation. For Pandas, this is currently the full set of unexpected_indices.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """  # noqa: E501
    result_format: dict = metric_value_kwargs["result_format"]

    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: Optional[bool] = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return None

    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]

    accessor_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs=accessor_domain_kwargs,
        batch_columns_list=metrics["table.columns"],
    )

    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )

    if "column" in accessor_domain_kwargs:
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[domain_records_df[column_name].notnull()]

    domain_values_df_filtered = domain_records_df[boolean_mapped_unexpected_values]
    index_list = domain_values_df_filtered.index.to_list()
    return f"df.filter(items={index_list}, axis=0)"


def _pandas_map_condition_rows(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> pd.DataFrame:
    """Return values from the specified domain (ignoring the column constraint) that match the map-style metric in the metrics dictionary."""  # noqa: E501
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]

    accessor_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs=accessor_domain_kwargs,
        batch_columns_list=metrics["table.columns"],
    )

    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column" in accessor_domain_kwargs:
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]

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

    result_format = metric_value_kwargs["result_format"]

    df = df[boolean_mapped_unexpected_values]

    if result_format["result_format"] == "COMPLETE":
        return df[:MAX_RESULT_RECORDS]

    limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
    return df.iloc[:limit]


def _sqlalchemy_map_condition_unexpected_count_aggregate_fn(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> tuple[Any, Any, Any]:
    """Returns unexpected count for MapExpectations"""
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]

    return (
        sa.func.sum(
            sa.case(
                (unexpected_condition, 1),
                else_=0,
            )
        ),
        compute_domain_kwargs,
        accessor_domain_kwargs,
    )


def _sqlalchemy_map_condition_unexpected_count_value(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> float | int:
    """Returns unexpected count for MapExpectations. This is a *value* metric, which is useful for
    when the unexpected_condition is a window function.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # The integral values are cast to SQL Numeric in order to avoid a bug in AWS Redshift (converted to integer later).  # noqa: E501
    count_case_statement: List[sqlalchemy.Label] = sa.case(  # type: ignore[assignment]
        (
            unexpected_condition,
            sa.sql.expression.cast(1, sa.Numeric),
        ),
        else_=sa.sql.expression.cast(0, sa.Numeric),
    ).label("condition")

    count_selectable: sqlalchemy.Select = sa.select(count_case_statement)  # type: ignore[call-overload]
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)  # type: ignore[arg-type]
        count_selectable = count_selectable.select_from(selectable)  # type: ignore[arg-type]

    try:
        if execution_engine.dialect_name == GXSqlDialect.MSSQL:
            with execution_engine.get_connection() as connection:
                if not connection.closed:
                    temp_table_obj = _generate_temp_table(
                        connection=connection,
                        metric_domain_kwargs=metric_domain_kwargs,
                        metric_value_kwargs=metric_value_kwargs,
                        metrics=metrics,
                    )
                else:
                    with connection.begin():
                        temp_table_obj = _generate_temp_table(
                            connection=connection,
                            metric_domain_kwargs=metric_domain_kwargs,
                            metric_value_kwargs=metric_value_kwargs,
                            metrics=metrics,
                        )
            inner_case_query: sqlalchemy.Insert = temp_table_obj.insert().from_select(
                [count_case_statement],  # type: ignore[list-item]
                count_selectable,
            )
            execution_engine.execute_query_in_transaction(inner_case_query)  # type: ignore[arg-type]

            count_selectable = temp_table_obj  # type: ignore[assignment]

        count_selectable = get_sqlalchemy_selectable(count_selectable)  # type: ignore[assignment]
        unexpected_count_query: sqlalchemy.Select = (
            sa.select(  # type: ignore[assignment]
                sa.func.sum(sa.column("condition")).label("unexpected_count"),
            )
            .select_from(count_selectable)  # type: ignore[arg-type]
            .alias("UnexpectedCountSubquery")
        )
        unexpected_count: Union[float, int] = execution_engine.execute_query(  # type: ignore[assignment]
            sa.select(
                unexpected_count_query.c[
                    f"{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
                ],
            )
        ).scalar()
        # Unexpected count can be None if the table is empty, in which case the count
        # should default to zero.
        try:
            unexpected_count = int(unexpected_count)
        except TypeError:
            unexpected_count = 0

    except sqlalchemy.OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {oe!s}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(message=exception_message)

    return convert_to_json_serializable(unexpected_count)


def _sqlalchemy_map_condition_rows(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[Sequence[sa.Row[Any]], Any]:
    """
    Returns all rows of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """  # noqa: E501
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    table_columns: list[str] = metrics["table.columns"]
    column_selector = [sa.column(column_name) for column_name in table_columns]  # type: ignore[var-annotated]
    query = sa.select(*column_selector).where(unexpected_condition)
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)  # type: ignore[arg-type]
        query = query.select_from(selectable)  # type: ignore[arg-type]

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        query = query.limit(limit)
    try:
        return execution_engine.execute_query(query).fetchmany(MAX_RESULT_RECORDS)
    except sqlalchemy.OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {oe!s}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(message=exception_message)


def _sqlalchemy_map_condition_query(  # noqa: C901 - too complex
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[str]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """  # noqa: E501
    (
        unexpected_condition,
        _compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]

    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: Optional[bool] = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    column_selector: List[sa.Column] = []

    all_table_columns: List[str] = metrics.get("table.columns", [])
    unexpected_index_column_names: List[str] | None = result_format.get(
        "unexpected_index_column_names"
    )
    if unexpected_index_column_names:
        for column_name in unexpected_index_column_names:
            if column_name not in all_table_columns:
                raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                    message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '  # noqa: E501
                    f"Please check your configuration and try again."
                )

            column_selector.append(sa.column(column_name))  # type: ignore[arg-type]

    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))  # type: ignore[arg-type]

    unexpected_condition_query_with_selected_columns: sa.select = sa.select(*column_selector).where(  # type: ignore[valid-type]
        unexpected_condition
    )
    source_table_and_schema: sa.Table = get_sqlalchemy_source_table_and_schema(execution_engine)

    source_table_and_schema_as_selectable: Union[sa.Table, sa.Select] = get_sqlalchemy_selectable(
        source_table_and_schema
    )
    final_select_statement: sa.select = (  # type: ignore[valid-type]
        unexpected_condition_query_with_selected_columns.select_from(  # type: ignore[attr-defined]
            source_table_and_schema_as_selectable
        )
    )

    query_as_string: str = sql_statement_with_post_compile_to_string(
        engine=execution_engine, select_statement=final_select_statement
    )
    return query_as_string


def _sqlalchemy_map_condition_index(  # noqa: C901 - too complex
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict[str, Any]] | None:
    """
    Returns indices of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return.
    """  # noqa: E501
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]

    result_format = metric_value_kwargs["result_format"]
    if "unexpected_index_column_names" not in result_format:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    domain_kwargs: dict = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    all_table_columns: List[str] = metrics.get("table.columns", [])

    unexpected_index_column_names: List[str] = result_format.get(
        "unexpected_index_column_names", []
    )

    column_selector: List[sa.Column] = []
    for column_name in unexpected_index_column_names:
        if column_name not in all_table_columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '  # noqa: E501
                f"Please check your configuration and try again."
            )
        column_selector.append(sa.column(column_name))  # type: ignore[arg-type]

    # the last column we SELECT is the column the Expectation is being run on
    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))  # type: ignore[arg-type]

    domain_records_as_selectable: sa.sql.Selectable = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    unexpected_condition_query_with_selected_columns: sa.select = sa.select(*column_selector).where(  # type: ignore[valid-type]
        unexpected_condition
    )

    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        domain_records_as_selectable = get_sqlalchemy_selectable(domain_records_as_selectable)  # type: ignore[arg-type]

    # since SQL tables can be **very** large, truncate query_result values at 20, or at `partial_unexpected_count`  # noqa: E501
    final_query: sa.select = unexpected_condition_query_with_selected_columns.select_from(  # type: ignore[valid-type,attr-defined]
        domain_records_as_selectable
    ).limit(result_format["partial_unexpected_count"])
    query_result: List[sqlalchemy.Row] = execution_engine.execute_query(final_query).fetchall()  # type: ignore[assignment]

    exclude_unexpected_values: bool = result_format.get("exclude_unexpected_values", False)

    return _get_sqlalchemy_customized_unexpected_index_list(
        exclude_unexpected_values=exclude_unexpected_values,
        unexpected_index_column_names=unexpected_index_column_names,
        query_result=query_result,
        domain_column_name_list=domain_column_name_list,
    )


def _spark_map_condition_unexpected_count_aggregate_fn(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> tuple[Any, Any, Any]:
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]
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
) -> int:
    # fn_domain_kwargs maybe updated to reflect null filtering
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    return filtered.count()


def _spark_map_condition_rows(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[pyspark.Row]:
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics[
        "unexpected_condition"
    ]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return filtered.limit(MAX_RESULT_RECORDS).collect()

    limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
    return filtered.limit(limit).collect()


def _spark_map_condition_index(  # noqa: C901 - too complex
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
    """  # noqa: E501
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
        column_name: Union[str, sqlalchemy.quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list = list()
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
            message="unexpected_indices cannot be returned without 'unexpected_index_column_names'. Please check your configuration.",  # noqa: E501
            failed_metrics=["unexpected_index_list"],
        )
    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )
    exclude_unexpected_values: bool = result_format.get("exclude_unexpected_values", False)

    unexpected_index_column_names: List[str] = result_format["unexpected_index_column_names"]
    columns_to_keep: List[str] = [column for column in unexpected_index_column_names]
    columns_to_keep += domain_column_name_list

    # check that column name is in row
    for col_name in columns_to_keep:
        if col_name not in filtered.columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(  # noqa: TRY003
                f"Error: The unexpected_index_column '{col_name}' does not exist in Spark DataFrame. Please check your configuration and try again."  # noqa: E501
            )

    if result_format["result_format"] != "COMPLETE":
        filtered = filtered.limit(result_format["partial_unexpected_count"])

    # Prune the dataframe down only the columns we care about
    filtered = filtered.select(columns_to_keep)

    return _get_spark_customized_unexpected_index_list(
        exclude_unexpected_values=exclude_unexpected_values,
        unexpected_index_column_names=unexpected_index_column_names,
        filtered=filtered,
        columns_to_keep=columns_to_keep,
    )


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

    """  # noqa: E501
    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: Optional[bool] = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return None

    (
        unexpected_condition,
        _,
        _,
    ) = metrics.get("unexpected_condition", (None, None, None))

    # unexpected_condition is an F.column object, meaning the str representation is wrapped in Column<> syntax.  # noqa: E501
    # like Column<'[unexpected_expression]'>
    unexpected_condition_as_string: str = str(unexpected_condition)
    unexpected_condition_filtered: str = unexpected_condition_as_string.replace(
        "Column<'(", ""
    ).replace(")'>", "")
    return f"df.filter(F.expr({unexpected_condition_filtered}))"


def _generate_temp_table(
    connection: sa.engine.base.Connection,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> sa.Table:
    temp_table_name: str = generate_temporary_table_name(default_table_name_prefix="#ge_temp_")
    metadata: sa.MetaData = sa.MetaData()
    metadata.reflect(bind=connection)
    temp_table_obj: sa.Table = sa.Table(
        temp_table_name,
        metadata,
        sa.Column("condition", sa.Integer, primary_key=False, nullable=False),
    )
    temp_table_obj.create(bind=connection, checkfirst=True)
    return temp_table_obj


def _get_sqlalchemy_customized_unexpected_index_list(
    exclude_unexpected_values: bool,
    unexpected_index_column_names: List[str],
    query_result: List[sqlalchemy.Row],
    domain_column_name_list: List[Union[str, sqlalchemy.quoted_name]],
) -> Union[List[Dict[str, Any]], None]:
    unexpected_index_list: List[Dict[str, Any]] = []

    if (
        exclude_unexpected_values
        and len(query_result) != 0
        and len(unexpected_index_column_names) != 0
    ):
        primary_key_dict_list: dict[str, List[Any]] = {
            idx_col: [] for idx_col in unexpected_index_column_names
        }
        for row in query_result:
            for index in range(len(unexpected_index_column_names)):
                primary_key_dict_list[unexpected_index_column_names[index]].append(row[index])
        unexpected_index_list.append(primary_key_dict_list)

    else:
        for row in query_result:
            primary_key_dict: Dict[str, Any] = {}
            # add the actual unexpected value
            all_columns = unexpected_index_column_names + domain_column_name_list
            for index in range(len(all_columns)):
                name: str = all_columns[index]
                primary_key_dict[name] = row[index]
            unexpected_index_list.append(primary_key_dict)

    return unexpected_index_list


def _get_spark_customized_unexpected_index_list(
    exclude_unexpected_values: bool,
    unexpected_index_column_names: List[str],
    filtered: pyspark.sql.dataframe.DataFrame,
    columns_to_keep: List[str],
) -> Union[List[Dict[str, Any]], None]:
    unexpected_index_list: List[Dict[str, Any]] = []

    if exclude_unexpected_values and not filtered.isEmpty():
        dict_list_to_add: dict[str, List[Any]] = {
            idx_col: [] for idx_col in unexpected_index_column_names
        }
        for row in filtered.collect():
            for col_name in unexpected_index_column_names:
                dict_list_to_add[col_name].append(row[col_name])
        unexpected_index_list.append(dict_list_to_add)

    else:
        for row in filtered.collect():
            dict_to_add: dict = {}
            for col_name in columns_to_keep:
                dict_to_add[col_name] = row[col_name]
            unexpected_index_list.append(dict_to_add)

    return unexpected_index_list
