from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Union,
)

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable import (  # noqa: E501
    _is_sqlalchemy_metric_selectable,
)
from great_expectations.expectations.metrics.util import (
    MAX_RESULT_RECORDS,
    get_dbms_compatible_metric_domain_kwargs,
)
from great_expectations.util import (
    get_sqlalchemy_selectable,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.execution_engine import (
        PandasExecutionEngine,
        SparkDFExecutionEngine,
        SqlAlchemyExecutionEngine,
    )


logger = logging.getLogger(__name__)


def _pandas_column_pair_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[tuple[Any, Any]]:
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
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

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(  # noqa: TRY003
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_values).
"""  # noqa: E501
        )

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, sqlalchemy.quoted_name]] = [
        column_A_name,
        column_B_name,
    ]

    domain_values = df[column_names]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    unexpected_list = [
        value_pair
        for value_pair in zip(
            domain_values[column_A_name].values, domain_values[column_B_name].values
        )
    ]
    if result_format["result_format"] == "COMPLETE":
        return unexpected_list[:MAX_RESULT_RECORDS]

    limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
    return unexpected_list[:limit]


def _pandas_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> int:
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]

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

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(  # noqa: TRY003
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_filtered_row_count).
"""  # noqa: E501
        )

    return df.shape[0]


def _sqlalchemy_column_pair_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[tuple[Any, Any]]:
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
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
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    query = sa.select(  # type: ignore[var-annotated]
        sa.column(column_A_name).label("unexpected_values_A"),
        sa.column(column_B_name).label("unexpected_values_B"),
    ).where(boolean_mapped_unexpected_values)
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)  # type: ignore[arg-type]
        query = query.select_from(selectable)  # type: ignore[arg-type]

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        query = query.limit(limit)

    unexpected_list = [
        (val.unexpected_values_A, val.unexpected_values_B)
        for val in execution_engine.execute_query(query).fetchmany(MAX_RESULT_RECORDS)
    ]
    return unexpected_list


def _sqlalchemy_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Any | None:
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]

    accessor_domain_kwargs = get_dbms_compatible_metric_domain_kwargs(
        metric_domain_kwargs=accessor_domain_kwargs,
        batch_columns_list=metrics["table.columns"],
    )

    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """  # noqa: E501
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    return execution_engine.execute_query(
        sa.select(sa.func.count()).select_from(selectable)  # type: ignore[arg-type]
    ).scalar()


def _spark_column_pair_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[tuple[Any, Any]]:
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
    (
        unexpected_condition,
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

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        query = filtered.select(
            [
                F.col(column_A_name).alias(column_A_name),
                F.col(column_B_name).alias(column_B_name),
            ]
        ).limit(MAX_RESULT_RECORDS)
    else:
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        query = filtered.select(
            [
                F.col(column_A_name).alias(column_A_name),
                F.col(column_B_name).alias(column_B_name),
            ]
        ).limit(limit)

    unexpected_list = [(row[column_A_name], row[column_B_name]) for row in query.collect()]
    return unexpected_list


def _spark_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> int:
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""  # noqa: E501
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]

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

    return df.count()
