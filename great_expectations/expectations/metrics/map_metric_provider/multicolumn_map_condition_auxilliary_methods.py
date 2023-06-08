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
from great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable import (
    _is_sqlalchemy_metric_selectable,
)
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
    verify_column_names_exist,
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


def _pandas_multicolumn_map_condition_values(
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
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    domain_values = df[column_list]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return domain_values.to_dict("records")

    return domain_values[: result_format["partial_unexpected_count"]].to_dict("records")


def _pandas_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
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
(_pandas_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.shape[0]


def _sqlalchemy_multicolumn_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
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
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    column_selector = [sa.column(column_name) for column_name in column_list]
    query = sa.select(*column_selector).where(boolean_mapped_unexpected_values)
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])

    return [val._asdict() for val in execution_engine.execute_query(query).fetchall()]


def _sqlalchemy_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
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
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    selectable = get_sqlalchemy_selectable(selectable)

    return execution_engine.execute_query(
        sa.select(sa.func.count()).select_from(selectable)
    ).scalar()


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

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]

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

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs[
        "column_list"
    ]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.count()
