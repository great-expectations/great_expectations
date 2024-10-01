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


def _pandas_multicolumn_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict]:
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_values).
"""  # noqa: E501
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs["column_list"]

    domain_values = df[column_list]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return domain_values[:MAX_RESULT_RECORDS].to_dict("records")

    limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
    return domain_values[:limit].to_dict("records")


def _pandas_multicolumn_map_condition_filtered_row_count(
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_filtered_row_count).
"""  # noqa: E501
        )

    return df.shape[0]


def _sqlalchemy_multicolumn_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict]:
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_values).
"""  # noqa: E501
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs["column_list"]

    column_selector = [sa.column(column_name) for column_name in column_list]  # type: ignore[var-annotated]
    query = sa.select(*column_selector).where(boolean_mapped_unexpected_values)
    if not _is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)  # type: ignore[arg-type]
        query = query.select_from(selectable)  # type: ignore[arg-type]

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        query = query.limit(limit)

    return [
        val._asdict() for val in execution_engine.execute_query(query).fetchmany(MAX_RESULT_RECORDS)
    ]


def _sqlalchemy_multicolumn_map_condition_filtered_row_count(
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_filtered_row_count).
"""  # noqa: E501
        )

    selectable = get_sqlalchemy_selectable(selectable)  # type: ignore[arg-type]

    return execution_engine.execute_query(
        sa.select(sa.func.count()).select_from(selectable)  # type: ignore[arg-type]
    ).scalar()


def _spark_multicolumn_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict]:
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_values).
"""  # noqa: E501
        )

    column_list: List[Union[str, sqlalchemy.quoted_name]] = accessor_domain_kwargs["column_list"]

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    column_selector = [F.col(column_name).alias(column_name) for column_name in column_list]

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        domain_values = (
            filtered.select(column_selector).limit(MAX_RESULT_RECORDS).toPandas().to_dict("records")
        )
    else:
        limit = min(result_format["partial_unexpected_count"], MAX_RESULT_RECORDS)
        domain_values = filtered.select(column_selector).limit(limit).toPandas().to_dict("records")

    return domain_values


def _spark_multicolumn_map_condition_filtered_row_count(
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

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(  # noqa: TRY003
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_filtered_row_count).
"""  # noqa: E501
        )

    return df.count()
