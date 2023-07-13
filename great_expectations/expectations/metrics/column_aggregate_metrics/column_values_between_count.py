from typing import Any, Dict

import numpy as np

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import get_sql_dialect_floating_point_infinity_value
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)


class ColumnValuesBetweenCount(MetricProvider):
    """This metric is an aggregate helper for rare cases."""

    metric_name = "column_values.between.count"
    value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: PLR0913, PLR0912
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        min_value = metric_value_kwargs.get("min_value")
        max_value = metric_value_kwargs.get("max_value")
        strict_min = metric_value_kwargs.get("strict_min")
        strict_max = metric_value_kwargs.get("strict_max")
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        val = df[accessor_domain_kwargs["column"]]

        if min_value is not None and max_value is not None:
            if strict_min and strict_max:
                series = (min_value < val) and (val < max_value)
            elif strict_min:
                series = (min_value < val) and (val <= max_value)
            elif strict_max:
                series = (min_value <= val) and (val < max_value)
            else:
                series = (min_value <= val) and (val <= max_value)

        elif min_value is None and max_value is not None:
            if strict_max:
                series = val < max_value
            else:
                series = val <= max_value

        elif min_value is not None and max_value is None:
            if strict_min:
                series = min_value < val
            else:
                series = min_value <= val
        else:
            raise ValueError("unable to parse domain and value kwargs")

        return np.count_nonzero(series)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913, PLR0912
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        min_value = metric_value_kwargs.get("min_value")
        max_value = metric_value_kwargs.get("max_value")
        strict_min = metric_value_kwargs.get("strict_min")
        strict_max = metric_value_kwargs.get("strict_max")
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")
        dialect_name = execution_engine.engine.dialect.name.lower()

        if (
            min_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            min_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            min_value = get_sql_dialect_floating_point_infinity_value(
                schema=dialect_name, negative=True
            )

        if (
            min_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            min_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            min_value = get_sql_dialect_floating_point_infinity_value(
                schema=dialect_name, negative=False
            )

        if (
            max_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            max_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            max_value = get_sql_dialect_floating_point_infinity_value(
                schema=dialect_name, negative=True
            )

        if (
            max_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            max_value
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            max_value = get_sql_dialect_floating_point_infinity_value(
                schema=dialect_name, negative=False
            )

        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column = sa.column(accessor_domain_kwargs["column"])

        if min_value is None:
            if strict_max:
                condition = column < max_value
            else:
                condition = column <= max_value

        elif max_value is None:
            if strict_min:
                condition = column > min_value
            else:
                condition = column >= min_value

        else:
            if strict_min and strict_max:  # noqa: PLR5501
                condition = sa.and_(column > min_value, column < max_value)
            elif strict_min:
                condition = sa.and_(column > min_value, column <= max_value)
            elif strict_max:
                condition = sa.and_(column >= min_value, column < max_value)
            else:
                condition = sa.and_(column >= min_value, column <= max_value)

        return execution_engine.execute_query(
            sa.select(sa.func.count()).select_from(selectable).where(condition)
        ).scalar()

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913, PLR0912
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        min_value = metric_value_kwargs.get("min_value")
        max_value = metric_value_kwargs.get("max_value")
        strict_min = metric_value_kwargs.get("strict_min")
        strict_max = metric_value_kwargs.get("strict_max")
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )
        column = df[accessor_domain_kwargs["column"]]

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if min_value is None:
            if strict_max:
                condition = column < max_value
            else:
                condition = column <= max_value

        elif max_value is None:
            if strict_min:
                condition = column > min_value
            else:
                condition = column >= min_value

        else:
            if strict_min and strict_max:  # noqa: PLR5501
                condition = (column > min_value) & (column < max_value)
            elif strict_min:
                condition = (column > min_value) & (column <= max_value)
            elif strict_max:
                condition = (column >= min_value) & (column < max_value)
            else:
                condition = (column >= min_value) & (column <= max_value)

        return df.filter(condition).count()
