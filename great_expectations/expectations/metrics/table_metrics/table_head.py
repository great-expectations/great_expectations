from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy_and_pandas import (
    pandas_read_sql,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


class TableHead(TableMetricProvider):
    metric_name = "table.head"
    value_keys = ("n_rows", "fetch_all")
    default_kwarg_values = {"n_rows": 5, "fetch_all": False}

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: PLR0913
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ) -> pd.DataFrame:
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        if metric_value_kwargs.get("fetch_all", cls.default_kwarg_values["fetch_all"]):
            return df
        n_rows: int = (
            metric_value_kwargs.get("n_rows")
            if metric_value_kwargs.get("n_rows") is not None
            else cls.default_kwarg_values["n_rows"]
        )
        return df.head(n=n_rows)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ) -> pd.DataFrame:
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        n_rows: int = (
            metric_value_kwargs.get("n_rows")
            if metric_value_kwargs.get("n_rows") is not None
            else cls.default_kwarg_values["n_rows"]
        )

        # None means no limit
        limit: int | None = n_rows
        if metric_value_kwargs["fetch_all"]:
            limit = None

        selectable = sa.select("*").select_from(selectable).limit(limit).selectable

        try:
            with execution_engine.get_connection() as con:
                df = pandas_read_sql(
                    sql=selectable,
                    con=con,
                )
        except StopIteration:
            # empty table. At least try to get the column names
            validator = Validator(execution_engine=execution_engine)
            columns = validator.get_metric(
                MetricConfiguration("table.columns", metric_domain_kwargs)
            )
            df = pd.DataFrame(columns=columns)
        return df  # type: ignore[return-value]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ) -> pd.DataFrame:
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        rows: list[pyspark.Row] | list[dict]
        if metric_value_kwargs["fetch_all"]:
            rows = df.collect()
        else:
            n_rows: int = (
                metric_value_kwargs.get("n_rows")
                if metric_value_kwargs.get("n_rows") is not None
                else cls.default_kwarg_values["n_rows"]
            )
            if n_rows >= 0:
                rows = df.head(n=n_rows)
            else:
                rows = df.head(n=df.count() + n_rows)

        rows = [element.asDict() for element in rows]
        df = pd.DataFrame(data=rows)  # type: ignore[assignment]

        return df  # type: ignore[return-value]
