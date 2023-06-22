from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator

import pandas as pd

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy_and_pandas import (
    pandas_read_sql,
    pandas_read_sql_query,
)
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    read_sql_table_as_df,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
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
    def _sqlalchemy(  # noqa: C901, PLR0912, PLR0913, PLR0915
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
        dialect = execution_engine.engine.dialect.name.lower()
        if dialect not in GXSqlDialect.get_all_dialect_names():
            dialect = GXSqlDialect.OTHER
        table_name = getattr(selectable, "name", None)
        n_rows: int = (
            metric_value_kwargs.get("n_rows")
            if metric_value_kwargs.get("n_rows") is not None
            else cls.default_kwarg_values["n_rows"]
        )
        df_chunk_iterator: Iterator[pd.DataFrame]
        if (table_name is None) or (
            sqlalchemy._anonymous_label
            and isinstance(table_name, sqlalchemy._anonymous_label)
        ):
            # if a custom query was passed
            try:
                if metric_value_kwargs["fetch_all"]:
                    with execution_engine.get_connection() as con:
                        df = pandas_read_sql_query(
                            sql=selectable,
                            con=con,
                            execution_engine=execution_engine,
                        )
                else:
                    # passing chunksize causes the Iterator to be returned
                    with execution_engine.get_connection() as con:
                        # convert subquery into query using select_from()
                        if not selectable.supports_execution:
                            selectable = sa.select(sa.text("*")).select_from(selectable)
                        df_chunk_iterator = pandas_read_sql_query(
                            sql=selectable,
                            con=con,
                            execution_engine=execution_engine,
                            chunksize=abs(n_rows),
                        )
                        df = TableHead._get_head_df_from_df_iterator(
                            df_chunk_iterator=df_chunk_iterator, n_rows=n_rows
                        )
            except (ValueError, NotImplementedError):
                # MetaData that is used by pd.read_sql_table
                # cannot work on a temp table with pandas < 1.4.0.
                # If it fails, we try to get the data using read_sql.
                df = None
            except StopIteration:
                validator = Validator(execution_engine=execution_engine)
                columns = validator.get_metric(
                    MetricConfiguration("table.columns", metric_domain_kwargs)
                )
                df = pd.DataFrame(columns=columns)
        else:
            try:
                if metric_value_kwargs["fetch_all"]:
                    with execution_engine.get_connection() as con:
                        df = read_sql_table_as_df(
                            table_name=getattr(selectable, "name", None),
                            schema=getattr(selectable, "schema", None),
                            con=con,
                            dialect=dialect,
                        )
                else:
                    with execution_engine.get_connection() as con:
                        # passing chunksize causes the Iterator to be returned
                        df_chunk_iterator = read_sql_table_as_df(
                            table_name=getattr(selectable, "name", None),
                            schema=getattr(selectable, "schema", None),
                            con=con,
                            chunksize=abs(n_rows),
                            dialect=dialect,
                        )
                        df = TableHead._get_head_df_from_df_iterator(
                            df_chunk_iterator=df_chunk_iterator, n_rows=n_rows
                        )
            except (ValueError, NotImplementedError):
                # MetaData that is used by pd.read_sql_table
                # cannot work on a temp table with pandas < 1.4.0.
                # If it fails, we try to get the data using read_sql.
                df = None
            except StopIteration:
                validator = Validator(execution_engine=execution_engine)
                columns = validator.get_metric(
                    MetricConfiguration("table.columns", metric_domain_kwargs)
                )
                df = pd.DataFrame(columns=columns)

        if df is None:
            # we want to compile our selectable
            stmt = sa.select("*").select_from(selectable)
            fetch_all = metric_value_kwargs["fetch_all"]
            if fetch_all:
                sql = stmt.compile(
                    dialect=execution_engine.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            elif execution_engine.engine.dialect.name.lower() == GXSqlDialect.MSSQL:
                # limit doesn't compile properly for mssql
                sql = str(
                    stmt.compile(
                        dialect=execution_engine.engine.dialect,
                        compile_kwargs={"literal_binds": True},
                    )
                )
                if n_rows > 0:
                    sql = f"SELECT TOP {n_rows}{sql[6:]}"
            else:
                if n_rows > 0:
                    stmt = stmt.limit(n_rows)

                sql = stmt.compile(
                    dialect=execution_engine.engine.dialect,
                    compile_kwargs={"literal_binds": True},
                )

            # if read_sql_query or read_sql_table failed, we try to use the read_sql convenience method
            if n_rows <= 0 and not fetch_all:
                with execution_engine.get_connection() as con:
                    df_chunk_iterator = pandas_read_sql(
                        sql=sql, con=con, chunksize=abs(n_rows)
                    )
                    df = TableHead._get_head_df_from_df_iterator(
                        df_chunk_iterator=df_chunk_iterator, n_rows=n_rows
                    )
            else:
                with execution_engine.get_connection() as con:
                    df = pandas_read_sql_query(
                        sql=sql, con=con, execution_engine=execution_engine
                    )

        return df

    @staticmethod
    def _get_head_df_from_df_iterator(
        df_chunk_iterator: Iterator[pd.DataFrame], n_rows: int
    ) -> pd.DataFrame:
        if n_rows > 0:
            df = next(df_chunk_iterator)
        else:
            # if n_rows is zero or negative, remove the last chunk
            df_chunk_list: list[pd.DataFrame]
            df_last_chunk: pd.DataFrame
            *df_chunk_list, df_last_chunk = df_chunk_iterator
            if df_chunk_list:
                df = pd.concat(objs=df_chunk_list, ignore_index=True)
            else:
                # if n_rows is zero, the last chunk is the entire dataframe,
                # so we truncate it to preserve the header
                df = df_last_chunk.head(0)

        return df

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
        df = pd.DataFrame(data=rows)

        return df
