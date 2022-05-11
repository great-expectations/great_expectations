import inspect
import itertools
import logging
import traceback
import warnings
from datetime import datetime
from functools import wraps
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.core.util import (
    convert_to_json_serializable,
    get_sql_dialect_floating_point_infinity_value,
)
from great_expectations.data_asset import DataAsset
from great_expectations.data_asset.util import DocInherit, parse_result_format
from great_expectations.dataset.dataset import Dataset
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.dataset.util import (
    check_sql_engine_dialect,
    get_approximate_percentile_disc_sql,
)
from great_expectations.util import (
    generate_temporary_table_name,
    get_pyathena_potential_type,
    get_sqlalchemy_inspector,
    import_library_module,
)

logger = logging.getLogger(__name__)
try:
    import sqlalchemy as sa
    from sqlalchemy.dialects import registry
    from sqlalchemy.engine import reflection
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.exc import DatabaseError, ProgrammingError
    from sqlalchemy.sql.elements import Label, TextClause, WithinGroup, quoted_name
    from sqlalchemy.sql.expression import BinaryExpression, literal
    from sqlalchemy.sql.operators import custom_op
    from sqlalchemy.sql.selectable import CTE, Select
except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    registry = None
    reflection = None
    BinaryExpression = None
    literal = None
    Select = None
    CTE = None
    custom_op = None
    Label = None
    WithinGroup = None
    TextClause = None
    DefaultDialect = None
    ProgrammingError = None
try:
    from sqlalchemy.engine.row import Row
except ImportError:
    try:
        from sqlalchemy.engine.row import RowProxy

        Row = RowProxy
    except ImportError:
        logger.debug(
            "Unable to load SqlAlchemy Row class; please upgrade you sqlalchemy installation to the latest version."
        )
        RowProxy = None
        Row = None
try:
    import psycopg2
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None
try:
    import sqlalchemy_dremio.pyodbc

    registry.register("dremio", "sqlalchemy_dremio.pyodbc", "dialect")
except ImportError:
    sqlalchemy_dremio = None
try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None
try:
    import snowflake.sqlalchemy.snowdialect

    registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
except (ImportError, KeyError, AttributeError):
    snowflake = None
_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
try:
    import sqlalchemy_bigquery as sqla_bigquery

    registry.register("bigquery", _BIGQUERY_MODULE_NAME, "BigQueryDialect")
    bigquery_types_tuple = None
except ImportError:
    try:
        import pybigquery.sqlalchemy_bigquery as sqla_bigquery

        warnings.warn(
            "The pybigquery package is obsolete and its usage within Great Expectations is deprecated as of v0.14.7. As support will be removed in v0.17, please transition to sqlalchemy-bigquery",
            DeprecationWarning,
        )
        _BIGQUERY_MODULE_NAME = "pybigquery.sqlalchemy_bigquery"
        registry.register("bigquery", _BIGQUERY_MODULE_NAME, "BigQueryDialect")
        try:
            getattr(sqla_bigquery, "INTEGER")
            bigquery_types_tuple = None
        except AttributeError:
            logger.warning(
                "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
            )
            from collections import namedtuple

            BigQueryTypes = namedtuple("BigQueryTypes", sorted(sqla_bigquery._type_map))
            bigquery_types_tuple = BigQueryTypes(**sqla_bigquery._type_map)
    except (ImportError, AttributeError):
        sqla_bigquery = None
        bigquery_types_tuple = None
        pybigquery = None
try:
    import sqlalchemy.dialects.mssql as mssqltypes

    try:
        getattr(mssqltypes, "INT")
    except AttributeError:
        mssqltypes.INT = mssqltypes.INTEGER
except ImportError:
    pass
try:
    import pyathena.sqlalchemy_athena
except ImportError:
    pyathena = None
try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None


class SqlAlchemyBatchReference:
    def __init__(self, engine, table_name=None, schema=None, query=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._engine = engine
        if (table_name is None) and (query is None):
            raise ValueError("Table_name or query must be specified")
        self._table_name = table_name
        self._schema = schema
        self._query = query

    def get_init_kwargs(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self._table_name and self._query:
            kwargs = {
                "engine": self._engine,
                "table_name": self._table_name,
                "custom_sql": self._query,
            }
        elif self._table_name:
            kwargs = {"engine": self._engine, "table_name": self._table_name}
        else:
            kwargs = {"engine": self._engine, "custom_sql": self._query}
        if self._schema:
            kwargs["schema"] = self._schema
        return kwargs


class MetaSqlAlchemyDataset(Dataset):
    def __init__(self, *args, **kwargs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(*args, **kwargs)

    @classmethod
    def column_map_expectation(cls, func):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "For SqlAlchemy, this decorator allows individual column_map_expectations to simply return the filter\n        that describes the expected condition on their data.\n\n        The decorator will then use that filter to obtain unexpected elements, relevant counts, and return the formatted\n        object.\n        "
        argspec = inspect.getfullargspec(func)[0][1:]

        @cls.expectation(argspec)
        @wraps(func)
        def inner_wrapper(
            self, column, mostly=None, result_format=None, *args, **kwargs
        ):
            import inspect

            __frame = inspect.currentframe()
            __file = __frame.f_code.co_filename
            __func = __frame.f_code.co_name
            for (k, v) in __frame.f_locals.items():
                if any((var in k) for var in ("__frame", "__file", "__func")):
                    continue
                print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
            if self.batch_kwargs.get("use_quoted_name"):
                column = quoted_name(column, quote=True)
            if result_format is None:
                result_format = self.default_expectation_args["result_format"]
            result_format = parse_result_format(result_format)
            if result_format["result_format"] == "COMPLETE":
                warnings.warn(
                    "Setting result format to COMPLETE for a SqlAlchemyDataset can be dangerous because it will not limit the number of returned results."
                )
                unexpected_count_limit = None
            else:
                unexpected_count_limit = result_format["partial_unexpected_count"]
            expected_condition: BinaryExpression = func(self, column, *args, **kwargs)
            ignore_values: list = [None]
            if func.__name__ in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                ignore_values = []
                result_format["partial_unexpected_count"] = 0
            ignore_values_conditions: List[BinaryExpression] = []
            if ((len(ignore_values) > 0) and (None not in ignore_values)) or (
                (len(ignore_values) > 1) and (None in ignore_values)
            ):
                ignore_values_conditions += [
                    sa.column(column).in_(
                        [val for val in ignore_values if (val is not None)]
                    )
                ]
            if None in ignore_values:
                ignore_values_conditions += [sa.column(column).is_(None)]
            ignore_values_condition: BinaryExpression
            if len(ignore_values_conditions) > 1:
                ignore_values_condition = sa.or_(*ignore_values_conditions)
            elif len(ignore_values_conditions) == 1:
                ignore_values_condition = ignore_values_conditions[0]
            else:
                ignore_values_condition = BinaryExpression(
                    sa.literal(False), sa.literal(True), custom_op("=")
                )
            count_query: Select
            if self.sql_engine_dialect.name.lower() == "mssql":
                count_query = self._get_count_query_mssql(
                    expected_condition=expected_condition,
                    ignore_values_condition=ignore_values_condition,
                )
            else:
                count_query = self._get_count_query_generic_sqlalchemy(
                    expected_condition=expected_condition,
                    ignore_values_condition=ignore_values_condition,
                )
            count_results: dict = dict(self.engine.execute(count_query).fetchone())
            if ("element_count" not in count_results) or (
                count_results["element_count"] is None
            ):
                count_results["element_count"] = 0
            if ("null_count" not in count_results) or (
                count_results["null_count"] is None
            ):
                count_results["null_count"] = 0
            if ("unexpected_count" not in count_results) or (
                count_results["unexpected_count"] is None
            ):
                count_results["unexpected_count"] = 0
            count_results["element_count"] = int(count_results["element_count"])
            count_results["null_count"] = int(count_results["null_count"])
            count_results["unexpected_count"] = int(count_results["unexpected_count"])
            if self.engine.dialect.name.lower() == "oracle":
                raw_query = (
                    sa.select([sa.column(column)])
                    .select_from(self._table)
                    .where(
                        sa.and_(
                            sa.not_(expected_condition),
                            sa.not_(ignore_values_condition),
                        )
                    )
                )
                query = str(
                    raw_query.compile(
                        self.engine, compile_kwargs={"literal_binds": True}
                    )
                )
                query += "\nAND ROWNUM <= %d" % unexpected_count_limit
            else:
                query = (
                    sa.select([sa.column(column)])
                    .select_from(self._table)
                    .where(
                        sa.and_(
                            sa.not_(expected_condition),
                            sa.not_(ignore_values_condition),
                        )
                    )
                    .limit(unexpected_count_limit)
                )
            unexpected_query_results = self.engine.execute(query)
            nonnull_count: int = (
                count_results["element_count"] - count_results["null_count"]
            )
            if "output_strftime_format" in kwargs:
                output_strftime_format = kwargs["output_strftime_format"]
                maybe_limited_unexpected_list = []
                for x in unexpected_query_results.fetchall():
                    if isinstance(x[column], str):
                        col = parse(x[column])
                    else:
                        col = x[column]
                    maybe_limited_unexpected_list.append(
                        datetime.strftime(col, output_strftime_format)
                    )
            else:
                maybe_limited_unexpected_list = [
                    x[column] for x in unexpected_query_results.fetchall()
                ]
            success_count = nonnull_count - count_results["unexpected_count"]
            (success, percent_success) = self._calc_map_expectation_success(
                success_count, nonnull_count, mostly
            )
            return_obj = self._format_map_output(
                result_format,
                success,
                count_results["element_count"],
                nonnull_count,
                count_results["unexpected_count"],
                maybe_limited_unexpected_list,
                None,
            )
            if func.__name__ in [
                "expect_column_values_to_not_be_null",
                "expect_column_values_to_be_null",
            ]:
                del return_obj["result"]["unexpected_percent_nonmissing"]
                del return_obj["result"]["missing_count"]
                del return_obj["result"]["missing_percent"]
                try:
                    del return_obj["result"]["partial_unexpected_counts"]
                    del return_obj["result"]["partial_unexpected_list"]
                except KeyError:
                    pass
            return return_obj

        inner_wrapper.__name__ = func.__name__
        inner_wrapper.__doc__ = func.__doc__
        return inner_wrapper

    def _get_count_query_mssql(
        self,
        expected_condition: BinaryExpression,
        ignore_values_condition: BinaryExpression,
    ) -> Select:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        temp_table_name: str = generate_temporary_table_name(
            default_table_name_prefix="#ge_temp_"
        )
        with self.engine.begin():
            metadata: sa.MetaData = sa.MetaData(self.engine)
            temp_table_obj: sa.Table = sa.Table(
                temp_table_name,
                metadata,
                sa.Column("condition", sa.Integer, primary_key=False, nullable=False),
            )
            temp_table_obj.create(self.engine, checkfirst=True)
            count_case_statement: List[sa.sql.elements.Label] = [
                sa.case(
                    [
                        (
                            sa.and_(
                                sa.not_(expected_condition),
                                sa.not_(ignore_values_condition),
                            ),
                            1,
                        )
                    ],
                    else_=0,
                ).label("condition")
            ]
            inner_case_query: sa.sql.dml.Insert = temp_table_obj.insert().from_select(
                count_case_statement,
                sa.select(count_case_statement).select_from(self._table),
            )
            self.engine.execute(inner_case_query)
        element_count_query: Select = (
            sa.select(
                [
                    sa.func.count().label("element_count"),
                    sa.func.sum(sa.case([(ignore_values_condition, 1)], else_=0)).label(
                        "null_count"
                    ),
                ]
            )
            .select_from(self._table)
            .alias("ElementAndNullCountsSubquery")
        )
        unexpected_count_query: Select = (
            sa.select([sa.func.sum(sa.column("condition")).label("unexpected_count")])
            .select_from(temp_table_obj)
            .alias("UnexpectedCountSubquery")
        )
        count_query: Select = sa.select(
            [
                element_count_query.c.element_count,
                element_count_query.c.null_count,
                unexpected_count_query.c.unexpected_count,
            ]
        )
        return count_query

    def _get_count_query_generic_sqlalchemy(
        self,
        expected_condition: BinaryExpression,
        ignore_values_condition: BinaryExpression,
    ) -> Select:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return sa.select(
            [
                sa.func.count().label("element_count"),
                sa.func.sum(sa.case([(ignore_values_condition, 1)], else_=0)).label(
                    "null_count"
                ),
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.and_(
                                    sa.not_(expected_condition),
                                    sa.not_(ignore_values_condition),
                                ),
                                1,
                            )
                        ],
                        else_=0,
                    )
                ).label("unexpected_count"),
            ]
        ).select_from(self._table)


class SqlAlchemyDataset(MetaSqlAlchemyDataset):
    "\n\n\n    --ge-feature-maturity-info--\n\n        id: validation_engine_sqlalchemy\n        title: Validation Engine - SQLAlchemy\n        icon:\n        short_description: Use SQLAlchemy to validate data in a database\n        description: Use SQLAlchemy to validate data in a database\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/creating_batches/how_to_load_a_database_table_or_a_query_result_as_a_batch.html\n        maturity: Production\n        maturity_details:\n            api_stability: High\n            implementation_completeness: Moderate (temp table handling/permissions not universal)\n            unit_test_coverage: High\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness:  Minimal (none)\n            bug_risk: Low\n\n    --ge-feature-maturity-info--"

    @classmethod
    def from_dataset(cls, dataset=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if isinstance(dataset, SqlAlchemyDataset):
            return cls(table_name=str(dataset._table.name), engine=dataset.engine)
        else:
            raise ValueError("from_dataset requires a SqlAlchemy dataset")

    def __init__(
        self,
        table_name=None,
        engine=None,
        connection_string=None,
        custom_sql=None,
        schema=None,
        *args,
        **kwargs,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if custom_sql and (not table_name):
            schema = None
            table_name = generate_temporary_table_name()
            if engine.dialect.name.lower() == "mssql":
                table_name = f"#{table_name}"
            self.generated_table_name = table_name
        else:
            self.generated_table_name = None
        if table_name is None:
            raise ValueError("No table_name provided.")
        if (engine is None) and (connection_string is None):
            raise ValueError("Engine or connection_string must be provided.")
        if engine is not None:
            self.engine = engine
        else:
            try:
                self.engine = sa.create_engine(connection_string)
            except Exception as err:
                raise err
        if self.engine.dialect.name.lower() == "bigquery":
            self._table = sa.Table(table_name, sa.MetaData(), schema=None)
            temp_table_schema_name = None
        else:
            try:
                temp_table_schema_name = self.engine.url.query.get("schema")
            except AttributeError as err:
                conn_object = self.engine
                temp_table_schema_name = conn_object.engine.url.query.get("schema")
            self._table = sa.Table(table_name, sa.MetaData(), schema=schema)
        dialect_name: str = self.engine.dialect.name.lower()
        if dialect_name in ["postgresql", "mysql", "sqlite", "oracle", "mssql", "hive"]:
            self.dialect = import_library_module(
                module_name=f"sqlalchemy.dialects.{self.engine.dialect.name}"
            )
        elif dialect_name == "snowflake":
            self.dialect = import_library_module(
                module_name="snowflake.sqlalchemy.snowdialect"
            )
        elif self.engine.dialect.name.lower() == "dremio":
            self.dialect = import_library_module(
                module_name="sqlalchemy_dremio.pyodbc.dialect"
            )
        elif dialect_name == "redshift":
            self.dialect = import_library_module(
                module_name="sqlalchemy_redshift.dialect"
            )
        elif dialect_name == "bigquery":
            self.dialect = import_library_module(module_name=_BIGQUERY_MODULE_NAME)
        elif dialect_name == "awsathena":
            self.dialect = import_library_module(
                module_name="pyathena.sqlalchemy_athena"
            )
        elif dialect_name == "teradatasql":
            self.dialect = import_library_module(
                module_name="teradatasqlalchemy.dialect"
            )
        else:
            self.dialect = None
        if engine and (engine.dialect.name.lower() in ["sqlite", "mssql", "snowflake"]):
            self.engine = engine.connect()
        if (schema is not None) and (custom_sql is not None):
            pass
        if (custom_sql is not None) and (
            self.engine.dialect.name.lower() == "bigquery"
        ):
            if (self.generated_table_name is not None) and (
                self.engine.dialect.dataset_id is None
            ):
                raise ValueError(
                    "No BigQuery dataset specified. Please specify a default dataset in engine url"
                )
        if custom_sql:
            self.create_temporary_table(
                table_name, custom_sql, schema_name=temp_table_schema_name
            )
            if self.generated_table_name is not None:
                if self.engine.dialect.name.lower() == "bigquery":
                    logger.warning(f"Created permanent table {table_name}")
                if self.engine.dialect.name.lower() == "awsathena":
                    logger.warning(f"Created permanent table default.{table_name}")
        try:
            insp = get_sqlalchemy_inspector(self.engine)
            self.columns = insp.get_columns(table_name, schema=schema)
        except KeyError:
            self.columns = self.column_reflection_fallback()
        if len(self.columns) == 0:
            self.columns = self.column_reflection_fallback()
        super().__init__(*args, **kwargs)

    @property
    def sql_engine_dialect(self) -> DefaultDialect:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.engine.dialect

    def attempt_allowing_relative_error(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        detected_redshift: bool = (
            sqlalchemy_redshift is not None
        ) and check_sql_engine_dialect(
            actual_sql_engine_dialect=self.sql_engine_dialect,
            candidate_sql_engine_dialect=sqlalchemy_redshift.dialect.RedshiftDialect,
        )
        detected_psycopg2: bool = (
            sqlalchemy_psycopg2 is not None
        ) and check_sql_engine_dialect(
            actual_sql_engine_dialect=self.sql_engine_dialect,
            candidate_sql_engine_dialect=sqlalchemy_psycopg2.PGDialect_psycopg2,
        )
        return detected_redshift or detected_psycopg2

    def head(self, n=5):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Returns a *PandasDataset* with the first *n* rows of the given Dataset"
        try:
            df = next(
                pd.read_sql_table(
                    table_name=self._table.name,
                    schema=self._table.schema,
                    con=self.engine,
                    chunksize=n,
                )
            )
        except (ValueError, NotImplementedError):
            head_sql_str = "select * from "
            if self._table.schema and (self.engine.dialect.name.lower() != "bigquery"):
                head_sql_str += f"{self._table.schema}.{self._table.name}"
            elif self.engine.dialect.name.lower() == "bigquery":
                head_sql_str += f"`{self._table.name}`"
            else:
                head_sql_str += self._table.name
            head_sql_str += f" limit {n:d}"
            if self.engine.dialect.name.lower() == "mssql":
                head_sql_str = f"select top({n}) * from {self._table.name}"
            if self.engine.dialect.name.lower() == "oracle":
                head_sql_str = f"select * from {self._table.name} WHERE ROWNUM <= {n}"
            if self.engine.dialect.name.lower() == "teradatasql":
                head_sql_str = f"select * from {self._table.name} sample {n}"
            df = pd.read_sql(head_sql_str, con=self.engine)
        except StopIteration:
            df = pd.DataFrame(columns=self.get_table_columns())
        return PandasDataset(
            df,
            expectation_suite=self.get_expectation_suite(
                discard_failed_expectations=False,
                discard_result_format_kwargs=False,
                discard_catch_exceptions_kwargs=False,
                discard_include_config_kwargs=False,
            ),
        )

    def get_row_count(self, table_name=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if table_name is None:
            table_name = self._table
        else:
            table_name = sa.table(table_name)
        count_query = sa.select([sa.func.count()]).select_from(table_name)
        return int(self.engine.execute(count_query).scalar())

    def get_column_count(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return len(self.columns)

    def get_table_columns(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return [col["name"] for col in self.columns]

    def get_column_nonnull_count(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        ignore_values = [None]
        count_query = sa.select(
            [
                sa.func.count().label("element_count"),
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.or_(
                                    (
                                        sa.column(column).in_(ignore_values)
                                        if (
                                            self.engine.dialect.name.lower()
                                            != "teradatasql"
                                        )
                                        else False
                                    ),
                                    (
                                        sa.column(column).is_(None)
                                        if (None in ignore_values)
                                        else False
                                    ),
                                ),
                                1,
                            )
                        ],
                        else_=0,
                    )
                ).label("null_count"),
            ]
        ).select_from(self._table)
        count_results = dict(self.engine.execute(count_query).fetchone())
        element_count = int(count_results.get("element_count") or 0)
        null_count = int(count_results.get("null_count") or 0)
        return element_count - null_count

    def get_column_sum(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return convert_to_json_serializable(
            self.engine.execute(
                sa.select([sa.func.sum(sa.column(column))]).select_from(self._table)
            ).scalar()
        )

    def get_column_max(self, column, parse_strings_as_datetimes=False):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if parse_strings_as_datetimes:
            raise NotImplementedError
        return convert_to_json_serializable(
            self.engine.execute(
                sa.select([sa.func.max(sa.column(column))]).select_from(self._table)
            ).scalar()
        )

    def get_column_min(self, column, parse_strings_as_datetimes=False):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if parse_strings_as_datetimes:
            raise NotImplementedError
        return convert_to_json_serializable(
            self.engine.execute(
                sa.select([sa.func.min(sa.column(column))]).select_from(self._table)
            ).scalar()
        )

    def get_column_value_counts(self, column, sort="value", collate=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if sort not in ["value", "count", "none"]:
            raise ValueError("sort must be either 'value', 'count', or 'none'")
        query = (
            sa.select(
                [
                    sa.column(column).label("value"),
                    sa.func.count(sa.column(column)).label("count"),
                ]
            )
            .where(sa.column(column) != None)
            .group_by(sa.column(column))
        )
        if sort == "value":
            if collate is not None:
                query = query.order_by(sa.column(column).collate(collate))
            else:
                query = query.order_by(sa.column(column))
        elif sort == "count":
            query = query.order_by(sa.column("count").desc())
        results = self.engine.execute(query.select_from(self._table)).fetchall()
        series = pd.Series(
            [row[1] for row in results],
            index=pd.Index(data=[row[0] for row in results], name="value"),
            name="count",
        )
        return series

    def get_column_mean(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return convert_to_json_serializable(
            self.engine.execute(
                sa.select([sa.func.avg(sa.column(column) * 1.0)]).select_from(
                    self._table
                )
            ).scalar()
        )

    def get_column_unique_count(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return convert_to_json_serializable(
            self.engine.execute(
                sa.select(
                    [sa.func.count(sa.func.distinct(sa.column(column)))]
                ).select_from(self._table)
            ).scalar()
        )

    def get_column_median(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.sql_engine_dialect.name.lower() == "awsathena":
            element_values = self.engine.execute(
                f"SELECT approx_percentile({column},  0.5) FROM {self._table}"
            )
            return convert_to_json_serializable(element_values.fetchone()[0])
        else:
            nonnull_count = self.get_column_nonnull_count(column)
            element_values = self.engine.execute(
                sa.select([sa.column(column)])
                .order_by(sa.column(column))
                .where(sa.column(column) != None)
                .offset(max(((nonnull_count // 2) - 1), 0))
                .limit(2)
                .select_from(self._table)
            )
            column_values = list(element_values.fetchall())
            if len(column_values) == 0:
                column_median = None
            elif (nonnull_count % 2) == 0:
                column_median = float(column_values[0][0] + column_values[1][0]) / 2.0
            else:
                column_median = column_values[1][0]
            return convert_to_json_serializable(column_median)

    def get_column_quantiles(
        self, column: str, quantiles: Iterable, allow_relative_error: bool = False
    ) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.sql_engine_dialect.name.lower() == "mssql":
            return self._get_column_quantiles_mssql(column=column, quantiles=quantiles)
        elif self.sql_engine_dialect.name.lower() == "awsathena":
            return self._get_column_quantiles_awsathena(
                column=column, quantiles=quantiles
            )
        elif self.sql_engine_dialect.name.lower() == "bigquery":
            return self._get_column_quantiles_bigquery(
                column=column, quantiles=quantiles
            )
        elif self.sql_engine_dialect.name.lower() == "mysql":
            return self._get_column_quantiles_mysql(column=column, quantiles=quantiles)
        elif self.sql_engine_dialect.name.lower() == "snowflake":
            quantiles = [round(x, 10) for x in quantiles]
            return self._get_column_quantiles_generic_sqlalchemy(
                column=column,
                quantiles=quantiles,
                allow_relative_error=allow_relative_error,
            )
        elif self.sql_engine_dialect.name.lower() == "sqlite":
            return self._get_column_quantiles_sqlite(column=column, quantiles=quantiles)
        else:
            return convert_to_json_serializable(
                self._get_column_quantiles_generic_sqlalchemy(
                    column=column,
                    quantiles=quantiles,
                    allow_relative_error=allow_relative_error,
                )
            )

    @classmethod
    def _treat_quantiles_exception(cls, pe) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        exception_message: str = "An SQL syntax Exception occurred."
        exception_traceback: str = traceback.format_exc()
        exception_message += (
            f'{type(pe).__name__}: "{str(pe)}".  Traceback: "{exception_traceback}".'
        )
        logger.error(exception_message)
        raise pe

    def _get_column_quantiles_mssql(self, column: str, quantiles: Iterable) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        selects: List[WithinGroup] = [
            sa.func.percentile_disc(quantile)
            .within_group(sa.column(column).asc())
            .over()
            for quantile in quantiles
        ]
        quantiles_query: Select = sa.select(selects).select_from(self._table)
        try:
            quantiles_results: Row = self.engine.execute(quantiles_query).fetchone()
            return list(quantiles_results)
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)

    def _get_column_quantiles_awsathena(self, column: str, quantiles: Iterable) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        import ast

        quantiles_list = list(quantiles)
        quantiles_query = f"SELECT approx_percentile({column}, ARRAY{str(quantiles_list)}) as quantiles from (SELECT {column} from {self._table})"
        try:
            quantiles_results = self.engine.execute(quantiles_query).fetchone()[0]
            quantiles_results_list = ast.literal_eval(quantiles_results)
            return quantiles_results_list
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)

    def _get_column_quantiles_bigquery(self, column: str, quantiles: Iterable) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        selects: List[WithinGroup] = [
            sa.func.percentile_disc(sa.column(column), quantile).over()
            for quantile in quantiles
        ]
        quantiles_query: Select = sa.select(selects).select_from(self._table)
        try:
            quantiles_results = self.engine.execute(quantiles_query).fetchone()
            return list(quantiles_results)
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)

    def _get_column_quantiles_mysql(self, column: str, quantiles: Iterable) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        percent_rank_query: CTE = (
            sa.select(
                [
                    sa.column(column),
                    sa.cast(
                        sa.func.percent_rank().over(order_by=sa.column(column).asc()),
                        sa.dialects.mysql.DECIMAL(18, 15),
                    ).label("p"),
                ]
            )
            .order_by(sa.column("p").asc())
            .select_from(self._table)
            .cte("t")
        )
        selects: List[WithinGroup] = []
        for (idx, quantile) in enumerate(quantiles):
            if np.issubdtype(type(quantile), np.float_):
                quantile = float(quantile)
            quantile_column: Label = (
                sa.func.first_value(sa.column(column))
                .over(
                    order_by=sa.case(
                        [
                            (
                                (
                                    percent_rank_query.c.p
                                    <= sa.cast(
                                        quantile, sa.dialects.mysql.DECIMAL(18, 15)
                                    )
                                ),
                                percent_rank_query.c.p,
                            )
                        ],
                        else_=None,
                    ).desc()
                )
                .label(f"q_{idx}")
            )
            selects.append(quantile_column)
        quantiles_query: Select = (
            sa.select(selects).distinct().order_by(percent_rank_query.c.p.desc())
        )
        try:
            quantiles_results: Row = self.engine.execute(quantiles_query).fetchone()
            return list(quantiles_results)
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)

    def _get_column_quantiles_sqlite(self, column, quantiles: Iterable) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        '\n        The present implementation is somewhat inefficient, because it requires as many calls to "self.engine.execute()"\n        as the number of partitions in the "quantiles" parameter (albeit, typically, only a few).  However, this is the\n        only mechanism available for SQLite at the present time (11/17/2021), because the analytical processing is not a\n        very strongly represented capability of the SQLite database management system.\n        '
        table_row_count_query: Select = sa.select([sa.func.count()]).select_from(
            self._table
        )
        table_row_count_result: Optional[Row] = None
        try:
            table_row_count_result = self.engine.execute(
                table_row_count_query
            ).fetchone()
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)
        table_row_count: int
        if table_row_count_result is not None:
            table_row_count = list(table_row_count_result)[0]
        else:
            table_row_count = 0
        offsets: List[int] = [
            ((quantile * table_row_count) - 1) for quantile in quantiles
        ]
        quantile_queries: List[Select] = [
            sa.select([sa.column(column)])
            .order_by(sa.column(column).asc())
            .offset(offset)
            .limit(1)
            .select_from(self._table)
            for offset in offsets
        ]
        quantile_result: Row
        quantile_query: Select
        try:
            quantiles_results: List[Row] = [
                self.engine.execute(quantile_query).fetchone()
                for quantile_query in quantile_queries
            ]
            return list(
                itertools.chain.from_iterable(
                    [list(quantile_result) for quantile_result in quantiles_results]
                )
            )
        except ProgrammingError as pe:
            self._treat_quantiles_exception(pe)

    def _get_column_quantiles_generic_sqlalchemy(
        self, column: str, quantiles: Iterable, allow_relative_error: bool
    ) -> list:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        selects: List[WithinGroup] = [
            sa.func.percentile_disc(quantile).within_group(sa.column(column).asc())
            for quantile in quantiles
        ]
        quantiles_query: Select = sa.select(selects).select_from(self._table)
        try:
            quantiles_results: Row = self.engine.execute(quantiles_query).fetchone()
            return list(quantiles_results)
        except ProgrammingError:
            if self.attempt_allowing_relative_error():
                sql_approx: str = get_approximate_percentile_disc_sql(
                    selects=selects, sql_engine_dialect=self.sql_engine_dialect
                )
                selects_approx: List[TextClause] = [sa.text(sql_approx)]
                quantiles_query_approx: Select = sa.select(selects_approx).select_from(
                    self._table
                )
                if allow_relative_error:
                    try:
                        quantiles_results: Row = self.engine.execute(
                            quantiles_query_approx
                        ).fetchone()
                        return list(quantiles_results)
                    except ProgrammingError as pe:
                        self._treat_quantiles_exception(pe)
                else:
                    raise ValueError(
                        f'The SQL engine dialect "{str(self.sql_engine_dialect)}" does not support computing quantiles without approximation error; set allow_relative_error to True to allow approximate quantiles.'
                    )
            else:
                raise ValueError(
                    f'The SQL engine dialect "{str(self.sql_engine_dialect)}" does not support computing quantiles with approximation error; set allow_relative_error to False to disable approximate quantiles.'
                )

    def get_column_stdev(self, column):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.sql_engine_dialect.name.lower() == "mssql":
            res = self.engine.execute(
                sa.select([sa.func.stdev(sa.column(column))])
                .select_from(self._table)
                .where(sa.column(column) is not None)
            ).fetchone()
        else:
            res = self.engine.execute(
                sa.select([sa.func.stddev_samp(sa.column(column))])
                .select_from(self._table)
                .where(sa.column(column) is not None)
            ).fetchone()
        return float(res[0])

    def get_column_hist(self, column, bins):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "return a list of counts corresponding to bins\n\n        Args:\n            column: the name of the column for which to get the histogram\n            bins: tuple of bin edges for which to get histogram values; *must* be tuple to support caching\n        "
        case_conditions = []
        idx = 0
        bins = list(bins)
        if (
            bins[0]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            bins[0]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            case_conditions.append(
                sa.func.sum(
                    sa.case([((sa.column(column) < bins[(idx + 1)]), 1)], else_=0)
                ).label(f"bin_{str(idx)}")
            )
            idx += 1
        for idx in range(idx, (len(bins) - 2)):
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.and_(
                                    (sa.column(column) >= bins[idx]),
                                    (sa.column(column) < bins[(idx + 1)]),
                                ),
                                1,
                            )
                        ],
                        else_=0,
                    )
                ).label(f"bin_{str(idx)}")
            )
        if (
            bins[(-1)]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            bins[(-1)]
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            case_conditions.append(
                sa.func.sum(
                    sa.case([((sa.column(column) >= bins[(-2)]), 1)], else_=0)
                ).label(f"bin_{str((len(bins) - 1))}")
            )
        else:
            case_conditions.append(
                sa.func.sum(
                    sa.case(
                        [
                            (
                                sa.and_(
                                    (sa.column(column) >= bins[(-2)]),
                                    (sa.column(column) <= bins[(-1)]),
                                ),
                                1,
                            )
                        ],
                        else_=0,
                    )
                ).label(f"bin_{str((len(bins) - 1))}")
            )
        query = (
            sa.select(case_conditions)
            .where(sa.column(column) != None)
            .select_from(self._table)
        )
        hist = convert_to_json_serializable(list(self.engine.execute(query).fetchone()))
        return hist

    def get_column_count_in_range(
        self, column, min_val=None, max_val=None, strict_min=False, strict_max=True
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (min_val is None) and (max_val is None):
            raise ValueError("Must specify either min or max value")
        if (min_val is not None) and (max_val is not None) and (min_val > max_val):
            raise ValueError("Min value must be <= to max value")
        if (
            min_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            min_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            min_val = get_sql_dialect_floating_point_infinity_value(
                schema=self.sql_engine_dialect.name.lower(), negative=True
            )
        if (
            min_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            min_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            min_val = get_sql_dialect_floating_point_infinity_value(
                schema=self.sql_engine_dialect.name.lower(), negative=False
            )
        if (
            max_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=True
            )
        ) or (
            max_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=True
            )
        ):
            max_val = get_sql_dialect_floating_point_infinity_value(
                schema=self.sql_engine_dialect.name.lower(), negative=True
            )
        if (
            max_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_np", negative=False
            )
        ) or (
            max_val
            == get_sql_dialect_floating_point_infinity_value(
                schema="api_cast", negative=False
            )
        ):
            max_val = get_sql_dialect_floating_point_infinity_value(
                schema=self.sql_engine_dialect.name.lower(), negative=False
            )
        min_condition = None
        max_condition = None
        if min_val is not None:
            if strict_min:
                min_condition = sa.column(column) > min_val
            else:
                min_condition = sa.column(column) >= min_val
        if max_val is not None:
            if strict_max:
                max_condition = sa.column(column) < max_val
            else:
                max_condition = sa.column(column) <= max_val
        if (min_condition is not None) and (max_condition is not None):
            condition = sa.and_(min_condition, max_condition)
        elif min_condition is not None:
            condition = min_condition
        else:
            condition = max_condition
        query = (
            sa.select([sa.func.count(sa.column(column))])
            .where(sa.and_((sa.column(column) != None), condition))
            .select_from(self._table)
        )
        return convert_to_json_serializable(self.engine.execute(query).scalar())

    def create_temporary_table(self, table_name, custom_sql, schema_name=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Create Temporary table based on sql query. This will be used as a basis for executing expectations.\n        WARNING: this feature is new in v0.4.\n        It hasn't been tested in all SQL dialects, and may change based on community feedback.\n        :param custom_sql:\n        "
        engine_dialect = self.sql_engine_dialect.name.lower()
        if isinstance(engine_dialect, bytes):
            engine_dialect = str(engine_dialect, "utf-8")
        if engine_dialect == "bigquery":
            stmt = f"CREATE OR REPLACE VIEW `{table_name}` AS {custom_sql}"
        elif engine_dialect == "databricks":
            stmt = f"CREATE OR REPLACE TEMPORARY VIEW `{table_name}` AS {custom_sql}"
        elif engine_dialect == "dremio":
            stmt = f"CREATE OR REPLACE VDS {table_name} AS {custom_sql}"
        elif engine_dialect == "snowflake":
            table_type = "TEMPORARY" if self.generated_table_name else "TRANSIENT"
            logger.info(f"Creating temporary table {table_name}")
            if schema_name is not None:
                table_name = f"{schema_name}.{table_name}"
            stmt = f"CREATE OR REPLACE {table_type} TABLE {table_name} AS {custom_sql}"
        elif self.sql_engine_dialect.name == "mysql":
            stmt = f"CREATE TEMPORARY TABLE {table_name} AS {custom_sql}"
        elif self.sql_engine_dialect.name == "mssql":
            if "from" in custom_sql:
                strsep = "from"
            else:
                strsep = "FROM"
            custom_sqlmod = custom_sql.split(strsep, maxsplit=1)
            stmt = (
                f"{custom_sqlmod[0]}into {{table_name}} from{custom_sqlmod[1]}".format(
                    table_name=table_name
                )
            )
        elif engine_dialect == "awsathena":
            stmt = f"CREATE TABLE {table_name} AS {custom_sql}"
        elif engine_dialect == "oracle":
            stmt_1 = "CREATE PRIVATE TEMPORARY TABLE {table_name} ON COMMIT PRESERVE DEFINITION AS {custom_sql}".format(
                table_name=table_name, custom_sql=custom_sql
            )
            stmt_2 = "CREATE GLOBAL TEMPORARY TABLE {table_name} ON COMMIT PRESERVE ROWS AS {custom_sql}".format(
                table_name=table_name, custom_sql=custom_sql
            )
        elif engine_dialect == "teradatasql":
            stmt = 'CREATE VOLATILE TABLE "{table_name}" AS ({custom_sql}) WITH DATA NO PRIMARY INDEX ON COMMIT PRESERVE ROWS'.format(
                table_name=table_name, custom_sql=custom_sql
            )
        elif self.sql_engine_dialect.name.lower() in ("hive", b"hive"):
            stmt = "CREATE TEMPORARY TABLE {schema_name}.{table_name} AS {custom_sql}".format(
                schema_name=(schema_name if (schema_name is not None) else "default"),
                table_name=table_name,
                custom_sql=custom_sql,
            )
        else:
            stmt = f'CREATE TEMPORARY TABLE "{table_name}" AS {custom_sql}'
        if engine_dialect == "oracle":
            try:
                self.engine.execute(stmt_1)
            except DatabaseError:
                self.engine.execute(stmt_2)
        else:
            self.engine.execute(stmt)

    def column_reflection_fallback(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "If we can't reflect the table, use a query to at least get column names."
        col_info_dict_list: List[Dict]
        if self.sql_engine_dialect.name.lower() == "mssql":
            type_module = self._get_dialect_type_module()
            col_info_query: TextClause = sa.text(
                f"""
SELECT
    cols.NAME, ty.NAME
FROM
    tempdb.sys.columns AS cols
JOIN
    sys.types AS ty
ON
    cols.user_type_id = ty.user_type_id
WHERE
    object_id = OBJECT_ID('tempdb..{self._table}')
                """
            )
            col_info_tuples_list = self.engine.execute(col_info_query).fetchall()
            col_info_dict_list = [
                {"name": col_name, "type": getattr(type_module, col_type.upper())()}
                for (col_name, col_type) in col_info_tuples_list
            ]
        else:
            query: Select = sa.select([sa.text("*")]).select_from(self._table).limit(1)
            col_names: list = self.engine.execute(query).keys()
            col_info_dict_list = [{"name": col_name} for col_name in col_names]
        return col_info_dict_list

    @DocInherit
    @MetaSqlAlchemyDataset.expectation(["other_table_name"])
    def expect_table_row_count_to_equal_other_table(
        self,
        other_table_name,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Expect the number of rows in this table to equal the number of rows in a different table.\n\n        expect_table_row_count_to_equal is a :func:`expectation         <great_expectations.data_asset.data_asset.DataAsset.expectation>`, not a\n        ``column_map_expectation`` or ``column_aggregate_expectation``.\n\n        Args:\n            other_table_name (str):                 The name of the other table to which to compare.\n\n        Other Parameters:\n            result_format (string or None):                 Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n                For more detail, see :ref:`result_format <result_format>`.\n            include_config (boolean):                 If True, then include the expectation config as part of the result object.                 For more detail, see :ref:`include_config`.\n            catch_exceptions (boolean or None):                 If True, then catch exceptions and include them as part of the result object.                 For more detail, see :ref:`catch_exceptions`.\n            meta (dict or None):                 A JSON-serializable dictionary (nesting allowed) that will be included in the output without                 modification. For more detail, see :ref:`meta`.\n\n        Returns:\n           An ExpectationSuiteValidationResult\n\n            Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n            :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n        See Also:\n            expect_table_row_count_to_be_between\n        "
        row_count = self.get_row_count()
        other_table_row_count = self.get_row_count(table_name=other_table_name)
        return {
            "success": (row_count == other_table_row_count),
            "result": {
                "observed_value": {"self": row_count, "other": other_table_row_count}
            },
        }

    @DocInherit
    @MetaSqlAlchemyDataset.expectation(["column_list", "ignore_row_if"])
    def expect_compound_columns_to_be_unique(
        self,
        column_list,
        ignore_row_if="all_values_are_missing",
        result_format=None,
        row_condition=None,
        condition_parser=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        columns = [
            sa.column(col["name"])
            for col in self.columns
            if (col["name"] in column_list)
        ]
        query = (
            sa.select([sa.func.count()])
            .group_by(*columns)
            .having(sa.func.count() > 1)
            .select_from(self._table)
        )
        if ignore_row_if == "all_values_are_missing":
            query = query.where(~sa.and_(*((col == None) for col in columns)))
        elif ignore_row_if == "any_value_is_missing":
            query = query.where(sa.and_(*((col != None) for col in columns)))
        elif ignore_row_if == "never":
            pass
        else:
            raise ValueError(
                f"ignore_row_if was set to an unexpected value: {ignore_row_if}"
            )
        unexpected_count = self.engine.execute(query).fetchone()
        if unexpected_count is None:
            unexpected_count = 0
        else:
            unexpected_count = unexpected_count[0]
        total_count_query = sa.select([sa.func.count()]).select_from(self._table)
        total_count = self.engine.execute(total_count_query).fetchone()[0]
        if total_count > 0:
            unexpected_percent = (100.0 * unexpected_count) / total_count
        else:
            unexpected_percent = 0
        return {
            "success": (unexpected_count == 0),
            "result": {"unexpected_percent": unexpected_percent},
        }

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return sa.column(column) == None

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_be_null(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return sa.column(column) != None

    def _get_dialect_type_module(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.dialect is None:
            logger.warning(
                "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
            )
            return sa
        try:
            if isinstance(
                self.sql_engine_dialect, sqlalchemy_redshift.dialect.RedshiftDialect
            ):
                return self.dialect.sa
        except (TypeError, AttributeError):
            pass
        try:
            if isinstance(self.sql_engine_dialect, sqla_bigquery.BigQueryDialect) and (
                bigquery_types_tuple is not None
            ):
                return bigquery_types_tuple
        except (TypeError, AttributeError):
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, teradatasqlalchemy.dialect.TeradataDialect
            ) and (teradatatypes is not None):
                return teradatatypes
        except (TypeError, AttributeError):
            pass
        return self.dialect

    @DocInherit
    @DataAsset.expectation(["column", "type_", "mostly"])
    def expect_column_values_to_be_of_type(
        self,
        column,
        type_,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if mostly is not None:
            raise ValueError(
                "SqlAlchemyDataset does not support column map semantics for column types"
            )
        try:
            col_data = [col for col in self.columns if (col["name"] == column)][0]
            col_type = type(col_data["type"])
        except IndexError:
            raise ValueError(f"Unrecognized column: {column}")
        except KeyError:
            raise ValueError(f"No database type data available for column: {column}")
        try:
            if type_ is None:
                success = True
            else:
                type_module = self._get_dialect_type_module()
                if type_module.__name__ == "pyathena.sqlalchemy_athena":
                    potential_type = get_pyathena_potential_type(type_module, type_)
                    if not inspect.isclass(potential_type):
                        real_type = type(potential_type)
                    else:
                        real_type = potential_type
                else:
                    real_type = getattr(type_module, type_)
                success = issubclass(col_type, real_type)
            return {"success": success, "result": {"observed_value": col_type.__name__}}
        except AttributeError:
            raise ValueError(f"Type not recognized by current driver: {type_}")

    @DocInherit
    @DataAsset.expectation(["column", "type_list", "mostly"])
    def expect_column_values_to_be_in_type_list(
        self,
        column,
        type_list,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if mostly is not None:
            raise ValueError(
                "SqlAlchemyDataset does not support column map semantics for column types"
            )
        try:
            col_data = [col for col in self.columns if (col["name"] == column)][0]
            col_type = type(col_data["type"])
        except IndexError:
            raise ValueError(f"Unrecognized column: {column}")
        except KeyError:
            raise ValueError(f"No database type data available for column: {column}")
        if type_list is None:
            success = True
        else:
            types = []
            type_module = self._get_dialect_type_module()
            for type_ in type_list:
                try:
                    if type_module.__name__ == "pyathena.sqlalchemy_athena":
                        potential_type = get_pyathena_potential_type(type_module, type_)
                        if not inspect.isclass(potential_type):
                            real_type = type(potential_type)
                        else:
                            real_type = potential_type
                        types.append(real_type)
                    else:
                        potential_type = getattr(type_module, type_)
                        types.append(potential_type)
                except AttributeError:
                    logger.debug(f"Unrecognized type: {type_}")
            if len(types) == 0:
                logger.warning(
                    "No recognized sqlalchemy types in type_list for current dialect."
                )
            types = tuple(types)
            success = issubclass(col_type, types)
        return {"success": success, "result": {"observed_value": col_type.__name__}}

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_in_set(
        self,
        column,
        value_set,
        mostly=None,
        parse_strings_as_datetimes=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if value_set is None:
            return True
        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        return sa.column(column).in_(tuple(parsed_value_set))

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_be_in_set(
        self,
        column,
        value_set,
        mostly=None,
        parse_strings_as_datetimes=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if parse_strings_as_datetimes:
            parsed_value_set = self._parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        return sa.column(column).notin_(tuple(parsed_value_set))

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        strict_min=False,
        strict_max=False,
        allow_cross_type_comparisons=None,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if parse_strings_as_datetimes:
            if min_value:
                min_value = parse(min_value)
            if max_value:
                max_value = parse(max_value)
        if (
            (min_value is not None)
            and (max_value is not None)
            and (min_value > max_value)
        ):
            raise ValueError("min_value cannot be greater than max_value")
        if (min_value is None) and (max_value is None):
            raise ValueError("min_value and max_value cannot both be None")
        if min_value is None:
            if strict_max:
                return sa.column(column) < max_value
            else:
                return sa.column(column) <= max_value
        elif max_value is None:
            if strict_min:
                return sa.column(column) > min_value
            else:
                return sa.column(column) >= min_value
        elif strict_min and strict_max:
            return sa.and_(
                (sa.column(column) > min_value), (sa.column(column) < max_value)
            )
        elif strict_min:
            return sa.and_(
                (sa.column(column) > min_value), (sa.column(column) <= max_value)
            )
        elif strict_max:
            return sa.and_(
                (sa.column(column) >= min_value), (sa.column(column) < max_value)
            )
        else:
            return sa.and_(
                (sa.column(column) >= min_value), (sa.column(column) <= max_value)
            )

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_value_lengths_to_equal(
        self,
        column,
        value,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return sa.func.length(sa.column(column)) == value

    @DocInherit
    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_value_lengths_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if (min_value is None) and (max_value is None):
            raise ValueError("min_value and max_value cannot both be None")
        try:
            if (min_value is not None) and (not float(min_value).is_integer()):
                raise ValueError("min_value and max_value must be integers")
            if (max_value is not None) and (not float(max_value).is_integer()):
                raise ValueError("min_value and max_value must be integers")
        except ValueError:
            raise ValueError("min_value and max_value must be integers")
        if (min_value is not None) and (max_value is not None):
            return sa.and_(
                (sa.func.length(sa.column(column)) >= min_value),
                (sa.func.length(sa.column(column)) <= max_value),
            )
        elif (min_value is None) and (max_value is not None):
            return sa.func.length(sa.column(column)) <= max_value
        elif (min_value is not None) and (max_value is None):
            return sa.func.length(sa.column(column)) >= min_value

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_be_unique(
        self,
        column,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        dup_query = (
            sa.select([sa.column(column)])
            .select_from(self._table)
            .group_by(sa.column(column))
            .having(sa.func.count(sa.column(column)) > 1)
        )
        if self.sql_engine_dialect.name.lower() == "mysql":
            temp_table_name = generate_temporary_table_name()
            temp_table_stmt = "CREATE TEMPORARY TABLE {new_temp_table} AS SELECT tmp.{column_name} FROM {source_table} tmp".format(
                new_temp_table=temp_table_name,
                source_table=self._table,
                column_name=column,
            )
            self.engine.execute(temp_table_stmt)
            dup_query = (
                sa.select([sa.column(column)])
                .select_from(sa.text(temp_table_name))
                .group_by(sa.column(column))
                .having(sa.func.count(sa.column(column)) > 1)
            )
        return sa.column(column).notin_(dup_query)

    def _get_dialect_regex_expression(self, column, regex, positive=True):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        try:
            if isinstance(self.sql_engine_dialect, sa.dialects.postgresql.dialect):
                if positive:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("~")
                    )
                else:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("!~")
                    )
        except AttributeError:
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, sqlalchemy_redshift.dialect.RedshiftDialect
            ):
                if positive:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("~")
                    )
                else:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("!~")
                    )
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(self.sql_engine_dialect, sa.dialects.mysql.dialect):
                if positive:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("REGEXP")
                    )
                else:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("NOT REGEXP")
                    )
        except AttributeError:
            pass
        try:
            if isinstance(
                self.sql_engine_dialect,
                snowflake.sqlalchemy.snowdialect.SnowflakeDialect,
            ):
                if positive:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("RLIKE")
                    )
                else:
                    return BinaryExpression(
                        sa.column(column), literal(regex), custom_op("NOT RLIKE")
                    )
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(self.sql_engine_dialect, sqla_bigquery.BigQueryDialect):
                if positive:
                    return sa.func.REGEXP_CONTAINS(sa.column(column), literal(regex))
                else:
                    return sa.not_(
                        sa.func.REGEXP_CONTAINS(sa.column(column), literal(regex))
                    )
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(self.sql_engine_dialect, sqlalchemy_dremio.pyodbc.dialect):
                if positive:
                    return sa.func.REGEXP_MATCHES(sa.column(column), literal(regex))
                else:
                    return sa.not_(
                        sa.func.REGEXP_MATCHES(sa.column(column), literal(regex))
                    )
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, teradatasqlalchemy.dialect.TeradataDialect
            ):
                if positive:
                    return (
                        sa.func.REGEXP_SIMILAR(
                            sa.column(column), literal(regex), literal("i")
                        )
                        == 1
                    )
                else:
                    return (
                        sa.func.REGEXP_SIMILAR(
                            sa.column(column), literal(regex), literal("i")
                        )
                        == 0
                    )
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, pyathena.sqlalchemy_athena.AthenaDialect
            ):
                if positive:
                    return sa.func.REGEXP_LIKE(sa.column(column), literal(regex))
                else:
                    return sa.not_(
                        sa.func.REGEXP_LIKE(sa.column(column), literal(regex))
                    )
        except (AttributeError, TypeError):
            pass
        return None

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        regex_expression = self._get_dialect_regex_expression(column, regex)
        if regex_expression is None:
            logger.warning(
                f"Regex is not supported for dialect {str(self.sql_engine_dialect)}"
            )
            raise NotImplementedError
        return regex_expression

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_regex(
        self,
        column,
        regex,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        regex_expression = self._get_dialect_regex_expression(
            column, regex, positive=False
        )
        if regex_expression is None:
            logger.warning(
                f"Regex is not supported for dialect {str(self.sql_engine_dialect)}"
            )
            raise NotImplementedError
        return regex_expression

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_regex_list(
        self,
        column,
        regex_list,
        match_on="any",
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if match_on not in ["any", "all"]:
            raise ValueError("match_on must be any or all")
        if len(regex_list) == 0:
            raise ValueError("At least one regex must be supplied in the regex_list.")
        regex_expression = self._get_dialect_regex_expression(column, regex_list[0])
        if regex_expression is None:
            logger.warning(
                f"Regex is not supported for dialect {str(self.sql_engine_dialect)}"
            )
            raise NotImplementedError
        if match_on == "any":
            condition = sa.or_(
                *(
                    self._get_dialect_regex_expression(column, regex)
                    for regex in regex_list
                )
            )
        else:
            condition = sa.and_(
                *(
                    self._get_dialect_regex_expression(column, regex)
                    for regex in regex_list
                )
            )
        return condition

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_regex_list(
        self,
        column,
        regex_list,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if len(regex_list) == 0:
            raise ValueError("At least one regex must be supplied in the regex_list.")
        regex_expression = self._get_dialect_regex_expression(
            column, regex_list[0], positive=False
        )
        if regex_expression is None:
            logger.warning(
                f"Regex is not supported for dialect {str(self.sql_engine_dialect)}"
            )
            raise NotImplementedError
        return sa.and_(
            *(
                self._get_dialect_regex_expression(column, regex, positive=False)
                for regex in regex_list
            )
        )

    def _get_dialect_like_pattern_expression(self, column, like_pattern, positive=True):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        dialect_supported: bool = False
        try:
            if hasattr(self.sql_engine_dialect, "BigQueryDialect"):
                dialect_supported = True
        except (AttributeError, TypeError):
            logger.debug(
                "Unable to load BigQueryDialect dialect while running _get_dialect_like_pattern_expression",
                exc_info=True,
            )
            pass
        if isinstance(
            self.sql_engine_dialect,
            (
                sa.dialects.sqlite.dialect,
                sa.dialects.postgresql.dialect,
                sa.dialects.mysql.dialect,
                sa.dialects.mssql.dialect,
            ),
        ):
            dialect_supported = True
        try:
            if isinstance(
                self.sql_engine_dialect, sqlalchemy_redshift.dialect.RedshiftDialect
            ):
                dialect_supported = True
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, teradatasqlalchemy.dialect.TeradataDialect
            ):
                dialect_supported = True
        except (AttributeError, TypeError):
            pass
        try:
            if isinstance(
                self.sql_engine_dialect, pyathena.sqlalchemy_athena.AthenaDialect
            ):
                dialect_supported = True
        except (AttributeError, TypeError):
            pass
        if dialect_supported:
            try:
                if positive:
                    return sa.column(column).like(literal(like_pattern))
                else:
                    return sa.not_(sa.column(column).like(literal(like_pattern)))
            except AttributeError:
                pass
        return None

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_like_pattern(
        self,
        column,
        like_pattern,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        like_pattern_expression = self._get_dialect_like_pattern_expression(
            column, like_pattern
        )
        if like_pattern_expression is None:
            logger.warning(
                "Like patterns are not supported for dialect %s"
                % str(self.sql_engine_dialect)
            )
            raise NotImplementedError
        return like_pattern_expression

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_like_pattern(
        self,
        column,
        like_pattern,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        like_pattern_expression = self._get_dialect_like_pattern_expression(
            column, like_pattern, positive=False
        )
        if like_pattern_expression is None:
            logger.warning(
                "Like patterns are not supported for dialect %s"
                % str(self.sql_engine_dialect)
            )
            raise NotImplementedError
        return like_pattern_expression

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_match_like_pattern_list(
        self,
        column,
        like_pattern_list,
        match_on="any",
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if match_on not in ["any", "all"]:
            raise ValueError("match_on must be any or all")
        if len(like_pattern_list) == 0:
            raise ValueError(
                "At least one like_pattern must be supplied in the like_pattern_list."
            )
        like_pattern_expression = self._get_dialect_like_pattern_expression(
            column, like_pattern_list[0]
        )
        if like_pattern_expression is None:
            logger.warning(
                "Like patterns are not supported for dialect %s"
                % str(self.sql_engine_dialect)
            )
            raise NotImplementedError
        if match_on == "any":
            condition = sa.or_(
                *(
                    self._get_dialect_like_pattern_expression(column, like_pattern)
                    for like_pattern in like_pattern_list
                )
            )
        else:
            condition = sa.and_(
                *(
                    self._get_dialect_like_pattern_expression(column, like_pattern)
                    for like_pattern in like_pattern_list
                )
            )
        return condition

    @MetaSqlAlchemyDataset.column_map_expectation
    def expect_column_values_to_not_match_like_pattern_list(
        self,
        column,
        like_pattern_list,
        mostly=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if len(like_pattern_list) == 0:
            raise ValueError(
                "At least one like_pattern must be supplied in the like_pattern_list."
            )
        like_pattern_expression = self._get_dialect_like_pattern_expression(
            column, like_pattern_list[0], positive=False
        )
        if like_pattern_expression is None:
            logger.warning(
                "Like patterns are not supported for dialect %s"
                % str(self.sql_engine_dialect)
            )
            raise NotImplementedError
        return sa.and_(
            *(
                self._get_dialect_like_pattern_expression(
                    column, like_pattern, positive=False
                )
                for like_pattern in like_pattern_list
            )
        )
