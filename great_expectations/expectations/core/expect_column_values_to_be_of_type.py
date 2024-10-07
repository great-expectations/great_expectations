from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd

from great_expectations.compatibility import aws, pydantic, pyspark, trino
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.model_field_descriptions import COLUMN_DESCRIPTION
from great_expectations.expectations.registry import get_metric_kwargs
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.util import (
    get_clickhouse_sqlalchemy_potential_type,
    get_pyathena_potential_type,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import (
        ExecutionEngine,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs
    from great_expectations.validator.validator import ValidationDependencies

logger = logging.getLogger(__name__)


_BIGQUERY_MODULE_NAME = "sqlalchemy_bigquery"
BIGQUERY_GEO_SUPPORT = False
from great_expectations.compatibility.bigquery import GEOGRAPHY, bigquery_types_tuple
from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as BigQueryDialect,
)

if GEOGRAPHY:
    BIGQUERY_GEO_SUPPORT = True
else:
    BIGQUERY_GEO_SUPPORT = False

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None

try:
    import clickhouse_sqlalchemy
    import clickhouse_sqlalchemy.types as ch_types
except (ImportError, KeyError):
    clickhouse_sqlalchemy = None
    ch_types = None

EXPECTATION_SHORT_DESCRIPTION = "Expect a column to contain values of a specified data type."
TYPE__DESCRIPTION = """
    A string representing the data type that each column should have as entries. \
    Valid types are defined by the current backend implementation and are dynamically loaded.
    """
SUPPORTED_DATA_SOURCES = [
    "Pandas",
    "Spark",
    "SQLite",
    "PostgreSQL",
    "MySQL",
    "MSSQL",
    "Redshift",
    "BigQuery",
    "Snowflake",
]
DATA_QUALITY_ISSUES = ["Schema"]


class ExpectColumnValuesToBeOfType(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToBeOfType is a \
    Column Map Expectation \
    for typed-column backends, and also for Pandas Datasources where the column dtype and provided \
    type_ are unambiguous constraints (any dtype except 'object' or dtype of 'object' with \
    type_ specified as 'object').

    For Pandas columns with dtype of 'object' ExpectColumnValuesToBeOfType will
    independently check each row's type.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        type\\_ (str): \
            {TYPE__DESCRIPTION}
            For example, valid types for Pandas Datasources include any numpy dtype values \
            (such as 'int64') or native python types (such as 'int'), whereas valid types \
            for a SqlAlchemy Datasource include types named by the current driver such as 'INTEGER' \
            in most SQL dialects and 'TEXT' in dialects such as postgresql. \
            Valid types for Spark Datasources include 'StringType', 'BooleanType' and other \
            pyspark-defined type names. Note that the strings representing these \
            types are sometimes case-sensitive. For instance, with a Pandas backend `timestamp` \
            will be unrecognized and fail the expectation, while `Timestamp` would pass with valid data.

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    See also:
        [ExpectColumnValuesToBeInTypeList](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2
            0 	"12345" 1
            1 	"abcde" 2
            2 	"1b3d5" 3

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToBeOfType(
                    column="test2",
                    type_="NUMBER"
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnValuesToBeOfType(
                    column="test",
                    type_="DOUBLE"
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 3,
                    "unexpected_count": 3,
                    "unexpected_percent": 100.0,
                    "partial_unexpected_list": [
                        "12345",
                        "abcde",
                        "1b3d5"
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 100.0,
                    "unexpected_percent_nonmissing": 100.0
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    type_: str = pydantic.Field(description=TYPE__DESCRIPTION)

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "column_values.of_type"
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "column",
        "row_condition",
        "condition_parser",
    )
    success_keys = (
        "type_",
        "mostly",
    )
    args_keys = (
        "column",
        "type_",
    )

    class Config:
        title = "Expect column values to be of type"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectColumnValuesToBeOfType]) -> None:
            ColumnMapExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "data_quality_issues": {
                        "title": "Data Quality Issues",
                        "type": "array",
                        "const": DATA_QUALITY_ISSUES,
                    },
                    "library_metadata": {
                        "title": "Library Metadata",
                        "type": "object",
                        "const": model._library_metadata,
                    },
                    "short_description": {
                        "title": "Short Description",
                        "type": "string",
                        "const": EXPECTATION_SHORT_DESCRIPTION,
                    },
                    "supported_data_sources": {
                        "title": "Supported Data Sources",
                        "type": "array",
                        "const": SUPPORTED_DATA_SOURCES,
                    },
                }
            )

    @override
    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("type_", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = "values must be of type $type_, at least $mostly_pct % of the time."
        else:
            template_str = "values must be of type $type_."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @override
    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")

        kwargs = configuration.kwargs if configuration is not None else {}

        params = substitute_none_for_missing(
            kwargs,
            ["column", "type_", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = "values must be of type $type_, at least $mostly_pct % of the time."
        else:
            template_str = "values must be of type $type_."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                content_block_type="string_template",
                string_template={
                    "template": template_str,
                    "params": params,
                    "styling": styling,
                },
            )
        ]

    def _validate_pandas(
        self,
        actual_column_type,
        expected_type,
    ):
        if expected_type is None:
            success = True
        else:
            comp_types = []

            try:
                comp_types.append(np.dtype(expected_type).type)
            except TypeError:
                try:
                    pd_type = getattr(pd, expected_type)
                    if isinstance(pd_type, type):
                        comp_types.append(pd_type)
                except AttributeError:
                    pass

                try:
                    pd_type = getattr(pd.core.dtypes.dtypes, expected_type)
                    if isinstance(pd_type, type):
                        comp_types.append(pd_type)
                except AttributeError:
                    pass

            native_type = _native_type_type_map(expected_type)
            if native_type is not None:
                comp_types.extend(native_type)

            success = actual_column_type.type in comp_types

        return {
            "success": success,
            "result": {"observed_value": actual_column_type.type.__name__},
        }

    def _validate_sqlalchemy(self, actual_column_type, expected_type, execution_engine):
        # Our goal is to be as explicit as possible. We will match the dialect
        # if that is possible. If there is no dialect available, we *will*
        # match against a top-level SqlAlchemy type.
        #
        # This is intended to be a conservative approach.
        #
        # In particular, we *exclude* types that would be valid under an ORM
        # such as "float" for postgresql with this approach

        if expected_type is None:
            success = True
        else:
            types = []
            type_module = _get_dialect_type_module(execution_engine=execution_engine)
            try:
                # bigquery geography requires installing an extra package
                if (
                    expected_type.lower() == "geography"
                    and execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
                    and not BIGQUERY_GEO_SUPPORT
                ):
                    logger.warning(
                        "BigQuery GEOGRAPHY type is not supported by default. "
                        + "To install support, please run:"
                        + "  $ pip install 'sqlalchemy-bigquery[geography]'"
                    )
                elif type_module.__name__ == "pyathena.sqlalchemy_athena":
                    potential_type = get_pyathena_potential_type(type_module, expected_type)
                    # In the case of the PyAthena dialect we need to verify that
                    # the type returned is indeed a type and not an instance.
                    if not inspect.isclass(potential_type):
                        real_type = type(potential_type)
                    else:
                        real_type = potential_type
                    types.append(real_type)
                elif type_module.__name__ == "clickhouse_sqlalchemy.drivers.base":
                    actual_column_type = get_clickhouse_sqlalchemy_potential_type(
                        type_module, actual_column_type
                    )()
                    potential_type = get_clickhouse_sqlalchemy_potential_type(
                        type_module, expected_type
                    )
                    types.append(potential_type)
                else:
                    potential_type = getattr(type_module, expected_type)
                    types.append(potential_type)
            except AttributeError:
                logger.debug(f"Unrecognized type: {expected_type}")
            if len(types) == 0:
                logger.warning("No recognized sqlalchemy types in type_list for current dialect.")
            types = tuple(types)
            success = isinstance(actual_column_type, types)

        return {
            "success": success,
            "result": {"observed_value": type(actual_column_type).__name__},
        }

    def _validate_spark(
        self,
        actual_column_type,
        expected_type,
    ):
        if expected_type is None:
            success = True
        else:
            types = []
            try:
                type_class = getattr(pyspark.types, expected_type)
                types.append(type_class)
            except AttributeError:
                logger.debug(f"Unrecognized type: {expected_type}")
            if len(types) == 0:
                raise ValueError("No recognized spark types in expected_types_list")  # noqa: TRY003
            types = tuple(types)
            success = isinstance(actual_column_type, types)
        return {
            "success": success,
            "result": {"observed_value": type(actual_column_type).__name__},
        }

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> ValidationDependencies:
        from great_expectations.execution_engine import (
            PandasExecutionEngine,
        )

        # This calls BatchExpectation.get_validation_dependencies to set baseline validation_dependencies for the aggregate version  # noqa: E501
        # of the expectation.
        # We need to keep this as super(ColumnMapExpectation, self), which calls
        # BatchExpectation.get_validation_dependencies instead of ColumnMapExpectation.get_validation_dependencies.  # noqa: E501
        # This is because the map version of this expectation is only supported for Pandas, so we want the aggregate  # noqa: E501
        # version for the other backends.
        validation_dependencies: ValidationDependencies = super(
            ColumnMapExpectation, self
        ).get_validation_dependencies(execution_engine, runtime_configuration)

        configuration = self.configuration

        # Only PandasExecutionEngine supports the column map version of the expectation.
        kwargs = configuration.kwargs if configuration else {}

        if isinstance(execution_engine, PandasExecutionEngine):
            column_name = kwargs.get("column")
            expected_type = kwargs.get("type_")
            metric_kwargs = get_metric_kwargs(
                metric_name="table.column_types",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_domain_kwargs = metric_kwargs.get("metric_domain_kwargs", {})
            metric_value_kwargs = metric_kwargs.get("metric_value_kwargs")
            table_column_types_configuration = MetricConfiguration(
                "table.column_types",
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
            )
            actual_column_types_list = execution_engine.resolve_metrics(
                [table_column_types_configuration]
            )[table_column_types_configuration.id]
            try:
                actual_column_type = [
                    type_dict["type"]
                    for type_dict in actual_column_types_list
                    if type_dict["name"] == column_name
                ][0]
            except IndexError:
                actual_column_type = None

            # only use column map version if column dtype is object
            if (
                actual_column_type
                and actual_column_type.type.__name__ == "object_"
                and expected_type
                not in [
                    "object",
                    "object_",
                    "O",
                    None,
                ]
            ):
                # this resets validation_dependencies using  ColumnMapExpectation.get_validation_dependencies  # noqa: E501
                validation_dependencies = super().get_validation_dependencies(
                    execution_engine, runtime_configuration
                )

        # this adds table.column_types dependency for both aggregate and map versions of expectation
        column_types_metric_kwargs = get_metric_kwargs(
            metric_name="table.column_types",
            configuration=configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.column_types",
            metric_configuration=MetricConfiguration(
                metric_name="table.column_types",
                metric_domain_kwargs=column_types_metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=column_types_metric_kwargs["metric_value_kwargs"],
            ),
        )

        return validation_dependencies

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        from great_expectations.execution_engine import (
            PandasExecutionEngine,
            SparkDFExecutionEngine,
            SqlAlchemyExecutionEngine,
        )

        configuration = self.configuration
        column_name = configuration.kwargs.get("column")
        expected_type = configuration.kwargs.get("type_")
        actual_column_types_list = metrics.get("table.column_types", [])
        actual_column_type = [
            type_dict["type"]
            for type_dict in actual_column_types_list
            if type_dict["name"] == column_name
        ][0]

        if isinstance(execution_engine, PandasExecutionEngine):
            # only PandasExecutionEngine supports map version of expectation and
            # only when column type is object
            if actual_column_type.type.__name__ == "object_" and expected_type not in [
                "object",
                "object_",
                "O",
                None,
            ]:
                # this calls ColumnMapMetric._validate
                return super()._validate(metrics, runtime_configuration, execution_engine)
            return self._validate_pandas(
                actual_column_type=actual_column_type, expected_type=expected_type
            )
        elif isinstance(execution_engine, SqlAlchemyExecutionEngine):
            return self._validate_sqlalchemy(
                actual_column_type=actual_column_type,
                expected_type=expected_type,
                execution_engine=execution_engine,
            )
        elif isinstance(execution_engine, SparkDFExecutionEngine):
            return self._validate_spark(
                actual_column_type=actual_column_type, expected_type=expected_type
            )


def _get_dialect_type_module(  # noqa: C901, PLR0911
    execution_engine,
):
    if execution_engine.dialect_module is None:
        logger.warning("No sqlalchemy dialect found; relying in top-level sqlalchemy types.")
        return sa

    # Redshift does not (yet) export types to top level; only recognize base SA types
    if aws.redshiftdialect and isinstance(
        execution_engine.dialect_module,
        aws.redshiftdialect.RedshiftDialect,
    ):
        return execution_engine.dialect_module.sa
    else:
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if BigQueryDialect and (
            isinstance(
                execution_engine.dialect_module,
                BigQueryDialect,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    # Teradata types module
    try:
        if (
            issubclass(
                execution_engine.dialect_module,
                teradatasqlalchemy.dialect.TeradataDialect,
            )
            and teradatatypes is not None
        ):
            return teradatatypes
    except (TypeError, AttributeError):
        pass

    try:
        if (
            issubclass(
                execution_engine.dialect_module,
                clickhouse_sqlalchemy.drivers.base.ClickHouseDialect,
            )
            and ch_types is not None
        ):
            return ch_types
    except (TypeError, AttributeError):
        pass

    # Trino types module
    try:
        if (
            trino.trinodialect
            and trino.trinotypes
            and isinstance(
                execution_engine.dialect,
                trino.trinodialect.TrinoDialect,
            )
        ):
            return trino.trinotypes
    except (TypeError, AttributeError):
        pass

    return execution_engine.dialect_module


def _native_type_type_map(type_):  # noqa: C901, PLR0911
    # We allow native python types in cases where the underlying type is "object":
    if type_.lower() == "none":
        return (type(None),)
    elif type_.lower() == "bool":
        return (bool,)
    elif type_.lower() in ["int", "long"]:
        return (int,)
    elif type_.lower() == "float":
        return (float,)
    elif type_.lower() == "bytes":
        return (bytes,)
    elif type_.lower() == "complex":
        return (complex,)
    elif type_.lower() in ["str", "string_types"]:
        return (str,)
    elif type_.lower() == "list":
        return (list,)
    elif type_.lower() == "dict":
        return (dict,)
    elif type_.lower() == "unicode":
        return None
