from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple, Type, Union

import numpy as np
import pandas as pd
from packaging import version

from great_expectations.compatibility import pydantic, pyspark
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (  # noqa: TCH001
    SuiteParameterDict,
)
from great_expectations.expectations.core.expect_column_values_to_be_of_type import (
    _get_dialect_type_module,
    _native_type_type_map,
)
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
    get_trino_potential_type,
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

EXPECTATION_SHORT_DESCRIPTION = "Expect a column to contain values from a specified type list."
TYPE_LIST_DESCRIPTION = """
    A list of strings representing the data type that each column should have as entries. \
    Valid types are defined by the current backend implementation and are dynamically loaded.
    """
SUPPORTED_DATA_SOURCES = [
    "Pandas",
    "Spark",
    "SQLite",
    "PostgreSQL",
    "MSSQL",
    "Trino",
    "Redshift",
    "BigQuery",
    "Snowflake",
]
DATA_QUALITY_ISSUES = ["Schema"]


class ExpectColumnValuesToBeInTypeList(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToBeInTypeList is a \
    Column Map Expectation \
    for typed-column backends, and also for Pandas Datasources where the column dtype provides an \
    unambiguous constraints (any dtype except 'object').

    For Pandas columns with dtype of 'object' ExpectColumnValuesToBeInTypeList will \
    independently check each row's type.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        type_list (list[str] or None): \
            {TYPE_LIST_DESCRIPTION}
            For example, valid types for Pandas Datasources include any numpy dtype values \
            (such as 'int64') or native python types (such as 'int'), whereas valid types for a \
            SqlAlchemy Datasource include types named by the current driver such as 'INTEGER' \
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for \
            Spark Datasources include 'StringType', 'BooleanType' and other pyspark-defined type names.

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
        [ExpectColumnValuesToBeOfType](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type)

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
                ExpectColumnValuesToBeInTypeList(
                    column="test2",
                    type_list=["NUMBER", "STRING"]
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
                ExpectColumnValuesToBeInTypeList(
                    column="test",
                    type_list=["NUMBER", "DOUBLE"]
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

    type_list: Union[List[str], SuiteParameterDict, None] = pydantic.Field(
        description=TYPE_LIST_DESCRIPTION
    )

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "column_values.in_type_list"
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "column",
        "row_condition",
        "condition_parser",
    )

    success_keys = (
        "type_list",
        "mostly",
    )
    args_keys = (
        "column",
        "type_list",
    )

    class Config:
        title = "Expect column values to be in type list"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValuesToBeInTypeList]
        ) -> None:
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

    @classmethod
    @override
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("type_list", RendererValueType.ARRAY),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if params.type_list:
            array_param_name = "type_list"
            param_prefix = "v__"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            values_string: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

            if params.mostly and params.mostly.value < 1.0:
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                template_str = (
                    "value types must belong to this set: "
                    + values_string
                    + ", at least $mostly_pct % of the time."
                )
            else:
                template_str = f"value types must belong to this set: {values_string}."
        else:
            template_str = "value types may be any value, but observed value will be reported"

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @override
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
        params = substitute_none_for_missing(
            configuration.kwargs if configuration else {},
            ["column", "type_list", "mostly", "row_condition", "condition_parser"],
        )

        if params["type_list"] is not None:
            for i, v in enumerate(params["type_list"]):
                params[f"v__{i!s}"] = v
            values_string = " ".join([f"$v__{i!s}" for i, v in enumerate(params["type_list"])])

            if params["mostly"] is not None and params["mostly"] < 1.0:
                params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")  # noqa: E501
                if include_column_name:
                    template_str = (
                        "$column value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
                else:
                    template_str = (
                        "value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
            else:  # noqa: PLR5501
                if include_column_name:
                    template_str = f"$column value types must belong to this set: {values_string}."
                else:
                    template_str = f"value types must belong to this set: {values_string}."
        else:  # noqa: PLR5501
            if include_column_name:
                template_str = (
                    "$column value types may be any value, but observed value will be reported"
                )
            else:
                template_str = "value types may be any value, but observed value will be reported"

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

    def _validate_pandas(  # noqa: C901, PLR0912
        self,
        actual_column_type,
        expected_types_list,
    ):
        if expected_types_list is None:
            success = True
        else:
            comp_types = []
            for type_ in expected_types_list:
                try:
                    comp_types.append(np.dtype(type_).type)
                    comp_types.append(np.dtype(type_))
                except TypeError:
                    try:
                        pd_type = getattr(pd, type_)
                    except AttributeError:
                        pass
                    else:
                        if isinstance(pd_type, type):
                            comp_types.append(pd_type)
                            try:
                                if isinstance(pd_type(), pd.core.dtypes.base.ExtensionDtype):
                                    comp_types.append(pd_type())
                            except TypeError:
                                pass
                    try:
                        pd_type = getattr(pd.core.dtypes.dtypes, type_)
                        if isinstance(pd_type, type):
                            comp_types.append(pd_type)
                    except AttributeError:
                        pass

                native_type = _native_type_type_map(type_)
                if native_type is not None:
                    comp_types.extend(native_type)

            # TODO: Remove when Numpy >=1.21 is pinned as a dependency
            _pandas_supports_extension_dtypes = version.parse(pd.__version__) >= version.parse(
                "0.24"
            )
            _numpy_doesnt_support_extensions_properly = version.parse(
                np.__version__
            ) < version.parse("1.21")
            if _numpy_doesnt_support_extensions_properly and _pandas_supports_extension_dtypes:
                # This works around a bug where Pandas nullable int types aren't compatible with Numpy dtypes  # noqa: E501
                # Note: Can't do set difference, the whole bugfix is because numpy types can't be compared to  # noqa: E501
                # ExtensionDtypes
                actual_type_is_ext_dtype = isinstance(
                    actual_column_type, pd.core.dtypes.base.ExtensionDtype
                )
                comp_types = {
                    dtype
                    for dtype in comp_types
                    if isinstance(dtype, pd.core.dtypes.base.ExtensionDtype)
                    == actual_type_is_ext_dtype
                }
            ###

            success = actual_column_type in comp_types

        return {
            "success": success,
            "result": {"observed_value": actual_column_type.type.__name__},
        }

    def _validate_sqlalchemy(  # noqa: C901 - too complex
        self, actual_column_type, expected_types_list, execution_engine
    ):
        # Our goal is to be as explicit as possible. We will match the dialect
        # if that is possible. If there is no dialect available, we *will*
        # match against a top-level SqlAlchemy type.
        #
        # This is intended to be a conservative approach.
        #
        # In particular, we *exclude* types that would be valid under an ORM
        # such as "float" for postgresql with this approach

        if expected_types_list is None:
            success = True
        else:
            types = []
            type_module = _get_dialect_type_module(execution_engine=execution_engine)
            for type_ in expected_types_list:
                try:
                    if type_module.__name__ == "pyathena.sqlalchemy_athena":
                        potential_type = get_pyathena_potential_type(type_module, type_)
                        # In the case of the PyAthena dialect we need to verify that
                        # the type returned is indeed a type and not an instance.
                        if not inspect.isclass(potential_type):
                            real_type = type(potential_type)
                        else:
                            real_type = potential_type
                        types.append(real_type)
                    elif type_module.__name__ == "trino.sqlalchemy.datatype":
                        potential_type = get_trino_potential_type(type_module, type_)
                        types.append(type(potential_type))
                    elif type_module.__name__ == "clickhouse_sqlalchemy.drivers.base":
                        actual_column_type = get_clickhouse_sqlalchemy_potential_type(
                            type_module, actual_column_type
                        )()
                        potential_type = get_clickhouse_sqlalchemy_potential_type(
                            type_module, type_
                        )
                        types.append(potential_type)
                    else:
                        potential_type = getattr(type_module, type_)
                        types.append(potential_type)
                except AttributeError:
                    logger.debug(f"Unrecognized type: {type_}")

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
        expected_types_list,
    ):
        if expected_types_list is None:
            success = True
        else:
            types = []
            for type_ in expected_types_list:
                try:
                    type_class = getattr(pyspark.types, type_)
                    types.append(type_class)
                except AttributeError:
                    logger.debug(f"Unrecognized type: {type_}")
            if len(types) == 0:
                raise ValueError("No recognized spark types in expected_types_list")  # noqa: TRY003
            success = isinstance(actual_column_type, tuple(types))
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
        if isinstance(execution_engine, PandasExecutionEngine):
            column_name = configuration.kwargs.get("column") if configuration else None
            expected_types_list = configuration.kwargs.get("type_list") if configuration else None
            metric_kwargs = get_metric_kwargs(
                metric_name="table.column_types",
                configuration=configuration,
                runtime_configuration=runtime_configuration,
            )
            metric_domain_kwargs: dict = metric_kwargs.get("metric_domain_kwargs") or {}
            metric_value_kwargs = metric_kwargs.get("metric_value_kwargs") or {}
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
                and expected_types_list is not None
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
        expected_types_list = configuration.kwargs.get("type_list")
        actual_column_types_list = metrics.get("table.column_types")
        actual_column_type = (
            [
                type_dict["type"]
                for type_dict in actual_column_types_list
                if type_dict["name"] == column_name
            ][0]
            if actual_column_types_list
            else []
        )

        if isinstance(execution_engine, PandasExecutionEngine):
            # only PandasExecutionEngine supports map version of expectation and
            # only when column type is object
            if actual_column_type.type.__name__ == "object_" and expected_types_list is not None:
                # this calls ColumnMapMetric._validate
                return super()._validate(metrics, runtime_configuration, execution_engine)
            return self._validate_pandas(
                actual_column_type=actual_column_type,
                expected_types_list=expected_types_list,
            )
        elif isinstance(execution_engine, SqlAlchemyExecutionEngine):
            return self._validate_sqlalchemy(
                actual_column_type=actual_column_type,
                expected_types_list=expected_types_list,
                execution_engine=execution_engine,
            )
        elif isinstance(execution_engine, SparkDFExecutionEngine):
            return self._validate_spark(
                actual_column_type=actual_column_type,
                expected_types_list=expected_types_list,
            )
