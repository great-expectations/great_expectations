from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

import numpy as np
import pandas as pd

from great_expectations.compatibility import pydantic, pyspark
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TC001
)
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.metadata_types import DataQualityIssues, SupportedDataSources
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    FAILURE_SEVERITY_DESCRIPTION,
)
from great_expectations.expectations.type_comparison import (
    compare_column_type,
    native_type_type_map,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs

logger = logging.getLogger(__name__)

EXPECTATION_SHORT_DESCRIPTION = "Expect a column to be of a specified data type."
TYPE_DESCRIPTION = """
    A string representing the data type of the column. \
    Valid types are defined by the current backend implementation and are dynamically loaded.
    """
DATA_QUALITY_ISSUES = [DataQualityIssues.SCHEMA.value]
SUPPORTED_DATA_SOURCES = [
    SupportedDataSources.PANDAS.value,
    SupportedDataSources.SPARK.value,
    SupportedDataSources.SQLITE.value,
    SupportedDataSources.POSTGRESQL.value,
    SupportedDataSources.AURORA.value,
    SupportedDataSources.CITUS.value,
    SupportedDataSources.ALLOY.value,
    SupportedDataSources.NEON.value,
    SupportedDataSources.MYSQL.value,
    SupportedDataSources.SQL_SERVER.value,
    SupportedDataSources.BIGQUERY.value,
    SupportedDataSources.SNOWFLAKE.value,
    SupportedDataSources.DATABRICKS.value,
    SupportedDataSources.REDSHIFT.value,
]


class ExpectColumnTypeToBe(BatchExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnTypeToBe is a \
    Batch Expectation.

    BatchExpectations are one of the most common types of Expectation. They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        column (str): {COLUMN_DESCRIPTION}
        type\\_ (str): {TYPE_DESCRIPTION}
            For example, valid types for Pandas Datasources include any numpy dtype values \
            (such as 'int64') or native python types (such as 'int'), whereas valid types \
            for a SqlAlchemy Datasource include types named by the current driver such as 'INTEGER' \
            in most SQL dialects and 'TEXT' in dialects such as postgresql. \
            Valid types for Spark Datasources include 'StringType', 'BooleanType' and other \
            pyspark-defined type names.

    Other Parameters:
        result_format (str or None, optional): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None, optional): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None, optional): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).
        severity (str or None): \
            {FAILURE_SEVERITY_DESCRIPTION} \
            For more detail, see [failure severity](https://docs.greatexpectations.io/docs/cloud/expectations/expectations_overview/#failure-severity).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Data Sources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[9]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[10]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[11]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[12]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Issues:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnTypeToBe(
                    column="test2",
                    type_="INTEGER"
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": "INTEGER"
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnTypeToBe(
                    column="test",
                    type_="INTEGER"
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": "FLOAT"
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    column: pydantic.StrictStr = pydantic.Field(min_length=1, description=COLUMN_DESCRIPTION)
    type_: Union[str, SuiteParameterDict] = pydantic.Field(description=TYPE_DESCRIPTION)

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "experimental",
        "tags": ["core expectation", "table expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    metric_dependencies = ("table.column_types",)
    success_keys = (
        "column",
        "type_",
    )
    domain_keys = ("batch_id",)
    args_keys = (
        "column",
        "type_",
    )

    class Config:
        title = "Expect column to be of type"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectColumnTypeToBe]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
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
            ("type_", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        template_str = "$column must be of type $type_."

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
    ) -> list[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,  # type: ignore[union-attr]
            ["column", "type_"],
        )

        template_str = "$column must be of type $type_."

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

    def _validate_pandas(self, actual_column_type, expected_type):
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

            native_type = native_type_type_map(expected_type)
            if native_type is not None:
                comp_types.extend(native_type)

            success = actual_column_type.type in comp_types

        return {
            "success": success,
            "result": {"observed_value": actual_column_type.type.__name__},
        }

    def _validate_sqlalchemy(self, actual_column_type, expected_type, execution_engine):
        if expected_type is None:
            observed = type(actual_column_type).__name__
            return {"success": True, "result": {"observed_value": observed}}
        success, observed_value = compare_column_type(
            execution_engine, actual_column_type, expected_type
        )
        return {"success": success, "result": {"observed_value": observed_value}}

    def _validate_spark(self, actual_column_type, expected_type):
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
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        from great_expectations.execution_engine import (
            SparkDFExecutionEngine,
            SqlAlchemyExecutionEngine,
        )

        column_name = self._get_success_kwarg("column")
        expected_type = self._get_success_kwarg("type_")
        actual_column_types_list = metrics.get("table.column_types", [])
        matches = [
            type_dict["type"]
            for type_dict in actual_column_types_list
            if type_dict["name"] == column_name
        ]
        if not matches:
            return {"success": False, "result": {"observed_value": None}}

        actual_column_type = matches[0]

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            return self._validate_sqlalchemy(
                actual_column_type=actual_column_type,
                expected_type=expected_type,
                execution_engine=execution_engine,
            )
        elif isinstance(execution_engine, SparkDFExecutionEngine):
            return self._validate_spark(
                actual_column_type=actual_column_type, expected_type=expected_type
            )
        return self._validate_pandas(
            actual_column_type=actual_column_type, expected_type=expected_type
        )
