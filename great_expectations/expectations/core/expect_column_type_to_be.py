from __future__ import annotations

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
    CASE_INSENSITIVE_DIALECTS,
    compare_column_type,
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
            For example, valid types for Pandas Datasources include any dtype name pandas \
            itself accepts, such as numpy dtype names ('int64', 'float64', 'object'), pandas \
            nullable/extension dtype names ('Int64', 'boolean', 'string', 'category'), or \
            aliases pandas resolves to a dtype ('int', 'float'). \
            This is a schema-level check against the column's exact dtype: 'int64' and 'Int64' \
            are different types, as are 'int64' and 'int32', and 'str' and 'string'; a datetime \
            unit or time zone, when given, must match; and an object-dtype column matches only \
            an object type request ('object' or 'O'). The storage of a string dtype \
            ('python' or 'pyarrow') is not checked. \
            'category' matches any categorical column, 'interval' any interval column, and \
            'datetime64' or 'timedelta64' any time-zone-naive numpy column of that kind; other \
            names match only the exact dtype they name. \
            For a SqlAlchemy Datasource, type_ matches when it is the type name reported as the \
            observed value, compared case-insensitively, such as 'INTEGER' in most SQL dialects \
            and 'TEXT' in dialects such as postgresql. On dialects other than postgresql, \
            snowflake, SQL Server, databricks and trino, type_ may also name a type class \
            exported by the dialect's SQLAlchemy module, which matches every column that is an \
            instance of it. Dialect aliases are not resolved: use the observed type name, for \
            example 'INTEGER' rather than 'int4' on postgresql. \
            Valid types for Spark Datasources are pyspark DataType class names such as \
            'StringType' and 'BooleanType'; an abstract class such as 'NumericType' matches every \
            type in that family. Spark SQL type names such as 'int' are not accepted. \
            An unrecognized type_ raises an error on Pandas and Spark Datasources. SQL dialects \
            expose no reliable list of their types, so on a SqlAlchemy Datasource an \
            unrecognized type_ returns success=False with the column's actual type as the \
            observed value. A column that does not exist raises an error.

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
        [{SUPPORTED_DATA_SOURCES[13]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Issues:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
            A SQLite table created as (test FLOAT, test2 INTEGER):

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

        template_str = "must be of type $type_."
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
    ) -> list[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")

        kwargs = configuration.kwargs if configuration is not None else {}

        params = substitute_none_for_missing(
            kwargs,
            ["column", "type_"],
        )

        template_str = "must be of type $type_."
        if include_column_name:
            template_str = f"$column {template_str}"

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
        from pandas.api.types import pandas_dtype

        try:
            parsed = pandas_dtype(expected_type)
        except Exception as e:
            # pandas_dtype is the parser for the requested name; whatever it raises (a
            # TypeError, or an ImportError for a pyarrow dtype without pyarrow installed)
            # means pandas cannot construct the requested type.
            msg = f"Unrecognized pandas type: {expected_type} ({e})"
            raise ValueError(msg) from e

        if (
            isinstance(parsed, np.dtype)
            and parsed.kind in "mM"
            and np.datetime_data(parsed)[0] == "generic"
        ):
            # A unit-less "datetime64"/"timedelta64" names the kind, not a resolution:
            # pandas versions differ in the default unit they produce for the same data.
            success = (
                isinstance(actual_column_type, np.dtype) and actual_column_type.kind == parsed.kind
            )
        elif isinstance(actual_column_type, pd.StringDtype) and isinstance(parsed, pd.StringDtype):
            # Every StringDtype compares equal to the name "string", but "str" (NaN for
            # missing values) and "string" (pd.NA) are distinct column types. The storage
            # ("python" or "pyarrow") is deliberately not compared.
            success = str(actual_column_type) == str(parsed)
        else:
            # Compare dtypes, not their scalar `.type`: nullable and non-nullable dtypes
            # (e.g. Int64 and int64) share a scalar type but are distinct column types.
            # The dtype's own comparison against the requested name lets a parameter-free
            # name such as "category" or "interval" match any dtype of that family; the
            # parsed dtype matches spellings pandas normalizes, such as a lower-case time zone.
            success = actual_column_type in (parsed, expected_type)

        return {
            "success": success,
            "result": {"observed_value": str(actual_column_type)},
        }

    def _validate_sqlalchemy(self, actual_column_type, expected_type, execution_engine):
        success, observed_value = compare_column_type(
            execution_engine, actual_column_type, expected_type
        )
        if not success and execution_engine.dialect_name not in CASE_INSENSITIVE_DIALECTS:
            # Where types are compared by SQLAlchemy class, the observed value is the class
            # name, so naming it must match too -- including where the dialect module does not
            # export that name, or resolves it to a dialect-specific class the reflected type is
            # not an instance of. Case-insensitive dialects already compare type_ against the
            # observed type name.
            success = str(observed_value).casefold() == expected_type.casefold()
        return {"success": success, "result": {"observed_value": observed_value}}

    def _validate_spark(self, actual_column_type, expected_type):
        # Only DataType subclasses name a Spark type; pyspark.types also exports helpers,
        # typing aliases and imported modules that must not be treated as types.
        type_class = getattr(pyspark.types, expected_type, None)
        is_data_type = isinstance(type_class, type) and issubclass(
            type_class, pyspark.types.DataType
        )
        if not is_data_type:
            msg = f"Unrecognized spark type: {expected_type}"
            raise ValueError(msg)
        return {
            "success": isinstance(actual_column_type, type_class),
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
        from great_expectations.expectations.metrics.util import (
            get_dbms_compatible_column_names,
        )

        column_name = self._get_success_kwarg("column")
        expected_type = self._get_success_kwarg("type_")
        actual_column_types_list = metrics.get("table.column_types", [])
        batch_column_names = [type_dict["name"] for type_dict in actual_column_types_list]
        # Resolve the column the way other column expectations do: case-insensitively unless
        # quoted, and accepting Spark's unescaped dotted names. Raises if it does not exist.
        resolved_name = get_dbms_compatible_column_names(
            column_names=column_name, batch_columns_list=batch_column_names
        )
        if resolved_name not in batch_column_names:
            # An explicitly quoted name resolves to the name as the user wrote it; find the
            # column it was matched against, unquoting it the same way.
            quoted = str(resolved_name).casefold()
            unquoted = {quoted.strip('"'), quoted.strip("[]"), quoted.strip("`")}
            resolved_name = next(
                name for name in batch_column_names if str(name).casefold() in unquoted
            )
        actual_column_type = actual_column_types_list[batch_column_names.index(resolved_name)][
            "type"
        ]

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
