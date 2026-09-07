from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Set, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TC001 # FIXME CoP
)
from great_expectations.exceptions.exceptions import InvalidSetTypeError
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.metadata_types import DataQualityIssues, SupportedDataSources
from great_expectations.expectations.model_field_descriptions import FAILURE_SEVERITY_DESCRIPTION
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedStringTemplateContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.observed_value_renderer import ObservedValueRenderState
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
    from great_expectations.expectations.metrics.util import CaseInsensitiveString
    from great_expectations.render.renderer_configuration import AddParamArgs


EXPECTATION_SHORT_DESCRIPTION = "Expect the columns in a table to match an unordered set."
COLUMN_SET_DESCRIPTION = (
    "The column names, in any order. In SQL datasources, if the column names are "
    "double quoted, for example '\"column_name\"', a case sensitive match is "
    "done. Otherwise a case insensitive match is done."
)
EXACT_MATCH_DESCRIPTION = (
    "If True, the list of columns must exactly match the observed columns. "
    "If False, observed columns must include column_set but additional columns will pass."
)
EXACT_MATCH_DEFAULT = True
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


class ExpectTableColumnsToMatchSet(BatchExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectTableColumnsToMatchSet is a \
    Batch Expectation.

    BatchExpectations are one of the most common types of Expectation.
    They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        column_set (list of str): {COLUMN_SET_DESCRIPTION}
        exact_match (boolean): \
            {EXACT_MATCH_DESCRIPTION} Default True.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
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
                test 	test2
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

    Code Examples:
        Passing Case:
            Input:
                ExpectTableColumnsToMatchSet(
                    column_set=["test"],
                    exact_match=False
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": [
                      "test",
                      "test2"
                    ],
                    "details": {{
                      "mismatched": {{
                        "unexpected": [
                          "test2"
                        ]
                      }}
                    }}
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectTableColumnsToMatchSet(
                    column_set=["test2", "test3"],
                    exact_match=True
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": [
                      "test",
                      "test2"
                    ],
                    "details": {{
                      "mismatched": {{
                        "unexpected": [
                          "test"
                        ],
                        "missing": [
                          "test3"
                        ]
                      }}
                    }}
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501 # FIXME CoP

    column_set: Union[list, set, SuiteParameterDict, None] = pydantic.Field(
        description=COLUMN_SET_DESCRIPTION
    )
    exact_match: Union[bool, SuiteParameterDict, None] = pydantic.Field(
        default=EXACT_MATCH_DEFAULT, description=EXACT_MATCH_DESCRIPTION
    )

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    metric_dependencies = ("table.columns",)
    success_keys = (
        "column_set",
        "exact_match",
    )
    args_keys = (
        "column_set",
        "exact_match",
    )

    class Config:
        title = "Expect table columns to match set"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectTableColumnsToMatchSet]) -> None:
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
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_set", RendererValueType.ARRAY),
            ("exact_match", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.column_set:
            template_str = "Must specify a set or list of columns."
        else:
            array_param_name = "column_set"
            param_prefix = "column_set_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            column_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

            exact_match = (
                params.exact_match.value if params.exact_match is not None else EXACT_MATCH_DEFAULT
            )
            exact_match_str = "at least" if not exact_match else "exactly"

            template_str = (
                f"Must have {exact_match_str} these columns (in any order): {column_set_str}"
            )

        renderer_configuration.template_str = template_str

        return renderer_configuration

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
        _ = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["column_set", "exact_match"])

        if params["column_set"] is None:
            template_str = "Must specify a set or list of columns."

        else:
            # standardize order of the set for output
            params["column_list"] = list(params["column_set"])

            column_list_template_str = ", ".join(
                [f"$column_list_{idx}" for idx in range(len(params["column_list"]))]
            )

            exact_match = (
                params["exact_match"] if params["exact_match"] is not None else EXACT_MATCH_DEFAULT
            )
            exact_match_str = "at least" if not exact_match else "exactly"

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_list_template_str}"  # noqa: E501 # FIXME CoP

            for idx in range(len(params["column_list"])):
                params[f"column_list_{idx!s}"] = params["column_list"][idx]

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

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    @override
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        expected_param_prefix = "exp__"
        expected_param_name = "expected_value"
        ov_param_prefix = "ov__"
        ov_param_name = "observed_value"

        renderer_configuration.add_param(
            name=expected_param_name,
            param_type=RendererValueType.ARRAY,
            value=renderer_configuration.kwargs.get("column_set", []),
        )
        renderer_configuration = cls._add_array_params(
            array_param_name=expected_param_name,
            param_prefix=expected_param_prefix,
            renderer_configuration=renderer_configuration,
        )

        renderer_configuration.add_param(
            name=ov_param_name,
            param_type=RendererValueType.ARRAY,
            value=result.get("result", {}).get("observed_value", []) if result else [],
        )
        renderer_configuration = cls._add_array_params(
            array_param_name=ov_param_name,
            param_prefix=ov_param_prefix,
            renderer_configuration=renderer_configuration,
        )

        observed_columns = (
            (name, sch)
            for name, sch in renderer_configuration.params
            if name.startswith(ov_param_prefix)
        )
        expected_columns = (
            (name, sch)
            for name, sch in renderer_configuration.params
            if name.startswith(expected_param_prefix)
        )
        mismatched_columns = {"unexpected": [], "missing": []}
        if (
            "details" in result["result"]
            and "mismatched" in result["result"]["details"]
            and result["result"]["details"]["mismatched"]
        ):
            mismatched_columns.update(result["result"]["details"]["mismatched"])

        template_str_list = []
        for name, schema in observed_columns:
            render_state = (
                ObservedValueRenderState.UNEXPECTED.value
                if schema.value in mismatched_columns["unexpected"]
                else ObservedValueRenderState.EXPECTED.value
            )
            renderer_configuration.params.__dict__[name].render_state = render_state
            template_str_list.append(f"${name}")

        for name, schema in expected_columns:
            if schema.value in mismatched_columns["missing"]:
                renderer_configuration.params.__dict__[
                    name
                ].render_state = ObservedValueRenderState.MISSING.value
                template_str_list.append(f"${name}")

        renderer_configuration.template_str = " ".join(template_str_list)

        value_obj = renderedAtomicValueSchema.load(
            {
                "template": renderer_configuration.template_str,
                "params": renderer_configuration.params.dict(),
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        return RenderedAtomicContent(
            name=AtomicDiagnosticRendererType.OBSERVED_VALUE,
            value=value_obj,
            value_type="StringValueType",
        )

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        from great_expectations.execution_engine import SqlAlchemyExecutionEngine

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            return self._validate_sqlalchemy(metrics)

        # Retrieve expected and observed column names
        expected_column_list = self._get_success_kwargs().get("column_set")
        expected_column_set = (
            set(expected_column_list) if expected_column_list is not None else set()
        )
        actual_column_list = metrics.get("table.columns")
        actual_column_set = set(actual_column_list)

        unmatched_actual_column_set = actual_column_set - expected_column_set
        unmatched_expected_column_set = expected_column_set - actual_column_set
        return _validate_result(
            actual_column_set,
            expected_column_set,
            unmatched_actual_column_set,
            unmatched_expected_column_set,
            self._get_success_kwargs().get("exact_match"),
        )

    def _validate_sqlalchemy(self, metrics: Dict):
        # We want to match the expected columns with the actual columns. We first break up the
        # expected columns into 2 sets, the quoted columns which must match exactly and the unquoted
        # columns, which we case insensitive match.
        expected_column_set = set(self._get_success_kwargs().get("column_set"))
        quoted_expected_column_set = set()
        unquoted_expected_column_set = set()
        for col in expected_column_set:
            if col.startswith('"') and col.endswith('"'):
                quoted_expected_column_set.add(col[1:-1])
            else:
                unquoted_expected_column_set.add(col)

        # The actual columns from the db will be unquoted and may be strs or CaseInsensitiveStrings.
        # We normalize the actual_column_list to CaseInsensitiveStrings so we can use set operations
        # going forward.
        actual_column_list = metrics.get("table.columns")
        actual_column_set = _make_case_insensitive_set(actual_column_list)

        # We make copies of the expected and actual column sets and remove items from them as we
        # find matches between the 2 sets.
        unmatched_expected_column_set = expected_column_set.copy()
        unmatched_actual_column_set = actual_column_set.copy()

        # We first match quoted strings. The expected set is a set of strs while the actual set
        # is a set of CaseInsensitiveStrings so we can't use set operations.
        for col in actual_column_set:
            if str(col) in quoted_expected_column_set:
                unmatched_expected_column_set.remove(f'"{col!s}"')
                unmatched_actual_column_set.remove(col)

        # We normalize the unmatched_expected_column_set to CaseInsensitiveStrings
        unmatched_expected_column_set = _make_case_insensitive_set(unmatched_expected_column_set)

        # We now do the unquoted match
        unquoted_expected_column_set = _make_case_insensitive_set(unquoted_expected_column_set)
        unquoted_matches = unquoted_expected_column_set.intersection(unmatched_actual_column_set)

        # We subtract the unquoted matches from the current unmatched sets to finalize them
        unmatched_actual_column_set = unmatched_actual_column_set - unquoted_matches
        unmatched_expected_column_set = unmatched_expected_column_set - unquoted_matches

        return _validate_result(
            actual_column_set,
            expected_column_set,
            unmatched_actual_column_set,
            unmatched_expected_column_set,
            self._get_success_kwargs().get("exact_match"),
        )


def _make_case_insensitive_set(
    str_set: Optional[set[str | CaseInsensitiveString]],
) -> set[CaseInsensitiveString]:
    """
    Transforms a set of strs to CaseInsensitiveStrings.

    Args:
        str_set: A set of strs.

    Returns:
        A set of CaseInsensitiveString.
    """
    from great_expectations.expectations.metrics.util import (
        CaseInsensitiveString,
    )

    if str_set is None:
        return set()

    case_insensitive_strs = set()
    for s in str_set:
        if isinstance(s, CaseInsensitiveString):
            case_insensitive_strs.add(s)
        elif isinstance(s, str):
            case_insensitive_strs.add(CaseInsensitiveString(s))
        else:
            raise InvalidSetTypeError(
                expected_type="str or CaseInsensitiveString", actual_type=str(type(s))
            )
    return case_insensitive_strs


def _validate_result(
    actual_column_set: Set[Union[str, CaseInsensitiveString]],
    expected_column_set: Set[str],
    unmatched_actual_column_set: Set[Union[str, CaseInsensitiveString]],
    unmatched_expected_column_set: Set[Union[str, CaseInsensitiveString]],
    exact_match: bool,
) -> Dict[str, Any]:
    empty_set = set()
    observed_value = sorted([str(col) for col in actual_column_set])

    if ((expected_column_set is None) and (exact_match is not True)) or (
        unmatched_expected_column_set == empty_set and unmatched_actual_column_set == empty_set
    ):
        return {"success": True, "result": {"observed_value": observed_value}}
    else:
        unexpected_list = sorted([str(col) for col in unmatched_actual_column_set])
        missing_list = sorted([str(col) for col in unmatched_expected_column_set])

        mismatched = {}
        if len(unexpected_list) > 0:
            mismatched["unexpected"] = unexpected_list
        if len(missing_list) > 0:
            mismatched["missing"] = missing_list

        result = {
            "observed_value": observed_value,
            "details": {"mismatched": mismatched},
        }

        return_success = {
            "success": True,
            "result": result,
        }
        return_failed = {
            "success": False,
            "result": result,
        }

        if exact_match:
            return return_failed
        else:  # noqa: PLR5501 # FIXME CoP
            # Failed if there are items in the missing list (but OK to have unexpected_list)
            if len(missing_list) > 0:
                return return_failed
            # Passed if there are no items in the missing list
            else:
                return return_success
