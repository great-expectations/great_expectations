from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Tuple, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the Z-scores of a column's values to be less than a given threshold."
)
THRESHOLD_DESCRIPTION = (
    "A maximum Z-score threshold. "
    "All column Z-scores that are lower than this threshold will evaluate successfully."
)
DOUBLE_SIDED_DESCRIPTION = (
    "A True or False value indicating whether to evaluate double sidedly. Examples: "
    "(double_sided = True, threshold = 2) -> Z scores in non-inclusive interval(-2,2) | "
    "(double_sided = False, threshold = 2) -> Z scores in non-inclusive interval (-infinity,2)"
)
DATA_QUALITY_ISSUES = ["Distribution"]
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


class ExpectColumnValueZScoresToBeLessThan(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValueZScoresToBeLessThan is a \
    Column Map Expectation \
    for typed-column backends, and also for PandasExecutionEngine where the column \
    dtype and provided type_ are unambiguous constraints \
    (any dtype except 'object' or dtype of 'object' with type_ specified as 'object').

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        threshold (number): \
            {THRESHOLD_DESCRIPTION}
        double_sided (boolean): \
            {DOUBLE_SIDED_DESCRIPTION}

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            {MOSTLY_DESCRIPTION} \
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
            0 	1       -100000000000
            1 	1       -1
            2 	1       0
            3   3       1
            4   3       1

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValueZScoresToBeLessThan(
                    column="test",
                    threshold=1.96,
                    double_sided=True
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 5,
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
                ExpectColumnValueZScoresToBeLessThan(
                    column="test2",
                    threshold=1,
                    double_sided=True
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 5,
                    "unexpected_count": 1,
                    "unexpected_percent": 20.0,
                    "partial_unexpected_list": [
                      -100000000000
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 20.0,
                    "unexpected_percent_nonmissing": 20.0
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    condition_parser: Union[str, None] = "pandas"
    threshold: Union[float, SuiteParameterDict] = pydantic.Field(description=THRESHOLD_DESCRIPTION)
    double_sided: Union[bool, SuiteParameterDict] = pydantic.Field(
        description=DOUBLE_SIDED_DESCRIPTION
    )
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "column",
        "row_condition",
        "condition_parser",
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\  # noqa: E501
    map_metric = "column_values.z_score.under_threshold"
    success_keys = ("threshold", "double_sided", "mostly")
    args_keys = ("column", "threshold")

    class Config:
        title = "Expect column value z-scores to be less than"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValueZScoresToBeLessThan]
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

    @override
    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("threshold", RendererValueType.NUMBER),
            ("double_sided", RendererValueType.BOOLEAN),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if renderer_configuration.include_column_name:
            template_str = "$column value z-scores must be "
        else:
            template_str = "Value z-scores must be "

        if params.double_sided.value is True:
            inverse_threshold = params.threshold.value * -1
            renderer_configuration.add_param(
                name="inverse_threshold", param_type=RendererValueType.NUMBER
            )
            if inverse_threshold < params.threshold.value:
                template_str += "greater than $inverse_threshold and less than $threshold"
            else:
                template_str += "greater than $threshold and less than $inverse_threshold"
        else:
            template_str += "less than $threshold"

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        renderer_configuration.template_str = template_str

        return renderer_configuration
