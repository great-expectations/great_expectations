from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.util import (
    add_values_with_json_schema_from_list_in_params,
    render_evaluation_parameter_string,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectColumnDistinctValuesToBeContinuous(ColumnAggregateExpectation):
    """Expect the set of distinct column values to be continuous."""

    examples = [
        {
            "data": {
                "a": [
                    "2021-01-01",
                    "2021-01-31",
                    "2021-02-28",
                    "2021-03-20",
                    "2021-02-21",
                    "2021-05-01",
                    "2021-06-18",
                ]
            },
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "fail_for_missing_date",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "datetime_format": "%Y-%m-%d"},
                    "out": {"success": False},
                },
            ],
        },
        {
            "data": {
                "a": [
                    "2021-01-01 10:56:30",
                    "2021-01-03 10:56:30",
                    "2021-01-02 10:56:30",  # out of order row to make sure we're ignoring order
                    "2021-01-04 10:56:30",
                    "2021-01-05 10:56:30",
                    "2021-01-06 10:56:30",
                    "2021-01-07 10:56:30",
                ]
            },
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "pass_for_continuous_date",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "datetime_format": "%Y-%m-%d %H:%M:%S"},
                    "out": {"success": True},
                },
            ],
        },
        {
            "data": {"a": [2, 3, 4, 5, 6, 7, 8, 9, 1]},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "pass_for_continuous_integers",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a"},
                    "out": {"success": True},
                },
            ],
        },
        {
            "data": {"a": [1, 2, 3, 4, 5, 8, 9]},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "fail_for_non_continuous_integers",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a"},
                    "out": {"success": False},
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@jmoskovc"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": False,
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("column.value_counts", "column.max", "column.min")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("column", "datetime_format")

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """Validating that user has inputted a value set and that configuration has been initialized"""
        super().validate_configuration(configuration)

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "datetime_format",
                "row_condition",
                "condition_parser",
            ],
        )
        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "datetime_format": {
                "schema": {"type": "string"},
                "value": params.get("datetime_format"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }

        template_str = "distinct values must be continuous."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = f"{conditional_template_str}, then {template_str}"
            params_with_json_schema.update(conditional_params)

        params_with_json_schema = add_values_with_json_schema_from_list_in_params(
            params=params, params_with_json_schema=params_with_json_schema
        )

        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "datetime_format",
                "row_condition",
                "condition_parser",
            ],
        )

        template_str = "distinct values must be continuous."

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
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    def _expected_list(
        self,
        start_value: Any,
        end_value: Any,
        configuration: ExpectationConfiguration,
    ):
        datetime_format = configuration.kwargs.get("datetime_format")
        if datetime_format:
            try:
                # user defined datetime_format, so we're expecting to handle dates
                start_date = datetime.strptime(start_value, datetime_format)
                end_date = datetime.strptime(end_value, datetime_format)
                return [
                    (start_date + timedelta(days=x)).strftime("%Y-%m-%d")
                    for x in range((end_date - start_date).days + 1)
                ]
            except TypeError as ex:
                raise InvalidExpectationConfigurationError(
                    f"Expecting datetime when datetime_format is set\n{ex}"
                )
        # else - no datetime format, so we're assuming integers
        return [x for x in range(start_value, end_value + 1)]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        observed_value_counts = metrics.get("column.value_counts", [])
        observed_max = metrics.get("column.max")
        observed_min = metrics.get("column.min")

        datetime_format = configuration.kwargs.get("datetime_format")
        # need to strip the time part from the datetime strings
        if datetime_format is not None:
            observed_set = set(
                map(
                    lambda x: datetime.strptime(x, datetime_format).strftime(
                        "%Y-%m-%d"
                    ),
                    observed_value_counts.index,
                )
            )
        else:
            observed_set = set(observed_value_counts.index)

        expected_set = set(
            self._expected_list(observed_min, observed_max, configuration)
        )

        return {
            "success": expected_set == observed_set,
            "result": {
                "observed_value": f"Missing values {sorted(expected_set - observed_set)}",
            },
        }


if __name__ == "__main__":
    ExpectColumnDistinctValuesToBeContinuous().print_diagnostic_checklist()
