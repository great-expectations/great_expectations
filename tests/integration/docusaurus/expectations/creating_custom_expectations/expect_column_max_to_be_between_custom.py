from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnExpectation,
    ExpectationValidationResult,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    MetricDomainTypes,
    MetricFunctionTypes,
    column_aggregate_partial,
    column_aggregate_value,
    metric_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ColumnCustomMax(ColumnAggregateMetricProvider):
    """MetricProvider Class for Custom Aggregate Max MetricProvider"""

    metric_name = "column.custom_max"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Max Implementation"""
        return column.max()

    @metric_value(
        engine=SqlAlchemyExecutionEngine,
        metric_fn_type=MetricFunctionTypes.AGGREGATE_VALUE,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        sqlalchemy_engine = execution_engine.engine

        query = sa.select(sa.func.max(column)).select_from(selectable)
        result = sqlalchemy_engine.execute(query).fetchone()

        return result[0]

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Max Implementation"""
        return F.max(column)


class ExpectColumnMaxToBeBetweenCustom(ColumnExpectation):
    """Expect column max to be between a given range."""

    # Defining test cases
    examples = [
        {
            "data": {"x": [1, 2, 3, 4, 5], "y": [0, -1, -2, 4, None]},
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "min_value": 4,
                        "strict_min": True,
                        "max_value": 5,
                        "strict_max": False,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "min_value": -2,
                        "strict_min": False,
                        "max_value": 3,
                        "strict_max": True,
                    },
                    "out": {"success": False},
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite", "postgresql"],
                },
                {
                    "backend": "spark",
                    "dialects": None,
                },
            ],
        }
    ]

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    metric_dependencies = ("column.custom_max",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "mostly": 1,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        min_value = configuration.kwargs["min_value"]
        max_value = configuration.kwargs["max_value"]
        strict_min = configuration.kwargs["strict_min"]
        strict_max = configuration.kwargs["strict_max"]

        # Validating that min_val, max_val, strict_min, and strict_max are of the proper format and type
        try:
            assert (
                min_value is not None or max_value is not None
            ), "min_value and max_value cannot both be none"
            assert min_value is None or isinstance(
                min_value, (float, int)
            ), "Provided min threshold must be a number"
            assert max_value is None or isinstance(
                max_value, (float, int)
            ), "Provided max threshold must be a number"
            if min_value and max_value:
                assert (
                    min_value <= max_value
                ), "Provided min threshold must be less than or equal to max threshold"
            assert strict_min is None or isinstance(
                strict_min, bool
            ), "strict_min must be a boolean value"
            assert strict_max is None or isinstance(
                strict_max, bool
            ), "strict_max must be a boolean value"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        return True

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the column max"""
        column_max = metrics["column.custom_max"]

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_max}}

    @renderer(renderer_type="render.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        language: str = None,
        runtime_configuration: dict = None,
        **kwargs,
    ):
        assert (
            configuration or result
        ), "Must provide renderers either a configuration or result."

        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        # get params dict with all expected kwargs
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        # build the string, parameter by parameter
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "maximum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"maximum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"maximum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"maximum value must be {at_least_str} $min_value."
            else:
                template_str = ""

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
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

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["flexible max comparisons"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectColumnMaxToBeBetweenCustom().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnMaxToBeBetweenCustom().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_message"] is None
    assert check["stack_trace"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
