from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationValidationResult,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.render import CollapseContent, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.validator.metric_configuration import MetricConfiguration


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py ColumnValuesEqualThree class_def">
class ColumnValuesEqualThree(ColumnMapMetricProvider):
    # </snippet>

    # This is the id string that will be used to reference your metric.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py metric_name">
    condition_metric_name = "column_values.equal_three"
    # </snippet>

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py pandas">
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3
        # </snippet>

    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py spark_definition">
    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        # </snippet>
        # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py spark_selectable">
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = F.col(column_name)
        # </snippet>
        # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py spark_query">
        query = F.when(column == 3, F.lit(False)).otherwise(F.lit(True))

        return (query, compute_domain_kwargs, accessor_domain_kwargs)
        # </snippet>

    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py sqlalchemy">
    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return column.in_([3])
        # </snippet>

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[Dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""
        dependencies: Dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        table_domain_kwargs: Dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )

        return dependencies


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py ExpectColumnValuesToEqualThree class_def">
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py docstring">
    """Expect values in this column to equal 3."""
    # </snippet>
    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py examples">
    examples = [
        {
            "data": {
                "all_threes": [3, 3, 3, 3, 3],
                "some_zeroes": [3, 3, 3, 0, 0],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_threes"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "some_zeroes", "mostly": 0.8},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]
    # </snippet>

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py map_metric">
    map_metric = "column_values.equal_three"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys
    # for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    @renderer(renderer_type="renderer.diagnostic.observed_value")
    @render_evaluation_parameter_string
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        runtime_configuration: dict = None,
        **kwargs,
    ):
        assert result, "Must provide a result object."

        result_dict = result.result
        if result_dict is None:
            return "--"

        if result_dict.get("observed_value"):
            observed_value = result_dict.get("observed_value")
            if isinstance(observed_value, (int, float)) and not isinstance(
                observed_value, bool
            ):
                return num_to_str(observed_value, precision=10, use_locale=True)
            return str(observed_value)
        elif result_dict.get("unexpected_percent") is not None:
            return (
                num_to_str(result_dict.get("unexpected_percent"), precision=5)
                + "% unexpected"
            )
        else:
            return "--"

    @renderer(renderer_type="renderer.diagnostic.unexpected_statement")
    @render_evaluation_parameter_string
    def _diagnostic_unexpected_statement_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        runtime_configuration: dict = None,
        **kwargs,
    ):
        assert result, "Must provide a result object."

        success = result.success
        result = result.result

        if result.exception_info["raised_exception"]:
            exception_message_template_str = (
                "\n\n$expectation_type raised an exception:\n$exception_message"
            )

            exception_message = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": exception_message_template_str,
                        "params": {
                            "expectation_type": result.expectation_config.expectation_type,
                            "exception_message": result.exception_info[
                                "exception_message"
                            ],
                        },
                        "tag": "strong",
                        "styling": {
                            "classes": ["text-danger"],
                            "params": {
                                "exception_message": {"tag": "code"},
                                "expectation_type": {
                                    "classes": ["badge", "badge-danger", "mb-2"]
                                },
                            },
                        },
                    },
                }
            )

            exception_traceback_collapse = CollapseContent(
                **{
                    "collapse_toggle_link": "Show exception traceback...",
                    "collapse": [
                        RenderedStringTemplateContent(
                            **{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": result.exception_info[
                                        "exception_traceback"
                                    ],
                                    "tag": "code",
                                },
                            }
                        )
                    ],
                }
            )

            return [exception_message, exception_traceback_collapse]

        if success or not result_dict.get("unexpected_count"):
            return []
        else:
            unexpected_count = num_to_str(
                result_dict["unexpected_count"], use_locale=True, precision=20
            )
            unexpected_percent = (
                num_to_str(result_dict["unexpected_percent"], precision=4) + "%"
            )
            element_count = num_to_str(
                result_dict["element_count"], use_locale=True, precision=20
            )

            template_str = (
                "\n\n$unexpected_count unexpected values found. "
                "$unexpected_percent of $element_count total rows."
            )

            return [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": template_str,
                            "params": {
                                "unexpected_count": unexpected_count,
                                "unexpected_percent": unexpected_percent,
                                "element_count": element_count,
                            },
                            "tag": "strong",
                            "styling": {"classes": ["text-danger"]},
                        },
                    }
                )
            ]

    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    @render_evaluation_parameter_string
    def _diagnostic_unexpected_table_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        runtime_configuration: dict = None,
        **kwargs,
    ):
        try:
            result_dict = result.result
        except KeyError:
            return None

        if result_dict is None:
            return None

        if not result_dict.get("partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        if result_dict.get("partial_unexpected_counts"):
            total_count = 0
            for unexpected_count_dict in result_dict.get("partial_unexpected_counts"):
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            if total_count == result_dict.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["Sampled Unexpected Values"]
                table_rows = [[row[0]] for row in table_rows]

        else:
            header_row = ["Sampled Unexpected Values"]
            sampled_values_set = set()
            for unexpected_value in result_dict.get("partial_unexpected_list"):
                if unexpected_value:
                    string_unexpected_value = str(unexpected_value)
                elif unexpected_value == "":
                    string_unexpected_value = "EMPTY"
                else:
                    string_unexpected_value = "null"
                if string_unexpected_value not in sampled_values_set:
                    table_rows.append([unexpected_value])
                    sampled_values_set.add(string_unexpected_value)

        unexpected_table_content_block = RenderedTableContent(
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block

    # This dictionary contains metadata for display in the public gallery
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py library_metadata">
    library_metadata = {
        "tags": ["extremely basic math"],
        "contributors": ["@joegargery"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py diagnostics">
    ExpectColumnValuesToEqualThree().print_diagnostic_checklist()
#     </snippet>

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnValuesToEqualThree().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
