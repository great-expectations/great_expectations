from typing import Dict, Optional, Union

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
    ExpectationValidationResult,
    MulticolumnMapExpectation,
)
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py MulticolumnValuesMultipleThree class_def">
class MulticolumnValuesMultipleThree(MulticolumnMapMetricProvider):
    # </snippet>
    """MetricProvider Class for Multicolumn Values Multiple Of Three MetricProvider"""

    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py condition_metric_name">
    condition_metric_name = "multicolumn_values.multiple_three"
    # </snippet>
    condition_domain_keys = ("column_list",)
    condition_value_keys = ()
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py _pandas">

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        return column_list.apply(lambda x: abs(x) % 3 == 0)

    # </snippet>
    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        return sa.and_(sa.func.abs(x) % 3 == 0 for x in column_list)

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        raise NotImplementedError


# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py ExpectMulticolumnValuesToBeMultiplesOfThree class_def">
class ExpectMulticolumnValuesToBeMultiplesOfThree(MulticolumnMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py docstring">
    """Expect a set of columns to contain multiples of three."""
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py map_metric">
    map_metric = "multicolumn_values.multiple_three"
    # </snippet>
    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py examples">
    examples = [
        {
            "data": {
                "col_a": [3, 6, 9, 12, -3],
                "col_b": [0, -3, 6, 33, -9],
                "col_c": [1, 5, 6, 27, 3],
                "col_d": [-2, 3, 21, 0, 1],
            },
            "only_for": ["pandas", "sqlite", "postgresql"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_b", "col_c"], "mostly": 0.6},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_b", "col_c", "col_d"], "mostly": 0.5},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]
    # </snippet>
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    success_keys = (
        "column_list",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1.0,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("column_list",)

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        try:
            assert "column_list" in configuration.kwargs, "column_list must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This dictionary contains metadata for display in the public gallery
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py library_metadata">
    library_metadata = {
        "tags": [
            "basic math",
            "multi-column expectation",
        ],
        "contributors": ["@joegargery"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py print_diagnostic_checklist">
    ExpectMulticolumnValuesToBeMultiplesOfThree().print_diagnostic_checklist()
    # </snippet>
# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectMulticolumnValuesToBeMultiplesOfThree().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
