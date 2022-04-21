import json
from typing import Callable, Dict, Optional

from numpy import array

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    ExecutionEngine,
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
    ExpectationValidationResult,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnPairValuesDiffThree(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.diff_three"
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        return abs(column_A - column_B) == 3

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, **kwargs):
        row_wise_cond = sa.and_(
            sa.func.abs(column_A - column_B) == 3,
            sa.not_(sa.or_(column_A == None, column_B == None)),
        )
        return row_wise_cond

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, **kwargs):
        row_wise_cond = F.abs(column_A - column_B) == 3
        return row_wise_cond


class ExpectColumnPairValuesToHaveDifferenceOfThree(ColumnPairMapExpectation):
    """Expect two columns to have a row-wise difference of three."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [3, 0, 1, 2, 3, 2],
                "col_b": [0, -3, 4, -1, 0, 1],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "col_a", "column_B": "col_b", "mostly": 0.8},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "col_a", "column_B": "col_b", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": [
            "basic math",
            "multi-column expectation",
        ],
        "contributors": ["@joegargery"],
    }

    map_metric = "column_pair_values.diff_three"

    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ignore_row_if": "both_values_are_missing",
        "mostly": 1.0,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column_A",
        "column_B",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert (
                "column_A" in configuration.kwargs
                and "column_B" in configuration.kwargs
            ), "both columns must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


#
if __name__ == "__main__":
    ExpectColumnPairValuesToHaveDifferenceOfThree().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnPairValuesToHaveDifferenceOfThree().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
