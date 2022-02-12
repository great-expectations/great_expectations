import json
from typing import Callable, Dict, Optional

from numpy import array

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
    ColumnMapExpectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
    metric_partial,
    metric_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.validator.metric_configuration import MetricConfiguration


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesEqualThree(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.equal_three"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3

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
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = F.col(column_name)

        query = F.when(column == 3, F.lit(False)).otherwise(F.lit(True))

        return (query, compute_domain_kwargs, accessor_domain_kwargs)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return column.in_([3])

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
            metric_dependencies=None,
        )

        return dependencies


# This class defines the Expectation itself
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
    """Expect values in this column to equal 3."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
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

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.equal_three"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys
    # for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["extremely basic math"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToEqualThree().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectColumnValuesToEqualThree().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_message"] is None
    assert check["stack_trace"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    assert check["passed"] is True
