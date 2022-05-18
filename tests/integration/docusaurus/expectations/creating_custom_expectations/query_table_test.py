"""
This is a template for creating custom TableExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations
"""

from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import (
    MetricConfiguration,
    MetricDomainTypes,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


# <snippet>
# This class defines a Metric to support your Expectation.
class TableColumnsUnique(TableMetricProvider):

    # This is the id string that will be used to reference your Metric.
    metric_name = "table.queried.equals_three"
    value_keys = (
        "column",
        "query",
    )

    # This method implements the core logic for the PandasExecutionEngine
    # @metric_value(engine=PandasExecutionEngine)
    # def _pandas(
    #     cls,
    #     execution_engine,
    #     metric_domain_kwargs,
    #     metric_value_kwargs,
    #     metrics,
    #     runtime_configuration,
    # ):
    #     df, _, _ = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
    #     )
    #     breakpoint()
    #     unique_columns = set(df.T.drop_duplicates().T.columns)
    #
    #     return unique_columns

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        breakpoint()
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.TABLE
        )

        sqlalchemy_engine = execution_engine.engine

        breakpoint()

    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine,
    #     metric_domain_kwargs,
    #     metric_value_kwargs,
    #     metrics,
    #     runtime_configuration,
    # ):
    #     raise NotImplementedError

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# </snippet>
# <snippet>
class ExpectTableColumnsToBeUnique(TableExpectation):
    """Expect table to contain columns with unique contents."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col1": [3, 3, 3, 3, 3],
                "col2": [2, 3, 4, 5, 6],
                "col3": [3, 4, 5, 6, 7],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col1",
                        "query": "SELECT CASE WHEN EXISTS (SELECT * FROM [data] WHERE CASE col1 = 3) THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END",
                    },
                    "out": {"success": True},
                }
            ],
        },
        # {
        #     "data": {
        #         "col1": [1, 2, 3, 4, 5],
        #         "col2": [1, 2, 3, 4, 5],
        #         "col3": [3, 4, 5, 6, 7],
        #     },
        #     "tests": [
        #         {
        #             "title": "loose_positive_test",
        #             "exact_match_out": False,
        #             "include_in_gallery": True,
        #             "in": {"strict": False},
        #             "out": {"success": True},
        #         },
        #         {
        #             "title": "strict_negative_test",
        #             "exact_match_out": False,
        #             "include_in_gallery": True,
        #             "in": {"strict": True},
        #             "out": {"success": False},
        #         },
        #     ],
        # },
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("table.queried.equals_three",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("column", "query")

    # This dictionary contains default values for any parameters that should have default values.
    # default_kwarg_values = {"strict": True}

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        breakpoint()
        unique_columns = metrics.get("table.columns.unique")
        table_columns = metrics.get("table.columns")
        strict = configuration.kwargs.get("strict")

        duplicate_columns = unique_columns.symmetric_difference(table_columns)

        if strict is True:
            success = len(duplicate_columns) == 0
        else:
            success = len(duplicate_columns) < len(table_columns)

        return {
            "success": success,
            "result": {"observed_value": {"duplicate_columns": duplicate_columns}},
        }

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["uniqueness"],
        "contributors": ["@joegargery"],
    }


# </snippet>
if __name__ == "__main__":
    ExpectTableColumnsToBeUnique().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectTableColumnsToBeUnique().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
