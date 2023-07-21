from typing import Dict, Optional

import scipy.stats as stats

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.metrics.metric_provider import (
    MetricConfiguration,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


class ColumnChisquareTestPValueGreaterThan(TableMetricProvider):
    # This is the id string that will be used to reference your Metric.
    metric_name = "column.p_value_greater_than_threshold"
    value_keys = (
        "observed_column",
        "expected_column",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        # metric value kwargs: kwargs passed in through the expectation
        observed_column = metric_value_kwargs.get("observed_column")
        expected_column = metric_value_kwargs.get("expected_column")

        observed_values = df[observed_column].to_list()
        expected_values = df[expected_column].to_list()

        chi2, p_value = stats.chisquare(observed_values, expected_values)

        return chi2, p_value

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


class ExpectColumnChisquareSimpleTestPValueToBeGreaterThan(BatchExpectation):
    """Expect the chi-squared of 2 columns to have a p-value greater than the provided threshold."""

    examples = [
        {
            "data": {"x": [30, 45, 25, 20], "y": [40, 40, 20, 20]},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "observed_column": "x",
                        "expected_column": "y",
                        "p_value_threshold": 0.1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "observed_column": "x",
                        "expected_column": "y",
                        "p_value_threshold": 0.5,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.p_value_greater_than_threshold",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = (
        "p_value_threshold",
        "observed_column",
        "expected_column",
    )

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        threshold = configuration["kwargs"].get("p_value_threshold")
        chi2, p_value = metrics.get("column.p_value_greater_than_threshold")

        success = p_value >= threshold

        return {"success": success, "result": {"observed_value": p_value}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "statistical",
            "test",
            "testing",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@HaebichanGX",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnChisquareSimpleTestPValueToBeGreaterThan().print_diagnostic_checklist()
