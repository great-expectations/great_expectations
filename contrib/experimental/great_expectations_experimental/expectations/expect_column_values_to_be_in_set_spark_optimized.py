import warnings
from typing import Dict, Optional

import numpy as np

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.exceptions.exceptions import MetricResolutionError
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import ColumnAggregateMetricProvider
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    MetricDomainTypes,
    MetricPartialFunctionTypes,
    column_condition_partial,
    metric_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_value


# This class defines a Metric to support your Expectation.
# For most ColumnExpectations, the main business logic for calculation will live in this class.
class ColumnValuesInSetSparkOptimized(ColumnAggregateMetricProvider):

    metric_name = "column_values.in_set.spark_optimized"
    value_keys = (
        "column",
        "value_set",
    )

    @metric_value(
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
        **kwargs,
    ):
        column_name = metric_domain_kwargs.get("column")
        value_set = metric_value_kwargs.get("value_set")
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        try:
            value_set = execution_engine.spark.createDataFrame(
                data=value_set, schema=df.schema[column_name].dataType
            )
        except TypeError as e:
            raise InvalidExpectationConfigurationError(
                f"`value_set` must be of same type as `column` : {e}"
            )

        joined = df.join(value_set, df[column_name] == value_set["value"], "left")
        success = joined.withColumn(
            "__success", F.when(joined["value"].isNull(), False).otherwise(True)
        )

        return success.select(column_name, "__success").collect()


# This class defines the Expectation itself
class ExpectColumnValuesToBeInSetSparkOptimized(ColumnExpectation):
    """Expect each column value to be in a given set; optimized using **join** for spark backends.

    Args:
        column (str): \
            The column name.
        value_set (set-like): \
            A set of objects used for comparison.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
        strict (boolean or None) : If True, percentage of values in set must exceed mostly.
    """

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column_values.in_set.spark_optimized",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("column", "value_set", "mostly", "strict")

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {"mostly": 1, "strict": True, "value_set": []}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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
        if configuration is None:
            configuration = self.configuration
        value_set = configuration.kwargs.get(
            "value_set"
        ) or self.default_kwarg_values.get("value_set")
        column = configuration.kwargs.get("column")

        try:
            assert column is not None, "`column` must be specified"
            assert (
                "value_set" in configuration.kwargs or value_set
            ), "value_set is required"
            assert isinstance(
                value_set, (list, set, dict)
            ), "value_set must be a list, set, or dict"
            if isinstance(value_set, dict):
                assert (
                    "$PARAMETER" in value_set
                ), 'Evaluation Parameter dict for value_set kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        mostly = configuration["kwargs"].get("mostly")
        strict = configuration["kwargs"].get("strict")
        result = metrics.get("column_values.in_set.spark_optimized")
        result = dict(result)

        if strict is True and mostly < 1:
            success = (sum(result.values()) / len(result.values())) > mostly
        else:
            success = (sum(result.values()) / len(result.values())) >= mostly

        return {"success": success, "result": {"observed_values": result}}

    examples = [
        {
            "data": {
                "col1": ["ES", "BE", "FR", "DE", "CH"],
                "col2": [1, 2, 3, 5, 8],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col1",
                        "value_set": ["ES", "BE", "UK"],
                        "mostly": 0.4,
                        "strict": False,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value_set": [3, 8, 1, 22, 74],
                        "mostly": 0.6,
                        "strict": True,
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "failing_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value_set": ["ES", "BE", "UK"],
                        "mostly": 0.6,
                        "strict": True,
                        "catch_exceptions": True,
                    },
                    "out": {},
                    "error": {
                        "traceback_substring": "`value_set` must be of same type as `column`",
                    },
                },
            ],
            "only_for": ["spark"],
        }
    ]

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "column values in set",
            "experimental",
            "spark optimized",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@austiezr",
            "@derek-hk",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeInSetSparkOptimized().print_diagnostic_checklist()
