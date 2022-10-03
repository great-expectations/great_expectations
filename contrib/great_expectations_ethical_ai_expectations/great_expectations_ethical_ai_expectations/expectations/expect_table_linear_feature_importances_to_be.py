import json

# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
from typing import Any, Dict, Optional, Tuple

from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
    TableExpectation,
)
from great_expectations.expectations.metrics import MetricDomainTypes, metric_value
from great_expectations.expectations.metrics.metric_provider import MetricConfiguration
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableModelingRidgeFeatureImportances(TableMetricProvider):

    metric_name = "table.modeling.linear.feature_importances"
    value_keys = ("y_column",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        X, y = (
            df.drop(columns=[metric_value_kwargs["y_column"]]),
            df[metric_value_kwargs["y_column"]],
        )
        model = LinearRegression().fit(X, y)
        importances = permutation_importance(
            model,
            X,
            y,
            n_repeats=30,
            random_state=42,
            scoring="neg_mean_absolute_percentage_error",
        )

        return {i: j for i, j in zip(X.columns, importances.importances_mean)}

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine=None,
        runtime_configuration=None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectTableLinearFeatureImportancesToBe(TableExpectation):
    """Expect Feature Importances of specified columns in table for Linear Regression to meet threshold."""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "x_1": [3, 5, 7],
                "x_2": [1, 1, 1],
                "x_3": [0.01, 0.02, 0.01],
                "y": [0.031, 0.052, 0.071],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "n_features": 1,
                        "important_columns": ["x_1"],
                        "y_column": "y",
                        "threshold": 0.35,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"n_features": 2, "y_column": "y", "threshold": 0.35},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["ai/ml", "fair-ai", "hackathon-22"],
        "contributors": ["@austiezr"],
        "requirements": ["sklearn"],
    }

    metric_dependencies = ("table.modeling.linear.feature_importances",)
    success_keys = (
        "n_features",
        "important_columns",
        "y_column",
        "threshold",
    )

    default_kwarg_values = {
        "n_features": None,
        "important_columns": None,
        "y_column": None,
        "threshold": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

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
        if configuration is None:
            configuration = self.configuration

        n_features = configuration.kwargs.get("n_features")
        columns = configuration.kwargs.get("important_columns")
        threshold = configuration.kwargs.get("threshold")
        y_column = configuration.kwargs.get("y_column")

        try:
            assert (
                columns is not None or threshold is not None
            ), "at least one of important_columns or threshold is required"
            assert (
                isinstance(n_features, int) or n_features is None
            ), "n_features must be an integer"
            if columns is not None:
                assert (
                    isinstance(columns, tuple) or isinstance(columns, list)
                ) and all(
                    isinstance(i, str) for i in columns
                ), "columns must be a tuple or list of string column names"
            assert (
                isinstance(threshold, float) and (0 <= threshold <= 1)
            ) or threshold is None, "threshold must be a float between 0 and 1"
            assert y_column is not None, "target y_column must be specified"
            assert isinstance(y_column, str), "y_column must be a string column name"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        super().validate_configuration(configuration)

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics,
        runtime_configuration=None,
        execution_engine=None,
    ):

        importances = dict(
            sorted(
                metrics["table.modeling.linear.feature_importances"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        n_features = configuration["kwargs"].get("n_features")
        columns = configuration["kwargs"].get("important_columns")
        threshold = configuration["kwargs"].get("threshold")

        if columns:
            column_success = []
            for i in columns:
                if importances[i] >= threshold:
                    column_success.append(True)
                else:
                    column_success.append(False)
            column_success = all(column_success)
        else:
            column_success = True

        if n_features:
            n_features_success = []
            for i in importances.keys():
                if importances[i] >= threshold:
                    n_features_success.append(True)
            n_features_success = len(n_features_success) == int(n_features)
        else:
            n_features_success = True

        success = column_success and n_features_success

        return {"success": success, "result": {"observed_value": importances}}


if __name__ == "__main__":
    ExpectTableLinearFeatureImportancesToBe().print_diagnostic_checklist()
