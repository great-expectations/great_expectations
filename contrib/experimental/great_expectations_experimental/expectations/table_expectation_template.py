import json

# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
from typing import Any, Dict, Optional, Tuple

import numpy as np
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor

from great_expectations.core import ExpectationConfiguration

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
    TableExpectation,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    MetricDomainTypes,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableFeatureImportances(TableMetricProvider):

    # This is a built in metric - you do not have to implement it yourself. If you would like to use
    # a metric that does not yet exist, you can use the template below to implement it!
    metric_name = "table.feature_importances"
    value_keys = ("y_column", "n", "model")

    # Below are metric computations for different dialects (Pandas, SqlAlchemy, Spark)
    # They can be used to compute the table data you will need to validate your Expectation
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
        model = Ridge().fit(X, y)
        importances = permutation_importance(
            model,
            X,
            y,
            n_repeats=30,
            random_state=42,
            scoring="neg_mean_absolute_percentage_error",
        )

        return {i: j for i, j in zip(X.columns, importances.importances_mean)}

    # @metric_value(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(
    #     cls,
    #     execution_engine: SqlAlchemyExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columns = metrics.get("table.columns")
    #
    #     # For each column, testing if alphabetical and returning number of alphabetical columns
    #     return len([column for column in columns if column.isalpha()])

    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #     cls,
    #     execution_engine: SparkDFExecutionEngine,
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     columns = metrics.get("table.columns")
    #
    #     # For each column, testing if alphabetical and returning number of alphabetical columns
    #     return len([column for column in columns if column.isalpha()])

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


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectTablePredictiveFeaturesToBe(TableExpectation):
    """TODO: add a docstring here"""

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
                        "n": "1",
                        "important_columns": ["x_1"],
                        "y_column": "y",
                        "threshold": 0.35,
                        "model": "LogisticRegression",
                    },
                    "out": {
                        "success": True,
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            #         "@your_name_here", # Don't forget to add your github handle here!
        ],
        "package": "experimental_expectations",
    }

    metric_dependencies = ("table.feature_importances",)
    success_keys = ("n", "important_columns", "y_column", "threshold", "model")

    default_kwarg_values = {
        "n": None,
        "important_columns": None,
        "y_column": None,
        "threshold": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "model": "LogisticRegression",
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

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        n = configuration.kwargs["n"]
        columns = configuration.kwargs["important_columns"]
        threshold = configuration.kwargs["threshold"]
        y = configuration.kwargs["y_column"]

        try:
            assert n is not None, "n number of features to return is required"
            assert isinstance(n, (str, int)), "n must be a string or integer"
            if columns is not None:
                assert (
                    isinstance(columns, tuple) or isinstance(columns, list)
                ) and all(
                    isinstance(i, str) for i in columns
                ), "columns must be a tuple or list of string column names"
            assert threshold is not None, "threshold is required"
            assert isinstance(threshold, float) and (
                0 <= threshold <= 1
            ), "threshold must be a float between 0 and 1"
            assert y is not None, "y target column must be specified"
            assert isinstance(y, str), "y must be a string column name"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        super().validate_configuration(configuration)
        return True

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):

        importances = dict(
            sorted(
                metrics["table.feature_importances"].items(),
                key=lambda item: item[1],
                reverse=True,
            )
        )
        n = configuration.get("n")
        columns = configuration.get("important_columns")
        threshold = configuration.get("threshold")
        breakpoint()

        return True


if __name__ == "__main__":
    # ExpectTablePredictiveFeaturesToBe().print_diagnostic_checklist()
    out = ExpectTablePredictiveFeaturesToBe().run_diagnostics()
    breakpoint()
