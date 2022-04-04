import json
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

import pandas as pd
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
import aequitas.plot as ap

# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableEvaluatingModelBias(TableMetricProvider):

    metric_name = "table.model_bias"
    value_keys = ("y_column",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
        alpha = .05
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )


        # Group() class evaluates biases across all subgroups in dataset by assembling a confusion matrix 
        # of each subgroup, calculating used metrics such as false positive rate and false omission rate, 
        # as well as counts by group and group prevelance among the sample population.
        g = Group()
        # The get_crosstabs() method expects a dataframe with predefined columns score, and label_value 
        # and treats other columns as attributes against which to test for disparities
        xtab, _ = g.get_crosstabs(df)
        # Bias() class calculates disparities between groups based on the crosstab returned by the Group() 
        # class get_crosstabs(). Disparities are calculated as a ratio of a metric for a group of interest 
        # compared to a base group.
        b = Bias()
        # can pick frame of reference (ex: Caucasians or males historically favored), but thos
        # chooses disparities in relation to the group with the lowest value for every disparity metric, 
        # as then every group's value will be at least 1.0, and relationships can be evaluated more linearly.
        majority_bdf = b.get_disparity_major_group(xtab, original_df=df)
        f = Fairness()
        fdf = f.get_group_value_fairness(majority_bdf)
        #gaf = f.get_group_attribute_fairness(fdf) #this produces cool chart that would be nice to display
        gof = f.get_overall_fairness(fdf)
        return gof['Overall Fairness']

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
class ExpectTableModelBias(TableExpectation):
    """Using Aeqitas we evaluate predicted and true values to evaluate certain metrics 
    on how a classider model imposes bias on a given attribute group. For more information
    go to https://dssg.github.io/aequitas/examples/compas_demo.html"""

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
        "contributors": ["@luismdiaz01","@derekma73"],
        "requirements": ["sklearn"],
    }

    metric_dependencies = ("table.model_bias",)
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
                metrics["table.model_bias"].items(),
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
    ExpectTableModelBias().print_diagnostic_checklist()