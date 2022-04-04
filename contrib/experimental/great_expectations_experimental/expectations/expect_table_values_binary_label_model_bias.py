import json
from typing import Any, Dict, Optional, Tuple

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
from aequitas.preprocessing import preprocess_input_df


# This class defines the Metric, a class used by the Expectation to compute important data for validating itself
class TableEvaluatingModelBias(TableMetricProvider):

    metric_name = "table.model_bias"
    value_keys = ("y_true","y_pred")

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
        reference_group = None,
        alpha = .05
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        df = df.rename(columns={"y_true": "label_value", "y_pred": "score"})
        df, _ = preprocess_input_df(df)

        # Group() class evaluates biases across all subgroups in dataset by assembling a confusion matrix 
        # of each subgroup, calculating used metrics such as false positive rate and false omission rate, 
        # as well as counts by group and group prevelance among the sample population.
        g = Group()
        # The get_crosstabs() method expects a dataframe with predefined columns score, and label_value 
        # and treats other columns as attributes against which to test for disparities. Requires attributes
        # to be strings
        xtab, _ = g.get_crosstabs(df)
        # Bias() class calculates disparities between groups based on the crosstab returned by the Group() 
        # class get_crosstabs(). Disparities are calculated as a ratio of a metric for a group of interest 
        # compared to a base group.
        b = Bias()
        # can pick frame of reference (ex: Caucasians or males historically favored), but thos
        # chooses disparities in relation to the group with the lowest value for every disparity metric, 
        # as then every group's value will be at least 1.0, and relationships can be evaluated more linearly.
        if reference_group:
            bdf = b.get_disparity_predefined_groups(xtab, original_df=df, 
                                        ref_groups_dict=reference_group, 
                                        alpha=alpha, check_significance=True, 
                                        mask_significance=True)
        else:
            bdf = b.get_disparity_major_group(xtab, original_df=df)
        f = Fairness()
        fdf = f.get_group_value_fairness(bdf)
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
    on how a classider model imposes bias on a given attribute group. Requirescolumns
    score (binary or continuous) and label_value (binary). For more information
    go to https://dssg.github.io/aequitas/examples/compas_demo.html"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "entity_id":[1,3,4,5,6],
                "pred": [0,0,0,1,0],
                "y": [0,1,1,0,0],
                "race": ["African-American", "African-American","African-American","African-American","African-American"],
                "sex":["Male","Male","Male","Male","Male"],
                "age_cat":["Greater than 45","25 - 45","Less than 25","Less than 25","25 - 45"]
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "important_columns":["race","sex"],
                        "y_true": "y",
                        "y_pred": "pred",
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "important_columns":["race","sex","age_cat"],
                        "y_pred": "pred", 
                        "y_true": "y"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["ai/ml", "fair-ai", "hackathon-22"],
        "contributors": ["@luismdiaz01","@derekma73"],
        "requirements": ["aequitas"],
    }

    metric_dependencies = ("table.model_bias",)
    success_keys = (
        "important_columns", # might use this if people want to use specific features
        "y_true",
        "y_pred",
    )

    default_kwarg_values = {
        "important_columns": None,
        "y_pred": None,
        "y_true": None, #When the y_true column is not included in the original data set, Aequitas calculates only Statistical Parity and Impact Parities.
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

#        columns = configuration.kwargs.get("important_columns")
        y_true = configuration.kwargs.get("y_true")
        y_pred = configuration.kwargs.get("y_pred")
        columns = configuration.kwargs.get("important_columns")


        try:
            assert columns is not None, "target columns must be specified"
            assert y_true is not None, "target y_true must be specified"
            assert y_pred is not None, "target y_true must be specified"
            assert isinstance(y_pred, str), "y_pred must be a string column name"
            assert isinstance(y_true, str), "y_true must be a string column name"

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
#        columns = configuration["kwargs"].get("important_columns")


        return {"result": {"observed_value": importances}}


if __name__ == "__main__":
    ExpectTableModelBias().print_diagnostic_checklist()