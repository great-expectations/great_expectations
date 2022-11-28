import json
import math
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

import great_expectations
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
    _format_map_output,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.validator.validation_graph import MetricConfiguration


# Source: https://stackoverflow.com/questions/26889711/extracting-significand-and-exponent-for-base-10-representation-from-decimal-form
def sig_exp(num):
    num_str = str(abs(num))
    parts = num_str.split(".", 2)
    decimal = parts[1] if len(parts) > 1 else ""
    exp = -len(decimal)
    digits = parts[0].lstrip("0") + decimal
    trimmed = digits.rstrip("0")
    exp += len(digits) - len(trimmed)
    sig = int(trimmed) if trimmed else 0
    return sig


# 1 for true, 0 for false
def matchFirstDigit(value, digit):
    significand_string = str(sig_exp(value))
    if int(significand_string[0]) == digit:
        return 1.0
    else:
        return 0.0


class ColumnDistributionMatchesBenfordsLaw(ColumnMetricProvider):
    """
    MetricProvider tests whether data matches Benford's Law Fraud Detection
    Algorithm.

    Uses a Chi-Square Goodness of Fit test with an 80@ p-value
    """

    metric_name = "column.custom.DistributionMatchesBenfordsLaw"
    value_keys = tuple()

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        totalVals = (column.apply(lambda x: 1.0 if x is not None else 0.0)).sum()
        num1 = (
            column.apply(lambda x: matchFirstDigit(x, 1) if x is not None else 0.0)
        ).sum()
        num2 = (
            column.apply(lambda x: matchFirstDigit(x, 2) if x is not None else 0.0)
        ).sum()
        num3 = (
            column.apply(lambda x: matchFirstDigit(x, 3) if x is not None else 0.0)
        ).sum()
        num4 = (
            column.apply(lambda x: matchFirstDigit(x, 4) if x is not None else 0.0)
        ).sum()
        num5 = (
            column.apply(lambda x: matchFirstDigit(x, 5) if x is not None else 0.0)
        ).sum()
        num6 = (
            column.apply(lambda x: matchFirstDigit(x, 6) if x is not None else 0.0)
        ).sum()
        num7 = (
            column.apply(lambda x: matchFirstDigit(x, 7) if x is not None else 0.0)
        ).sum()
        num8 = (
            column.apply(lambda x: matchFirstDigit(x, 8) if x is not None else 0.0)
        ).sum()
        num9 = (
            column.apply(lambda x: matchFirstDigit(x, 9) if x is not None else 0.0)
        ).sum()
        listdata = [
            num1 / totalVals,
            num2 / totalVals,
            num3 / totalVals,
            num4 / totalVals,
            num5 / totalVals,
            num6 / totalVals,
            num7 / totalVals,
            num8 / totalVals,
            num9 / totalVals,
        ]

        matchvalues = []
        for x in range(1, 10):
            matchvalues.append(math.log(1.0 + 1.0 / x) / math.log(10))

        """
        listdata: length 10
        matchvalues: length 10
        chi square them with 90 percent confidence
        """
        stat = 0
        for i in range(9):
            stat += ((listdata[i] - matchvalues[i]) ** 2) / (matchvalues[i])

        if stat >= 5.071:
            return False
        else:
            return True

    # @metric_value(engine=SqlAlchemyExecutionEngine, metric_fn_type="value")
    # def _sqlalchemy(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         selectable,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column_name = accessor_domain_kwargs["column"]
    #     column = sa.column(column_name)
    #     sqlalchemy_engine = execution_engine.engine
    #     dialect = sqlalchemy_engine.dialect

    #     column_median = None

    #     # TODO: compute the value and return it

    #     return column_median

    # @metric_value(engine=SparkDFExecutionEngine, metric_fn_type="value")
    # def _spark(
    #     cls,
    #     execution_engine: "SqlAlchemyExecutionEngine",
    #     metric_domain_kwargs: Dict,
    #     metric_value_kwargs: Dict,
    #     metrics: Dict[Tuple, Any],
    #     runtime_configuration: Dict,
    # ):
    #     (
    #         df,
    #         compute_domain_kwargs,
    #         accessor_domain_kwargs,
    #     ) = execution_engine.get_compute_domain(
    #         metric_domain_kwargs, MetricDomainTypes.COLUMN
    #     )
    #     column = accessor_domain_kwargs["column"]

    #     column_median = None

    #     # TODO: compute the value and return it

    #     return column_median

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """This should return a dictionary:

        {
          "dependency_name": MetricConfiguration,
          ...
        }
        """

        dependencies = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }

        dependencies.update(
            {
                "table.row_count": MetricConfiguration(
                    "table.row_count", table_domain_kwargs
                )
            }
        )

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies["column_values.nonnull.count"] = MetricConfiguration(
                "column_values.nonnull.count", metric.metric_domain_kwargs
            )

        return dependencies


class ExpectColumnDistributionToMatchBenfordsLaw(ColumnExpectation):
    """
    Tests whether data matches Benford's Law Fraud Detection Algorithm.
    Uses a Chi-Square Goodness of Fit test with an 80@ p-value
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "benford_distribution": [1, 1, 1, 1, 2, 2, 3, 4, 5, 6, 9],
                "non_benford_distribution": [1, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9],
            },
            "tests": [
                {
                    "title": "matching_distribution",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "benford_distribution"},
                    "out": {"success": True, "observed_value": True},
                },
                {
                    "title": "non_matching_distribution",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "non_benford_distribution"},
                    "out": {"success": False, "observed_value": False},
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["experimental"],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@shekark642",
            "@vinodkri1",  # Don't forget to add your github handle here!
        ],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.custom.DistributionMatchesBenfordsLaw",)
    # success_keys = ("min_value", "strict_min", "max_value", "strict_max")
    success_keys = tuple()

    # Default values

    # default_kwarg_values = {
    #     "min_value": None,
    #     "max_value": None,
    #     "strict_min": None,
    #     "strict_max": None,
    #     "result_format": "BASIC",
    #     "include_config": True,
    #     "catch_exceptions": False,
    # }

    # def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
    #     """
    #     Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
    #     necessary configuration arguments have been provided for the validation of the expectation.

    #     Args:
    #         configuration (OPTIONAL[ExpectationConfiguration]): \
    #             An optional Expectation Configuration entry that will be used to configure the expectation
    #     Returns:
    #         None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
    #     """
    #     super().validate_configuration(configuration)
    #     # self.validate_metric_value_between_configuration(configuration=configuration)

    # @classmethod
    # @renderer(renderer_type="renderer.prescriptive")
    # @render_evaluation_parameter_string
    # def _prescriptive_renderer(
    #     cls,
    #     configuration=None,
    #     result=None,
    #     language=None,
    #     runtime_configuration=None,
    #     **kwargs,
    # ):
    #     runtime_configuration = runtime_configuration or {}
    #     include_column_name = False if runtime_configuration.get("include_column_name") is False else True
    #     styling = runtime_configuration.get("styling")
    #     params = substitute_none_for_missing(
    #         configuration.kwargs,
    #         [
    #             "column",
    #             "min_value",
    #             "max_value",
    #             "row_condition",
    #             "condition_parser",
    #             "strict_min",
    #             "strict_max",
    #         ],
    #     )
    #
    #     if (params["min_value"] is None) and (params["max_value"] is None):
    #         template_str = "median may have any numerical value."
    #     else:
    #         at_least_str, at_most_str = handle_strict_min_max(params)
    #         if params["min_value"] is not None and params["max_value"] is not None:
    #             template_str = f"median must be {at_least_str} $min_value and {at_most_str} $max_value."
    #         elif params["min_value"] is None:
    #             template_str = f"median must be {at_most_str} $max_value."
    #         elif params["max_value"] is None:
    #             template_str = f"median must be {at_least_str} $min_value."
    #
    #     if include_column_name:
    #         template_str = "$column " + template_str
    #
    #     if params["row_condition"] is not None:
    #         (
    #             conditional_template_str,
    #             conditional_params,
    #         ) = parse_row_condition_string_pandas_engine(params["row_condition"])
    #         template_str = conditional_template_str + ", then " + template_str
    #         params.update(conditional_params)
    #
    #     return [
    #         RenderedStringTemplateContent(
    #             **{
    #                 "content_block_type": "string_template",
    #                 "string_template": {
    #                     "template": template_str,
    #                     "params": params,
    #                     "styling": styling,
    #                 },
    #             }
    #         )
    #     ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        # return self._validate_metric_value_between(
        #     metric_name="column.custom.DistributionMatchesBenfordsLaw",
        #     configuration=configuration,
        #     metrics=metrics,
        #     runtime_configuration=runtime_configuration,
        #     execution_engine=execution_engine,
        # )
        # return {"success": metrics.get("column.custom.DistributionMatchesBenfordsLaw"), "observed_value": metrics.get("column.custom.DistributionMatchesBenfordsLaw")}
        return {
            "success": metrics.get("column.custom.DistributionMatchesBenfordsLaw"),
            "result": {
                "observed_value": metrics.get(
                    "column.custom.DistributionMatchesBenfordsLaw"
                )
            },
        }


if __name__ == "__main__":
    ExpectColumnDistributionToMatchBenfordsLaw().print_diagnostic_checklist()
