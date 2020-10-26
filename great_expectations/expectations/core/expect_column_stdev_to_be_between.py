from typing import Dict, Optional

import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import Batch
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import DatasetExpectation, Expectation
from great_expectations.expectations.registry import extract_metrics
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectColumnStdevToBeBetween(DatasetExpectation):
    """Expect the column standard deviation to be between a minimum value and a maximum value.
            Uses sample standard deviation (normalized by N-1).

            expect_column_stdev_to_be_between is a \
            :func:`column_aggregate_expectation
            <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

            Args:
                column (str): \
                    The column name.
                min_value (float or None): \
                    The minimum value for the column standard deviation.
                max_value (float or None): \
                    The maximum value for the column standard deviation.
                strict_min (boolean):
                    If True, the column standard deviation must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the column standard deviation must be strictly smaller than max_value, default=False

            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                catch_exceptions (boolean or None): \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.

            Returns:
                An ExpectationSuiteValidationResult

                Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
                :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

            Notes:
                These fields in the result object are customized for this expectation:
                ::

                    {
                        "observed_value": (float) The true standard deviation for the column
                    }

                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound
                * If max_value is None, then min_value is treated as a lower bound

            See Also:
                :func:`expect_column_mean_to_be_between \
                <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_mean_to_be_between>`

                :func:`expect_column_median_to_be_between \
                <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_median_to_be_between>`

            """

    metric_dependencies = ("column.aggregate.standard_deviation",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
    )
    default_kwarg_values = {
        "min_value": None,
        "strict_min": False,
        "max_value": None,
        "strict_max": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        min_val = None
        max_val = None

        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for metric"
            assert (
                min_val is not None or max_val is not None
            ), "min_value and max_value cannot both be none"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    # @PandasExecutionEngine.metric(
    #        metric_name="column.aggregate.standard_deviation",
    #        metric_domain_keys=DatasetExpectation.domain_keys,
    #        metric_value_keys=tuple(),
    #        metric_dependencies=tuple(),
    #        filter_column_isnull=False,
    #    )
    def _standard_deviation(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )
        return series.std()

    @classmethod
    @renderer(renderer_type="descriptive")
    def _descriptive_renderer(
        cls, expectation_configuration, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation_configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "standard deviation may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"standard deviation must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"standard deviation must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"standard deviation must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    # @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        metric_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        metric_vals = extract_metrics(
            metric_dependencies, metrics, configuration, runtime_configuration
        )
        column_stdev = metric_vals.get("column.aggregate.standard_deviation")
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")
        if min_value is not None:
            if strict_min:
                above_min = column_stdev > min_value
            else:
                above_min = column_stdev >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_stdev < max_value
            else:
                below_max = column_stdev <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_stdev}}
