from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ...data_asset.util import parse_result_format
from ..expectation import (
    ColumnMapDatasetExpectation,
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics, get_domain_metrics_dict_by_name


class ExpectColumnValueZScoresToBeLessThan(ColumnMapDatasetExpectation):
    """
    Expect the Z-scores of a columns values to be less than a given threshold

            expect_column_values_to_be_of_type is a :func:`column_map_expectation \
            <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.column_map_expectation>` for
            typed-column
            backends,
            and also for PandasExecutionEngine where the column dtype and provided type_ are unambiguous constraints (any
            dtype
            except 'object' or dtype of 'object' with type_ specified as 'object').

            Parameters:
                column (str): \
                    The column name of a numerical column.
                threshold (number): \
                    A maximum Z-score threshold. All column Z-scores that are lower than this threshold will evaluate
                    successfully.


            Keyword Args:
                mostly (None or a float between 0 and 1): \
                    Return `"success": True` if at least mostly fraction of values match the expectation. \
                    For more detail, see :ref:`mostly`.
                double_sided (boolean): \
                    A True of False value indicating whether to evaluate double sidedly.
                    Example:
                    double_sided = True, threshold = 2 -> Z scores in non-inclusive interval(-2,2)
                    double_sided = False, threshold = 2 -> Z scores in non-inclusive interval (-infinity,2)


            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the Expectation config as part of the result object. \
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
    """

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    map_metric = "column_values.z_scores.under_threshold"
    metric_dependencies = (
        "column_values.z_scores.under_threshold.count",
        "column.aggregate.mean",
        "column.aggregate.standard_deviation",
        "column_values.nonnull.count",
        "column.z_scores",
    )
    success_keys = ("threshold", "double_sided", "mostly")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "threshold": None,
        "double_sided": True,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @PandasExecutionEngine.metric(
        metric_name="column.z_scores",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=(
            "column.aggregate.mean",
            "column.aggregate.standard_deviation",
        ),
    )
    def _pandas_z_scores(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        """Z-Score Metric Function"""
        # Series conversion - this works
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        # TODO: Necessary to check domain kwargs? Is there a better way to do this? (RL)
        domain_metrics_lookup = get_domain_metrics_dict_by_name(
            metrics=metrics, metric_domain_kwargs=metric_domain_kwargs
        )
        mean = domain_metrics_lookup["column.aggregate.mean"]
        std_dev = domain_metrics_lookup["column.aggregate.standard_deviation"]

        try:
            return (series - mean) / std_dev
        except TypeError:
            raise (
                TypeError(
                    "Cannot complete Z-score calculations on a non-numerical column."
                )
            )

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.z_scores.under_threshold",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("threshold", "double_sided",),
        metric_dependencies=("column.z_scores",),
    )
    def _pandas_under_threshold(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        """Checks if values under threshold"""
        threshold = metric_value_kwargs["threshold"]
        double_sided = metric_value_kwargs["double_sided"]

        # The series I'm getting does not consist of the z-scores themselves - PROBLEM
        domain_metrics_lookup = get_domain_metrics_dict_by_name(
            metrics=metrics, metric_domain_kwargs=metric_domain_kwargs
        )
        z_scores = domain_metrics_lookup["column.z_scores"]

        try:
            if double_sided:
                under_threshold = z_scores.abs() < abs(threshold)
            else:
                under_threshold = z_scores < threshold
            return pd.DataFrame(
                {"column_values.z_scores.under_threshold": under_threshold}
            )
        except TypeError:
            raise (
                TypeError("Cannot check if a string lies under a numerical threshold")
            )

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        neccessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            # Ensuring Z-score Threshold metric has been properly provided
            assert (
                "threshold" in configuration.kwargs
            ), "A Z-score threshold must be provided"
            assert isinstance(
                configuration.kwargs["threshold"], (float, int)
            ), "Provided threshold must be a number"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set Z Score threshold, returning a nested dictionary documenting the
        validation."""
        # Obtaining dependencies used to validate the expectation - PROBLEM: MEAN'S METHOD CURRENTLY BEING USED
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        # Extracting Pre-defined Metrics
        # Extracting metrics
        metric_vals = extract_metrics(
            validation_dependencies, metrics, configuration, runtime_configuration
        )

        # Runtime configuration has preference
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format",
                configuration.kwargs.get(
                    "result_format", self.default_kwarg_values.get("result_format")
                ),
            )
        else:
            result_format = configuration.kwargs.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )

        # Obtaining value for "mostly"
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )

        # If result_format is changed by the runtime configuration
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")

        # Returning dictionary output with necessary metrics based on the format
        return _format_map_output(
            result_format=parse_result_format(result_format),
            # Success = Ratio of successful nonnull values > mostly?
            success=(
                metric_vals.get("column_values.z_scores.under_threshold.count")
                / metric_vals.get("column_values.nonnull.count")
            )
            >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.z_scores.under_threshold.count"),
            unexpected_list=metric_vals.get(
                "column_values.z_scores.under_threshold.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.z_scores.under_threshold.unexpected_index"
            ),
        )
