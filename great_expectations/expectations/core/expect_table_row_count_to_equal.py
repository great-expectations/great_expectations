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
from ..registry import extract_metrics


class ExpectTableRowCountToEqual(DatasetExpectation):
    """Expect the number of rows to equal a value.

    expect_table_row_count_to_equal is a :func:`expectation \
    <great_expectations.validator.validator.Validator.expectation>`, not a
    ``column_map_expectation`` or ``column_aggregate_expectation``.

    Args:
        value (int): \
            The expected number of rows.

    Other Parameters:
        result_format (string or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
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

    See Also:
        expect_table_row_count_to_be_between
    """

    metric_dependencies = ("column_values.count",)
    success_keys = ("value",)

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "value": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    # """ A Metric Decorator for the Row Count"""
    #
    # @PandasExecutionEngine.metric(
    #     metric_name="rows.count",
    #     metric_domain_keys=DatasetExpectation.domain_keys,
    #     metric_value_keys=(),
    #     metric_dependencies=tuple(),
    #     filter_column_isnull=False,
    # )
    # def _pandas_row_count(
    #     self,
    #     batches: Dict[str, Batch],
    #     execution_engine: PandasExecutionEngine,
    #     metric_domain_kwargs: dict,
    #     metric_value_kwargs: dict,
    #     metrics: dict,
    #     runtime_configuration: dict = None,
    #     filter_column_isnull: bool = False,
    # ):
    #     """Row count metric function"""
    #     df = execution_engine.get_domain_dataframe(
    #         domain_kwargs=metric_domain_kwargs,
    #         batches=batches,
    #         filter_column_isnull=False,
    #     )
    #
    #     return df.shape[0]

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

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # Ensuring that a proper value has been provided
        try:
            assert (
                "value" in configuration.kwargs
            ), "An expected row count must be provided"
            assert isinstance(
                configuration.kwargs["value"], int
            ), "Provided threshold must be an integer"

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
        """Validates given row count against expected value"""
        # Obtaining dependencies used to validate the expectation
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
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
        row_count = metric_vals.get("column_values.count")

        # Obtaining components needed for validation
        value = self.get_success_kwargs(configuration).get("value")

        # Checking if the row count is equivalent to value
        success = row_count == value
        return {"success": success, "result": {"observed_value": row_count}}
