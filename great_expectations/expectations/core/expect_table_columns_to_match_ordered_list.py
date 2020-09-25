from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from itertools import zip_longest
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ..expectation import (
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
)
from ..registry import extract_metrics


class ExpectTableColumnsToMatchOrderedList(DatasetExpectation):
    metric_dependencies = ("columns",)
    success_keys = (
        "ordered_list",
    )
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ordered_list": None,
        "result_format": "BASIC",
        "column": None,
        "column_index": None,
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    """ A Column Metric Decorator for the Column Count"""
    @PandasExecutionEngine.metric(
        metric_name="columns",
        metric_domain_keys=("batch_id", "table", "row_condition", "condition_parser"),
        metric_value_keys=(),
        metric_dependencies=(),
    )
    def _pandas_columns(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        """Metric which returns all columns in a DataFrame"""
        df = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        cols = df.columns
        return cols.tolist()

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
            assert "ordered_list" in configuration.kwargs, "ordered_list is required"
            assert isinstance(
                configuration.kwargs["ordered_list"], (list, set)
            ), "ordered_list must be a list or a set"

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
        """Validates given column count against expected value"""
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

        # Obtaining columns and ordered list for sake of comparison
        columns = metric_vals.get("columns")
        column_list = self.get_success_kwargs(configuration).get("ordered_list")

        if column_list is None or list(columns) == list(column_list):
            return {"success": True, "result": {"observed_value": list(columns)}}
        else:
            # In the case of differing column lengths between the defined expectation and the observed column set, the
            # max is determined to generate the column_index.
            number_of_columns = max(len(column_list), len(columns))
            column_index = range(number_of_columns)

            # Create a list of the mismatched details
            compared_lists = list(
                zip_longest(column_index, list(column_list), list(columns))
            )
            mismatched = [
                {"Expected Column Position": i, "Expected": k, "Found": v}
                for i, k, v in compared_lists
                if k != v
            ]
            return {
                "success": False,
                "result": {
                    "observed_value": list(columns),
                    "details": {"mismatched": mismatched},
                },
            }