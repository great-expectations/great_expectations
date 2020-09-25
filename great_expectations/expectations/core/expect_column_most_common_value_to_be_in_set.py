from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ..expectation import (
    ColumnMapDatasetExpectation,
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics


class ExpectColumnMostCommonValueToBeInSet(DatasetExpectation):
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.aggregate.mode",)
    success_keys = (
        "value_set",
        "ties_okay",
    )

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "value_set": None,
        "ties_okay": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    """ A Column Map Metric Decorator for the Mode metric"""

    @PandasExecutionEngine.metric(
        metric_name="column.aggregate.mode",
        metric_domain_keys=DatasetExpectation.domain_keys,
        metric_value_keys=(),
        metric_dependencies=tuple(),
    )
    def _pandas_mode(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        """Mean Metric Function"""
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        return series.mode().values

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """Validating that user has inputted a value set and that configuration has been initialized"""
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set)
            ), "value_set must be a list or a set"
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
        """Validates the mode metric against the value set"""
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

        mode_list = metric_vals.get("column.aggregate.mode")
        value_set = self.get_success_kwargs(configuration).get("value_set")
        ties_okay = self.get_success_kwargs(configuration).get("ties_okay")

        intersection_count = len(set(value_set).intersection(mode_list))

        if ties_okay:
            success = intersection_count > 0
        else:
            success = len(mode_list) == 1 and intersection_count == 1

        return {"success": success, "result": {"observed_value": mode_list}}
