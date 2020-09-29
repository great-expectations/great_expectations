from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
    DatasetExpectation)
from ..registry import extract_metrics, get_metric_kwargs

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ExpectColumnPairValuesAToBeGreaterThanB(DatasetExpectation):
    metric_dependencies = ("equal_columns",)
    success_keys = ("column_A", "column_B",  "ignore_row_if",)

    default_kwarg_values = {
        "column_A": None,
        "column_B": None,
        "ignore_row_if": "both_values_are_missing",
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "column_A" in configuration.kwargs and "column_B" in configuration.kwargs, "both columns must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.metric(
        metric_name="equal_columns",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("column_A", "column_B"),
        metric_dependencies=tuple(),
    )
    def _pandas_equal_columns(
            self,
            batches: Dict[str, Batch],
            execution_engine: PandasExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: dict,
            runtime_configuration: dict = None,
    ):
        """Metric which returns all columns in a dataframe"""
        df = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )
        column_A = df[metric_value_kwargs["column_A"]]
        column_B = df[metric_value_kwargs["column_B"]]

        return (column_A == column_B).any()


    @Expectation.validates(metric_dependencies=metric_dependencies)
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
        equal_columns = metric_vals["equal_columns"]

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

        return {"success": equal_columns}
