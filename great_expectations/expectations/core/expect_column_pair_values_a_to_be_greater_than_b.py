from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from dateutil.parser import parse

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
    metric_dependencies = ("column_a_greater_than_b",)
    success_keys = ("column_A", "column_B",  "ignore_row_if", "parse_strings_as_datetimes","allow_cross_type_comparisons",
                    "or_equal")

    default_kwarg_values = {
        "column_A": None,
        "column_B": None,
        "or_equal": None,
        "parse_strings_as_datetimes": False,
        "allow_cross_type_comparisons": None,
        "ignore_row_if":"both_values_are_missing",
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "column_A" in configuration.kwargs and "column_B" in configuration.kwargs, \
                "both columns must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.metric(
        metric_name="column_a_greater_than_b",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("column_A", "column_B"),
        metric_dependencies=tuple(),
    )
    def _pandas_column_a_greater_than_b(
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
        # Initialization of necessary value kwargs
        allow_cross_type_comparisons = None
        parse_strings_as_datetimes = None
        or_equal = None


        column_A = df[metric_value_kwargs["column_A"]]
        column_B = df[metric_value_kwargs["column_B"]]

        # If value kwargs are given that could impact outcome, initializing them
        if allow_cross_type_comparisons in metric_value_kwargs:
            allow_cross_type_comparisons = metric_value_kwargs["allow_cross_type_comparisons"]

        if parse_strings_as_datetimes in metric_value_kwargs:
            parse_strings_as_datetimes = metric_value_kwargs["parse_strings_as_datetimes"]

        if or_equal in metric_value_kwargs:
            or_equal = metric_value_kwargs["or_equal"]

        if allow_cross_type_comparisons:
            column_A = column_A.apply(str)
            column_B = column_B.apply(str)

        if parse_strings_as_datetimes:
            temp_column_A = column_A.map(parse)
            temp_column_B = column_B.map(parse)

        else:
            temp_column_A = column_A
            temp_column_B = column_B

        if or_equal:
            return temp_column_A >= temp_column_B
        else:
            return temp_column_A > temp_column_B

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
        equal_columns = metric_vals["column_a_greater_than_b"]

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

        return {"success": equal_columns.all()}
