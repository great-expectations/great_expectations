from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ...core.batch import Batch
from ...data_asset.util import parse_result_format
from ...execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics, get_metric_kwargs

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ExpectColumnValueLengthsToEqual(ColumnMapDatasetExpectation):
    map_metric = "column_values.length_equals"
    metric_dependencies = (
        "column_values.length_equals.count",
        "column_values.nonnull.count",
        "column.value_lengths",
    )
    success_keys = ("value", "mostly", "parse_strings_as_datetimes")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert (
                "value" in configuration.kwargs
            ), "The length parameter 'value' is required"
            assert isinstance(
                configuration.kwargs["value"], (float, int)
            ), "given value must be numerical"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.length_equals",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("value",),
        metric_dependencies=("column.value_lengths",),
        filter_column_isnull=True,
    )
    def _pandas_column_values_length_equals(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
    ):
        """Tests whether or not value lengths equal threshold"""
        value = metric_value_kwargs["value"]
        length_equals = series.str.len() == value
        return pd.DataFrame({"column_values.length_equals": length_equals})

    """ A metric decorator for individual value lengths"""

    @PandasExecutionEngine.metric(
        metric_name="column.value_lengths",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=(),
        metric_dependencies=tuple(),
    )
    def _pandas_value_lengths(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        """Extracts lengths of individual entries"""
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        return series.str.len()

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
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
        mostly = configuration.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")
        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=(
                metric_vals.get("column_values.length_equals.count")
                / metric_vals.get("column_values.nonnull.count")
            )
            >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.length_equals.count"),
            unexpected_list=metric_vals.get(
                "column_values.length_equals.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.length_equals.unexpected_index_list"
            ),
        )

    #
    # @renders(StringTemplate, modes=())
    # def lkjdsf(self, mode={prescriptive}, {descriptive}, {valiation}):
    #     return "I'm a thing"
