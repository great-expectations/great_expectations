from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine

from ...core.batch import Batch
from ...data_asset.util import parse_result_format
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
)
from ..registry import extract_metrics, get_metric_kwargs


class ExpectColumnValuesToBeInSet(ColumnMapDatasetExpectation):
    metric_dependencies = ("map.in_set.count", "map.nonnull.count", "map.count")
    domain_keys = ("batch_id", "table", "column", "row_condition", "condition_parser")
    success_keys = ("value_set", "mostly", "parse_strings_as_datetimes")
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
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set)
            ), "value_set must be a list or a set"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    def get_validation_dependencies(
        self, configuration: Optional[ExpectationConfiguration] = None
    ):
        dependencies = super().get_validation_dependencies(configuration)
        dependencies["metrics"] = self.metric_dependencies
        return dependencies

    @PandasExecutionEngine.column_map_metric(
        metric_name="map.in_set",
        metric_domain_keys=domain_keys,
        metric_value_keys=("value_set",),
        metric_dependencies=tuple(),
    )
    def _pandas_values_in_set(
        self,
        series: pd.Series,
        value_set: Union[list, set],
        runtime_configuration: dict = None,
    ):
        if value_set is None:
            # Vacuously true
            return np.ones(len(series), dtype=np.bool_)
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed_value_set = PandasExecutionEngine.parse_value_set(
                value_set=value_set
            )
        else:
            parsed_value_set = value_set

        return series.isin(parsed_value_set)

    @PandasExecutionEngine.column_map_metric(
        metric_name="map.nonnull",
        metric_domain_keys=domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
    )
    def _nonnull_count(self, series: pd.Series, runtime_configuration: dict = None):
        return ~series.isnull()

    @PandasExecutionEngine.metric(
        metric_name="map.count",
        metric_domain_keys=domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
    )
    def _count(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        df = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )
        return df.shape[0]

    @PandasExecutionEngine.metric(
        metric_name="snippet",
        metric_domain_keys=domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        batchable=True,
    )
    def _snippet(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        df = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )
        return df

    @ColumnMapDatasetExpectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        metric_vals = extract_metrics(self.metric_dependencies, metrics, configuration)
        mostly = configuration.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")
        return self.format_map_output(
            result_format=parse_result_format(result_format),
            success=(
                metric_vals.get("map.in_set.count")
                / metric_vals.get("map.nonnull.count")
            )
            > mostly,
            element_count=metric_vals.get("map.count"),
            nonnull_count=metric_vals.get("map.nonnull.count"),
            unexpected_count=metric_vals.get("map.nonnull.count")
            - metric_vals.get("map.in_set.count"),
            # TODO:
            unexpected_list=[],
            unexpected_index_list=[],
        )

    #
    # @renders(StringTemplate, modes=())
    # def lkjdsf(self, mode={prescriptive}, {descriptive}, {valiation}):
    #     return "I'm a thing"
