from typing import Dict, Optional

import pandas as pd
import numpy as np

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import Batch
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import DatasetExpectation, Expectation
from great_expectations.expectations.registry import extract_metrics


class ExpectColumnQuantileValuesToBeBetween(DatasetExpectation):
    metric_dependencies = ("column.aggregate.quantiles",)
    success_keys = ("quantile_ranges","allow_relative_error",)
    default_kwarg_values = {
        "row_condition": None,
        "allow_relative_eror": None,
        "condition_parser": None,
        "quantile_ranges": None,
        "result_format": "BASIC",
        "allow_relative_error": False,
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)

        # Ensuring necessary parameters are present and of the proper type
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column metric expectations"
            assert "quantile_ranges" in configuration.kwargs, "quantile ranges must be provided"
            assert type(configuration.kwargs["quantile_ranges"]) == dict, "quantile_ranges should be a dictionary"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Ensuring actual quantiles and their value ranges match up
        quantile_ranges = configuration.kwargs["quantile_ranges"]
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]
        if "allow_relative_error" in configuration.kwargs:
            allow_relative_error = configuration.kwargs["allow_relative_error"]
        else:
            allow_relative_error = False

        if allow_relative_error is not False:
            raise ValueError(
                "PandasExecutionEngine does not support relative error in column quantiles."
            )

        if len(quantiles) != len(quantile_value_ranges):
            raise ValueError(
                "quntile_values and quantiles must have the same number of elements"
            )
        return True

    @PandasExecutionEngine.metric(
        metric_name="column.aggregate.quantiles",
        metric_domain_keys=DatasetExpectation.domain_keys,
        metric_value_keys=("quantile_ranges",),
        metric_dependencies=tuple(),
    )
    def _pandas_quantiles(self,
            batches: Dict[str, Batch],
            execution_engine: PandasExecutionEngine,
            metric_domain_kwargs: dict,
            metric_value_kwargs: dict,
            metrics: dict,
            runtime_configuration: dict = None,
            ):

        """Quantile Function"""
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches)
        quantile_ranges = metric_value_kwargs["quantile_ranges"]
        return series.quantile(tuple(quantile_ranges["quantiles"],), interpolation="nearest").tolist()

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        validation_dependencies = self.get_validation_dependencies(configuration)[
            "metrics"
        ]
        metric_vals = extract_metrics(validation_dependencies, metrics, configuration)

        quantile_vals = metric_vals.get("column.aggregate.quantiles")
        quantile_ranges = self.get_success_kwargs(configuration).get("quantile_ranges")
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]

        # We explicitly allow "None" to be interpreted as +/- infinity
        comparison_quantile_ranges = [
            [
                -np.inf if lower_bound is None else lower_bound,
                np.inf if upper_bound is None else upper_bound,
            ]
            for (lower_bound, upper_bound) in quantile_value_ranges
        ]
        success_details = [
            range_[0] <= quantile_vals[idx] <= range_[1]
            for idx, range_ in enumerate(comparison_quantile_ranges)
        ]

        return {
            "success": np.all(success_details),
            "result": {
                "observed_value": {"quantiles": quantiles, "values": quantile_vals},
                "details": {"success_details": success_details},
            },
        }