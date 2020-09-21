from typing import Dict, Optional

import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import Batch
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import DatasetExpectation, Expectation
from great_expectations.expectations.registry import extract_metrics


class ExpectColumnStdevToBeBetween(DatasetExpectation):
    metric_dependencies = tuple("standard_deviation")
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")
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
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.metric(
        metric_name="standard_deviation",
        metric_domain_keys=DatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
    )
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
        column_stdev = metric_vals.get("standard_deviation")
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
