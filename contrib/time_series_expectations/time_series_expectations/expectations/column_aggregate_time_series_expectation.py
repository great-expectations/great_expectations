from abc import ABC
from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation

from time_series_expectations.expectations.util import get_prophet_model_from_json

class ColumnAggregateTimeSeriesExpectation(ColumnExpectation, ABC):
    """This Expectation is used to compare the values in a column to a Prophet forecast, based on a timestamp in a second column."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # examples = []

    # metric_dependency

    # Expectations of this type can only have a single metric dependency.
    @property
    def metric_dependencies(self):
        return (self.metric_dependency,)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    # success_keys = ("min_value", "strict_min", "max_value", "strict_max")
    success_keys = ("model", "date", "column")

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        batch_volume = metrics[self.metric_dependency]
        model_json = configuration.kwargs["model"]
        date = configuration.kwargs["date"]

        model = get_prophet_model_from_json(model_json)
        forecast = model.predict(pd.DataFrame({"ds": [date]}))

        forecast_value = forecast.yhat[0]
        forecast_lower_bound = forecast.yhat_lower[0]
        forecast_upper_bound = forecast.yhat_upper[0]

        in_bounds = (forecast_lower_bound < batch_volume) & (
            batch_volume < forecast_upper_bound
        )

        return {
            "success": in_bounds,
            "result": {
                "observed_value": batch_volume,
                "forecast_value": forecast_value,
                "forecast_lower_bound": forecast_lower_bound,
                "forecast_upper_bound": forecast_upper_bound,
            },
        }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
