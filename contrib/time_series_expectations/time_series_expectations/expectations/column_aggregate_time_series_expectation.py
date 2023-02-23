from abc import ABC
from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from time_series_expectations.expectations.prophet_model_deserializer import (
    ProphetModelDeserializer,
)


class ColumnAggregateTimeSeriesExpectation(ColumnExpectation, ABC):
    """This Expectation is used to an aggregate statistic based on the values in a column, based on a timestamp."""

    # examples = []

    # metric_dependency

    # Expectations of this type can only have a single metric dependency.
    @property
    def metric_dependencies(self):
        return (self.metric_dependency,)

    success_keys = ("model", "date", "column")

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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        metric_value = metrics[self.metric_dependency]
        model_json = configuration.kwargs["model"]
        date = configuration.kwargs["date"]

        model = ProphetModelDeserializer().get_model(model_json)
        forecast = model.predict(pd.DataFrame({"ds": [date]}))

        forecast_value = forecast.yhat[0]
        forecast_lower_bound = forecast.yhat_lower[0]
        forecast_upper_bound = forecast.yhat_upper[0]

        in_bounds = (forecast_lower_bound < metric_value) & (
            metric_value < forecast_upper_bound
        )

        return {
            "success": in_bounds,
            "result": {
                "observed_value": metric_value,
                "forecast_value": forecast_value,
                "forecast_lower_bound": forecast_lower_bound,
                "forecast_upper_bound": forecast_upper_bound,
            },
        }

    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
