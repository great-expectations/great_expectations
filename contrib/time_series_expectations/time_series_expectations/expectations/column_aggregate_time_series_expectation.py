from abc import ABC
from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from time_series_expectations.expectations.prophet_model_deserializer import (
    ProphetModelDeserializer,
)


class ColumnAggregateTimeSeriesExpectation(ColumnAggregateExpectation, ABC):
    """This Expectation abstract base class checks to see if an aggregate statistic calculated from a column matches the predictions of a prophet model for a given date.

    To complete this Expectation, you must implement a metric_dependency. If you're referring to a metric that already exists, this can be as simple as:

        metric_dependency = "column.max"

    If you're referring to a new metric, you'll need to implement it. For more information, please see the documentation for [How to create a custom Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations/#6-implement-your-metric-and-connect-it-to-your-expectation).

    In addition to the `metric_dependency`, you should implement examples, which are used to generate the documentation and gallery entry for Expectations.

    Please see expect_column_max_to_match_prophet_date_model for an example of how to implement this kind of Expectation.

    Notes:
    * Prophet is an open source forecasting library created at facebook. For more information, please see the [project github page](https://github.com/facebook/prophet).
    """

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
