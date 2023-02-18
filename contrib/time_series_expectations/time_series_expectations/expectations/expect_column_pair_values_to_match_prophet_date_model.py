from typing import Optional

import pandas as pd
from prophet.serialize import model_from_json

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)

with open("./example_prophet_date_model.json") as f_:
    model_json = f_.read()

example_data = pd.read_csv("./example_data.csv")


class ColumnPairValuesMatchProphetModel(ColumnPairMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_pair_values.match_prophet_forecast"

    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ("model_json",)

    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, model_json, **kwargs):
        model = model_from_json(model_json)
        forecast = model.predict(pd.DataFrame({"ds": column_A}))
        in_bounds = (forecast.yhat_lower < column_B) & (column_B < forecast.yhat_upper)

        return in_bounds

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


class ExpectColumnPairValuesToMatchProphetDateForecast(ColumnPairMapExpectation):
    """This Expectation is used to compare the values in a column to a Prophet forecast, based on a timestamp in a second column."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": example_data,
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "ds",
                        "column_B": "y2",
                        # "date_column": "ds",
                        # "value_column": "y2",
                        "model_json": model_json,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "ds",
                        "column_B": "y1",
                        "model_json": model_json,
                        "mostly": 0.8,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "ds",
                        "column_B": "y1",
                        "model_json": model_json,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    map_metric = "column_pair_values.match_prophet_forecast"

    success_keys = (
        "column_A",
        "column_B",
        "model_json",
        "mostly",
    )

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

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnPairValuesToMatchProphetDateForecast().print_diagnostic_checklist()
