from typing import Dict, Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import (
    ExecutionEngine,
)
from great_expectations.expectations.expectation import BatchExpectation
from time_series_expectations.expectations.prophet_model_deserializer import (
    ProphetModelDeserializer,
)


class ExpectBatchRowCountToMatchProphetDateModel(BatchExpectation):
    """Expect the number of rows in a batch to match the predictions of a prophet model for a given date.

    expect_batch_row_count_to_match_prophet_date_model is a [BatchExpectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations)

    Args:
        date (str):
            A string representing the date to compare the batch row count to
        model_json (str):
            A string containing a JSON-serialized Prophet model

    Keyword Args:

    Other Parameters:
        result_format (str or None):
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY.
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean):
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None):
            If True, then catch exceptions and include them as part of the result object.
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None):
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    Notes:
        * Prophet is an open source forecasting library created at facebook. For more information, please see the [project github page](https://github.com/facebook/prophet).

    """

    with open(file_relative_path(__file__, "example_prophet_date_model.json")) as f_:
        example_prophet_date_model_json = f_.read()

    examples = [
        {
            "data": {"foo": range(100)},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "date": "2022-01-11",
                        "model": example_prophet_date_model_json,
                    },
                    "out": {
                        "success": True,
                        "observed_value": 100,
                    },
                }
            ],
        },
        {
            "data": {"foo": range(50)},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "date": "2022-01-01",
                        "model": example_prophet_date_model_json,
                    },
                    "out": {
                        "success": False,
                        "observed_value": 50,
                    },
                }
            ],
        },
    ]

    metric_dependencies = ("table.row_count",)

    success_keys = ()

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
        batch_row_count = metrics["table.row_count"]
        model_json = configuration.kwargs["model"]
        date = configuration.kwargs["date"]

        model = ProphetModelDeserializer().get_model(model_json)
        forecast = model.predict(pd.DataFrame({"ds": [date]}))

        forecast_value = forecast.yhat[0]
        forecast_lower_bound = forecast.yhat_lower[0]
        forecast_upper_bound = forecast.yhat_upper[0]

        in_bounds = (forecast_lower_bound < batch_row_count) & (
            batch_row_count < forecast_upper_bound
        )

        return {
            "success": in_bounds,
            "result": {
                "observed_value": batch_row_count,
                "forecast_value": forecast_value,
                "forecast_lower_bound": forecast_lower_bound,
                "forecast_upper_bound": forecast_upper_bound,
            },
        }

    library_metadata = {
        "tags": [],
        "contributors": [
            "@abegong",
        ],
        "requirements": ["prophet"],
    }


if __name__ == "__main__":
    ExpectBatchRowCountToMatchProphetDateModel().print_diagnostic_checklist()
