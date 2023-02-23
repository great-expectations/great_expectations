from typing import Optional

import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)
from time_series_expectations.expectations.prophet_model_deserializer import (
    ProphetModelDeserializer,
)

with open(file_relative_path(__file__, "example_prophet_date_model.json")) as f_:
    model_json = f_.read()

example_data = pd.read_csv(file_relative_path(__file__, "example_data.csv"))


class ColumnPairValuesMatchProphetModel(ColumnPairMapMetricProvider):

    condition_metric_name = "column_pair_values.match_prophet_forecast"

    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ("model_json",)

    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, model_json, **kwargs):
        model = ProphetModelDeserializer().get_model(model_json)
        forecast = model.predict(pd.DataFrame({"ds": column_A}))
        in_bounds = (forecast.yhat_lower < column_B) & (column_B < forecast.yhat_upper)

        return in_bounds

    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     print(column_A)
    #     print(type(column_A))
    #     raise NotImplementedError

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, model_json, **kwargs):
        # This approach converts the Spark Columns to Pandas Series. The business logic is executed in pandas.
        # print(column_A)
        # print(type(column_A))

        # column_A_pandas = column_A.toPandas()
        # column_B_pandas = column_B.toPandas()

        # model = model_from_json(model_json)
        # forecast = model.predict(pd.DataFrame({"ds": column_A_pandas}))
        # in_bounds = (forecast.yhat_lower < column_B_pandas) & (column_B_pandas < forecast.yhat_upper)

        # return in_bounds

        # This approach creates a Spark UDF, and executes the business logic within pyspark.
        model = ProphetModelDeserializer().get_model(model_json)

        def check_if_value_is_in_model_forecast_bounds(date_value_pair, model=model):
            date = date_value_pair[0]
            value = date_value_pair[1]

            forecast = model.predict(pd.DataFrame({"ds": [date]}))

            forecast_lower_bound = forecast.yhat_lower[0]
            forecast_upper_bound = forecast.yhat_upper[0]

            in_bounds = (forecast_lower_bound < value) & (value < forecast_upper_bound)

            return bool(in_bounds)

        check_if_value_is_in_model_forecast_bounds_udf = F.udf(
            check_if_value_is_in_model_forecast_bounds, sparktypes.BooleanType()
        )

        result = check_if_value_is_in_model_forecast_bounds_udf(
            F.struct(column_A, column_B)
        )

        return result


class ExpectColumnPairValuesToMatchProphetDateModel(ColumnPairMapExpectation):
    """This Expectation is used to compare the values in a column to a Prophet forecast, based on a timestamp in a second column."""

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
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
                # {
                #     "backend": "sqlalchemy",
                #     "dialects": ["sqlite", "postgresql"],
                # },
                {
                    "backend": "spark",
                    "dialects": None,
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

    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnPairValuesToMatchProphetDateModel().print_diagnostic_checklist()
