from typing import Optional

import pandas as pd

from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
class MulticolumnDatetimeDifferenceToBeLessThanTwoMonths(MulticolumnMapMetricProvider):
    condition_metric_name = (
        "multicolumn_values.column_datetime_difference_to_be_less_than_two_months"
    )
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ("start_datetime", "end_datetime")

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, dataframe, start_datetime, end_datetime, **kwargs):
        def date_diff_in_months(row):
            col_start = pd.to_datetime(row[start_datetime])
            col_end = pd.to_datetime(row[end_datetime])
            diff_days = abs(col_end.day - col_start.day)

            if pd.isnull(col_start) or pd.isnull(col_end):
                return True

            diff_months = (col_end.year - col_start.year) * 12 + (
                col_end.month - col_start.month
            )
            return abs(diff_months) < 2 or (abs(diff_months) == 2 and diff_days <= 0)

        return dataframe.apply(lambda row: date_diff_in_months(row), axis=1)


# This class defines the Expectation itself
class ExpectMulticolumnDatetimeDifferenceToBeLessThanTwoMonths(
    MulticolumnMapExpectation
):

    """Expect the difference of 2 datetime columns to be less than or equal to 2 months.

    This means that for each row, we expect end_datetime - start_datetime <= 2 (in months)

    Args:

        start_datetime (datetime): The first datetime column to compare.
        end_datetime (datetime): The second datetime column to compare.

    """

    examples = [
        {
            "data": {
                "start_datetime": [
                    "2023-01-01",
                    "2023-02-01",
                    "2023-03-01",
                    "2023-04-01",
                    "2023-05-01",
                ],
                "end_datetime_within_threshold": [
                    "2023-01-15",
                    "2023-03-02",
                    "2023-05-01",
                    "2023-06-01",
                    "2023-07-01",
                ],
                "end_datetime_above_threshold": [
                    "2023-04-15",
                    "2023-05-02",
                    "2023-06-01",
                    "2023-07-01",
                    "2023-09-01",
                ],
                "end_datetime_with_Nan": [
                    pd.NaT,
                    "2023-03-02",
                    "2023-05-01",
                    "2023-06-01",
                    pd.NaT,
                ],
            },
            "tests": [
                {
                    "title": "within threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "start_datetime": "start_datetime",
                        "end_datetime": "end_datetime_within_threshold",
                        "column_list": [
                            "start_datetime",
                            "end_datetime_within_threshold",
                        ],
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "above threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "start_datetime": "start_datetime",
                        "end_datetime": "end_datetime_above_threshold",
                        "column_list": [
                            "start_datetime",
                            "end_datetime_above_threshold",
                        ],
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "with Nan",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "start_datetime": "start_datetime",
                        "end_datetime": "end_datetime_with_Nan",
                        "column_list": ["start_datetime", "end_datetime_with_Nan"],
                    },
                    "out": {
                        "success": True,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    map_metric = (
        "multicolumn_values.column_datetime_difference_to_be_less_than_two_months"
    )

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("column_list", "start_datetime", "end_datetime")
    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "catch_exceptions": False,
        "base": 2,
    }

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

        start_datetime = configuration.kwargs["start_datetime"]
        end_datetime = configuration.kwargs["end_datetime"]
        column_list = configuration.kwargs["column_list"]
        # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            # parameter cannot be less than zero,
            assert start_datetime is None or isinstance(start_datetime, str)
            assert end_datetime is None or isinstance(end_datetime, str)
            assert start_datetime in column_list
            assert end_datetime in column_list

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "multi-column expectation",
            "multi-column column datetime difference to be less than two months",
        ],
        "contributors": ["@kcs-rohankolappa"],
    }


if __name__ == "__main__":
    ExpectMulticolumnDatetimeDifferenceToBeLessThanTwoMonths().print_diagnostic_checklist()
