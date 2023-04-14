from typing import Optional

from pandas import to_datetime

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
class ColumnDatetimeDifferenceInMonths(MulticolumnMapMetricProvider):

    condition_metric_name = "multicolumn_values.column_datetime_difference_in_months"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = (
        "start_datetime",
        "end_datetime",
        "gap",
    )

    # @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, dataframe, start_datetime, end_datetime, gap, **kwargs):
    #     print(dataframe)

    #     def date_diff_in_months(df):
    #         col_start = column(start_datetime)
    #         col_end = column(end_datetime)
    #         col_gap = column(gap)
    #         if col_start is None or col_end is None or col_gap is None:
    #             return None

    #         # Calculate the difference in months between the start_datetime and end_datetime columns
    #         diff_months = cast(
    #             (func.strftime("%Y", col_end) - func.strftime("%Y", col_start)) * 12
    #             + (func.strftime("%m", col_end) - func.strftime("%m", col_start)),
    #             Integer,
    #         )
    #         return col_gap == diff_months

    #     return date_diff_in_months(dataframe)

    # @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, dataframe, start_datetime, end_datetime, gap, **kwargs):
    #     def date_diff_in_months(row):
    #         col_start = row(start_datetime)
    #         col_end = row[end_datetime]
    #         col_gap = row[gap]

    #         if col_start is None or col_end is None or col_gap is None:
    #             return None

    #         # Convert start_datetime and end_datetime columns to date format
    #         col_start = to_date(col_start)
    #         col_end = to_date(col_end)

    #         # Calculate the difference in months between the start_datetime and end_datetime columns
    #         diff_months = floor(months_between(col_end, col_start))

    #         return col(col_gap) == diff_months

    #     return functools.reduce(operator.add, date_diff_in_months(col("*")))

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, dataframe, start_datetime, end_datetime, gap, **kwargs):
        def date_diff_in_months(row):
            col_start = to_datetime(row[start_datetime])
            col_end = to_datetime(row[end_datetime])
            col_gap = row[gap]
            if col_start is None or col_end is None or col_gap is None:
                return None

            diff_months = (col_end.year - col_start.year) * 12 + (
                col_end.month - col_start.month
            )
            return col_gap == diff_months

        return dataframe.apply(lambda row: date_diff_in_months(row), axis=1)


# This class defines the Expectation itself
class ExpectMulticolumnDatetimeDifferenceInMonths(MulticolumnMapExpectation):

    """Expect the difference of 2 datetime columns is equal to another column in month.

    This means that for each row, we expect end_datetime - start_datetime = gap (in months)

    Args:

        start_datetime (datetime): The first datetime column to compare.
        end_datetime (datetime): The second datetime column to compare.
        gap (int): The number of months that the difference between start_datetime and end_datetime should be.

    """

    examples = [
        {
            "data": {
                "start_datetime": [
                    "2022-03-22 10:00:00",
                    "2022-03-22 10:00:00",
                    "2022-03-22 10:00:00",
                ],
                "end_datetime": [
                    "2022-04-22 11:00:00",
                    "2022-04-22 11:00:00",
                    "2022-04-22 11:00:00",
                ],
                "gap_pass": [1, 1, 1],
                "gap_fail": [0, 1, 1],
            },
            "tests": [
                {
                    "title": "passed test cases",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["start_datetime", "end_datetime", "gap_pass"],
                        "start_datetime": "start_datetime",
                        "end_datetime": "end_datetime",
                        "gap": "gap_pass",
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "failed test cases",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["start_datetime", "end_datetime", "gap_fail"],
                        "start_datetime": "start_datetime",
                        "end_datetime": "end_datetime",
                        "gap": "gap_fail",
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
                #     "dialects": ["sqlite"],  # , "postgresql"
                # },
                # {
                #     "backend": "spark",
                #     "dialects": None,
                # },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    map_metric = "multicolumn_values.column_datetime_difference_in_months"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("column_list", "start_datetime", "end_datetime", "gap")
    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
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
        gap = configuration.kwargs["gap"]
        column_list = configuration.kwargs["column_list"]
        # # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            # parameter cannot be less than zero,
            assert start_datetime is None or isinstance(start_datetime, str)
            assert end_datetime is None or isinstance(end_datetime, str)
            assert gap is None or isinstance(gap, str)
            assert gap in column_list
            assert start_datetime in column_list
            assert end_datetime in column_list

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "beta",  # "experimental", "beta", or "production"
        "tags": [
            "multi-column expectation",
            "multi-column column datetime difference in months",
        ],
        "contributors": ["@tb102122"],
    }


if __name__ == "__main__":
    ExpectMulticolumnDatetimeDifferenceInMonths().print_diagnostic_checklist()
