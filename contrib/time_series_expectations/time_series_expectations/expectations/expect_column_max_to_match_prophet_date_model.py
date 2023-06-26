from great_expectations.data_context.util import file_relative_path
from time_series_expectations.expectations.column_aggregate_time_series_expectation import (
    ColumnAggregateTimeSeriesExpectation,
)


class ExpectColumnMaxToMatchProphetDateModel(ColumnAggregateTimeSeriesExpectation):
    """This Expectation checks to see if the max of a column matches the predictions of a prophet model for a given date.

    expect_column_max_to_match_prophet_date_model is a ColumnAggregateTimeSeriesExpectation.

    Args:
        column (str):
            The name of the column to calculate the max of
        date (str):
            A string representing the date to compare the max to
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
        * I apologize for the dangling prepositions in the Arg docstrings for this Expectation.

    """

    with open(file_relative_path(__file__, "example_prophet_date_model.json")) as f_:
        example_prophet_date_model_json = f_.read()

    examples = [
        {
            "data": {
                "x": [100, 102, 101, 100],
                "y": [100, 100, 100, 500],
            },
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "date": "2022-01-11",
                        "model": example_prophet_date_model_json,
                    },
                    "out": {
                        "success": True,
                        "observed_value": 102,
                    },
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "date": "2022-01-11",
                        "model": example_prophet_date_model_json,
                    },
                    "out": {
                        "success": False,
                        "observed_value": 500,
                    },
                },
            ],
        }
    ]

    metric_dependency = "column.max"

    library_metadata = {
        "tags": [],
        "contributors": [
            "@abegong",
        ],
        "requirements": ["prophet"],
    }


if __name__ == "__main__":
    ExpectColumnMaxToMatchProphetDateModel().print_diagnostic_checklist()
