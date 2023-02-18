from time_series_expectations.expectations.column_aggregate_time_series_expectation import (
    ColumnAggregateTimeSeriesExpectation,
)

with open("./example_prophet_date_model.json") as f_:
    example_prophet_date_model = f_.read()


class ExpectColumnMaxToMatchProphetDateModel(ColumnAggregateTimeSeriesExpectation):
    """This Expectation checks to see if the max of a column matches the predictions of a prophet model for a given date."""

    examples = [
        {
            "data": {
                "x": [1, 2, 3, 4],
                "y": [1, 2, 3, 40],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "date": "2022-01-11",
                        "model": example_prophet_date_model,
                    },
                    "out": {
                        "success": True,
                        "observed_value": 4,
                    },
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "date": "2022-01-11",
                        "model": example_prophet_date_model,
                    },
                    "out": {
                        "success": False,
                        "observed_value": 40,
                    },
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependency = "column.max"


if __name__ == "__main__":
    ExpectColumnMaxToMatchProphetDateModel().print_diagnostic_checklist()
