from __future__ import annotations

from typing import ClassVar, Tuple, Union

from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)


class ExpectColumnValueZScoresToBeLessThan(ColumnMapExpectation):
    """Expect the Z-scores of a column's values to be less than a given threshold.

    expect_column_value_z_scores_to_be_less_than is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations) \
    for typed-column backends, and also for PandasExecutionEngine where the column \
    dtype and provided type_ are unambiguous constraints \
    (any dtype except 'object' or dtype of 'object' with type_ specified as 'object').

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            The column name of a numerical column.
        threshold (number): \
            A maximum Z-score threshold. All column Z-scores that are lower than this threshold will evaluate \
            successfully.
        double_sided (boolean): \
            A True or False value indicating whether to evaluate double sidedly. Examples... \
            (double_sided = True, threshold = 2) -> Z scores in non-inclusive interval(-2,2) | \
            (double_sided = False, threshold = 2) -> Z scores in non-inclusive interval (-infinity,2)

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format and meta.

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Distribution

    Example Data:
                test 	test2
            0 	1       -100000000000
            1 	1       -1
            2 	1       0
            3   3       1
            4   3       1

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValueZScoresToBeLessThan(
                    column="test",
                    threshold=1.96,
                    double_sided=True
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 5,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0
                  },
                  "meta": {},
                  "success": true
                }

        Failing Case:
            Input:
                ExpectColumnValueZScoresToBeLessThan(
                    column="test2",
                    threshold=1,
                    double_sided=True
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 5,
                    "unexpected_count": 1,
                    "unexpected_percent": 20.0,
                    "partial_unexpected_list": [
                      -100000000000
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 20.0,
                    "unexpected_percent_nonmissing": 20.0
                  },
                  "meta": {},
                  "success": false
                }
    """  # noqa: E501

    condition_parser: Union[str, None] = "pandas"
    threshold: Union[float, SuiteParameterDict]
    double_sided: Union[bool, SuiteParameterDict]
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "column",
        "row_condition",
        "condition_parser",
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\  # noqa: E501
    map_metric = "column_values.z_score.under_threshold"
    success_keys = ("threshold", "double_sided", "mostly")
    args_keys = ("column", "threshold")
