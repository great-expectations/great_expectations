from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Tuple

from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
)


class ExpectColumnPairValuesToBeInSet(ColumnPairMapExpectation):
    """Expect the paired values from columns A and B to belong to a set of valid pairs.

    expect_column_pair_values_to_be_in_set is a \
    [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).

    Column Pair Map Expectations are evaluated for a pair of columns and ask a yes/no question about the row-wise relationship between those two columns.
    Based on the result, they then calculate the percentage of rows that gave a positive answer.
    If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        value_pairs_set (list of tuples): All the valid pairs to be matched

    Other Parameters:
        ignore_row_if (str): \
            "both_values_are_missing", "either_value_is_missing", "neither" \
            If specified, sets the condition on which a given row is to be ignored. Default "neither".
        mostly (None or a float between 0 and 1): \
            Successful if at least `mostly` fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
            For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Sets

    Example Data:
                test 	test2
            0 	1       1
            1 	2       1
            2 	4   	1

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnPairValuesToBeInSet(
                    column_A="test",
                    column_B="test2",
                    value_pairs_set=[(2,1), (1,1)],
                    mostly=.5
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 1,
                    "unexpected_percent": 33.33333333333333,
                    "partial_unexpected_list": [
                      [
                        4,
                        1
                      ]
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 33.33333333333333,
                    "unexpected_percent_nonmissing": 33.33333333333333
                  },
                  "meta": {},
                  "success": true
                }

        Failing Case:
            Input:
                ExpectColumnPairValuesToBeInSet(
                    column_A="test",
                    column_B="test2",
                    value_pairs_set=[(1,2) (4,1)],
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 2,
                    "unexpected_percent": 66.66666666666666,
                    "partial_unexpected_list": [
                      [
                        1,
                        1
                      ],
                      [
                        2,
                        1
                      ]
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 66.66666666666666,
                    "unexpected_percent_nonmissing": 66.66666666666666
                  },
                  "meta": {},
                  "success": false
                }
    """  # noqa: E501

    value_pairs_set: List[Tuple[Any, Any]]
    ignore_row_if: Literal["both_values_are_missing", "either_value_is_missing", "neither"] = (
        "both_values_are_missing"
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "column pair map expectation",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_pair_values.in_set"
    success_keys: ClassVar[Tuple[str, ...]] = (
        "value_pairs_set",
        "ignore_row_if",
        "mostly",
    )
    args_keys = (
        "column_A",
        "column_B",
        "value_pairs_set",
    )
