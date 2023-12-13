from typing import Any, List, Literal, Tuple

from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
)


class ExpectColumnPairValuesToBeInSet(ColumnPairMapExpectation):
    """Expect the paired values from columns A and B to belong to a set of valid pairs.

    expect_column_pair_values_to_be_in_set is a \
    [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).

    For example:
    ::
        >>> d = {'fruit': ['appple','apple','apple','banana','banana'],
                'color': ['red','green','yellow','yellow','red']}
        >>> my_df = pd.DataFrame(data=d)
        >>> my_df.expect_column_pair_values_to_be_in_set(
                'fruit',
                'color',
                [
                    ('apple','red'),
                    ('apple','green'),
                    ('apple','yellow'),
                    ('banana','yellow'),
                ]
        )
        {
            "success": false,
            "meta": {},
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
                    [
                        "banana",
                        "red"
                    ]
                ],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 20.0,
                "unexpected_percent_nonmissing": 20.0
            }
        }

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        value_pairs_set (list of tuples): All the valid pairs to be matched

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"

    Other Parameters:
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
    """

    value_pairs_set: List[Tuple[Any, Any]]
    ignore_row_if: Literal[
        "both_values_are_missing", "either_value_is_missing", "neither"
    ] = "both_values_are_missing"

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
    success_keys = ("value_pairs_set", "ignore_row_if", "mostly")
    args_keys = (
        "column_A",
        "column_B",
        "value_pairs_set",
    )
