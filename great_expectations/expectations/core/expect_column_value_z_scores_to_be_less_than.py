from typing import Union

from great_expectations.core.evaluation_parameters import (
    EvaluationParameterDict,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)


class ExpectColumnValueZScoresToBeLessThan(ColumnMapExpectation):
    """Expect the Z-scores of a column's values to be less than a given threshold.

    expect_column_values_to_be_of_type is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations) \
    for typed-column backends, and also for PandasExecutionEngine where the column \
    dtype and provided type_ are unambiguous constraints \
    (any dtype except 'object' or dtype of 'object' with type_ specified as 'object').

    Args:
        column (str): \
            The column name of a numerical column.
        threshold (number): \
            A maximum Z-score threshold. All column Z-scores that are lower than this threshold will evaluate \
            successfully.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
        double_sided (boolean): \
            A True or False value indicating whether to evaluate double sidedly. Examples... \
            (double_sided = True, threshold = 2) -> Z scores in non-inclusive interval(-2,2) | \
            (double_sided = False, threshold = 2) -> Z scores in non-inclusive interval (-infinity,2)

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.
    """

    threshold: Union[float, EvaluationParameterDict]
    double_sided: Union[bool, EvaluationParameterDict]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    map_metric = "column_values.z_score.under_threshold"
    success_keys = ("threshold", "double_sided", "mostly")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "threshold": None,
        "double_sided": True,
        "mostly": 1,
        "result_format": "BASIC",
        "catch_exceptions": False,
    }
    args_keys = ("column", "threshold")
