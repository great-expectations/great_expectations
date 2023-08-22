Expectation Docstring Formatting
================================

The content of an Expectation's docstring is the majority of what is rendered on that Expectation's "details page" in the [Expectation Gallery](https://greatexpectations.io/expectations).

The only strictly enforced rule for Expectation docstrings in [the validation checklist](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md#generate-the-expectation-validation-checklist) is that the first line begins with "Expect " and ends with a period. That first line should be directly next to the triple quote, not on a new line.

As a part of the [CI pipeline to build the Expectation Gallery](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/1-the-build_gallery.py-script.md#the-build_gallerypy-script-in-ci), there is a [`format_docstring_to_markdown` function](https://github.com/great-expectations/great_expectations/blob/98259c16bd439904da2fd08c2e244b6684821302/assets/scripts/build_gallery.py#L601-L672) that is called to take the raw docstring for an Expectation and convert it to markdown. That function makes a lot of assumptions based on indent-level, newlines, and special character sequences.

## Sample docstring

```
class ExpectColumnMaxToBeBetween(ColumnAggregateExpectation):
    """Expect the column maximum to be between a minimum value and a maximum value.

    expect_column_max_to_be_between is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations)

    Args:
        column (str): \
            The column name
        min_value (comparable type or None): \
            The minimum value of the acceptable range for the column maximum.
        max_value (comparable type or None): \
            The maximum value of the acceptable range for the column maximum.
        strict_min (boolean): \
            If True, the lower bound of the column maximum acceptable range must be strictly larger than min_value, default=False
        strict_max (boolean): \
            If True, the upper bound of the column maximum acceptable range must be strictly smaller than max_value, default=False

    Keyword Args:
        parse_strings_as_datetimes (Boolean or None): \
            If True, parse min_value, max_values, and all non-null column values to datetimes before making \
            comparisons.
        output_strftime_format (str or None): \
            A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound
        * If max_value is None, then min_value is treated as a lower bound
        * observed_value field in the result object is customized for this expectation to be a list \
            representing the actual column max

    See Also:
        [expect_column_min_to_be_between](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
    """
```

> See: <https://greatexpectations.io/expectations/expect_column_max_to_be_between>

## Docstring rules

- The one-line docstring should begin with "Expect " and end with a period
    - This first line should be directly next to the triple quote, not on a new line
- There should be a blank line after the first line of the docstring, before any further descriptive text or sections (For Example, Args, Keyword Args, Other Parameters, Returns, Notes, and See Also)
    - There should be one blank newline between each section
- Any other sections of the docstring should be aligned to the start of the triple quote, not indented
    - Throughout the docstring, there should not be words/phrases in backticks (renders ugly)
- If there is not a line that says something like `expect_blah is a SpecificExpectationType` after the one-line docstring, add it

    ```
    is a [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).
    is a [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).
    is a [Batch Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations).
    is a [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).
    is a [Multicolumn Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations).
    is a [Regex-Based Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations).
    is a [Set-Based Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations).
    is a [Query Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations).
    ```
- Next, there may be some sentences explaining the Expectation if it is particularly complex
    - Multiple lines that are part of the same statement should end with a space and backslash (` \`), except the final line
- If there is a `For example:` section, directly under the "For" should be a pair of colons (`::`) on their own line
    - Then another blank newline after the `::` line, followed by a code block indented one level
    - See the docstring for [expect_column_distinct_values_to_be_in_set.py](https://github.com/great-expectations/great_expectations/blob/230392da481dc1eef26064e0523a29e7011b3b3c/great_expectations/expectations/core/expect_column_distinct_values_to_be_in_set.py#L47-L76)
- In the Args/Keyword Args/Other Parameters sections, each item/arg should be on its own line
    - The type of the arg should be in parentheses, before the colon
    - The descriptive text for the arg may be on a new line as long as the previous line ends with a space and a backslash (` \`)
    - There should NOT be a blank newline between the specific args
- Replace any :ref: tags (in old docstrings) with markdown links to current docs

    ```
    see [partition_object](https://docs.greatexpectations.io/docs/reference/expectations/distributional_expectations/#partition-objects)

    For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
    ```
- The Other Parameters and Returns sections should ALWAYS just be the following

    ```
    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.
    ```
- In the Notes section, any multi-line bullet points should have all lines but the last line of the same bullet point end with a space and backslash (` \`)
    - The Notes section is the only section that should have bullet points (starting with `* `)
        - The statements in the notes section are required to be bullet points
    - If there is a statement (in old docstrings) like "These fields in the result object are customized" followed by a code block of a dict, that statement and code block should be changed to a single bullet point for each key in the dict like: `____ (key) field in the result object is customized for this expectation to be a ___ (type) representing _____ (other descriptive stuff from original text)`
        - See the docstring for [expect_column_quantile_values_to_be_between.py](https://github.com/great-expectations/great_expectations/blob/230392da481dc1eef26064e0523a29e7011b3b3c/great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py#L130)
- If there is a See Also section to refer to other related Expectations, use markdown links

    ```
    See Also:
        [expect_blah](https://greatexpectations.io/expectations/expect_blah)
    ```
- The ending triple quotes should be on their own line with no blank newlines above it, unless it is only a one-line docstring

## Updating docstrings for Expectations that are rendered poorly in the Gallery

When the legacy site hosted by [readthedocs](https://readthedocs.org) was active, docstrings for Expectations were written using [reStructuredText](https://docutils.sourceforge.io/rst.html).

There was a large effort to reformat the full docstrings for core Expectations and the one-line docstrings for contrib Expectations across the following PRs: [6340](https://github.com/great-expectations/great_expectations/pull/6340), [6423](https://github.com/great-expectations/great_expectations/pull/6423), [6577](https://github.com/great-expectations/great_expectations/pull/6577), and [8353](https://github.com/great-expectations/great_expectations/pull/8353). That process lead to the creation of the [Docstring rules](#docstring-rules) section defined above.

There are plenty of contrib Expectations that need updates to their full docstrings, so please make a contribution if you notice the formatting of an Expectation's "details page" in the [Expectation Gallery](https://greatexpectations.io/expectations) to not be consistent with the core Expectations. Thanks!
