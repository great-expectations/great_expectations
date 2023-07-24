Expectation Docstring Formatting
================================

The following rules were applied when doing some mass docstring reformatting in [PR 6340](https://github.com/great-expectations/great_expectations/pull/6340), [PR 6423](https://github.com/great-expectations/great_expectations/pull/6423), [PR 6577](https://github.com/great-expectations/great_expectations/pull/6577), and [PR 8353](https://github.com/great-expectations/great_expectations/pull/8353).

- The 1-line docstring should begin with "Expect" and end with a period
- There should be a blank line after the first line of the docstring and it should be next to the triple quote (not on a new line)
    - If there is only a 1-line docstring, move on to the next Expectation
    - If there is anything else, there should be at least one empty newline after the 1-line docstring, before other content
- Any other sections of the docstring should be aligned to the start of the triple quote, not indented
- Throughout the docstring, there should not be words/phrases in backticks (renders ugly)
- There may be some sentences explaining the Expectation if it is particularly complex
    - Multiple lines that are part of the same statement should end with a space and backslash (` \`), except the final line
- There should be one blank newline between each section
- If there is a `For example:` section, directly under the "For" should be a pair of colons (`::`) on their own line
- If there is not a line that says something like `expect_blah is a SpecificExpectationType`, add it

    ```
    is a [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).
    is a [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).
    is a [Batch Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations).
    is a [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).
    is a [Multicolumn Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations).
    ```
- In the Args/Keyword Args/Other Parameters sections, each item should be on its own line
    - if the item has nested details, each line except the final line should end with a space and backslash (` \`)
- In the Args/Keyword Args/Other Parameters sections, the type of the arg should be in parentheses, before the colon
- In the Args/Keyword Args/Other Parameters sections, there should not be a blank newline between the specific args
- Replace any :ref: tags with markdown links to current docs

    ```
    see [partition_object](https://docs.greatexpectations.io/docs/reference/expectations/distributional_expectations/#partition-objects)

    For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
    ```
- The Other Parameters and Returns section should ALWAYS just be the following

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
- In the Notes section, if there is a statement like "These fields in the result object are customized" followed by a code block of a dict, that statement and code block should be changed to a single bullet point for each key in the dict like: `____ (key) field in the result object is customized for this expectation to be a ___ (type) representing _____ (other descriptive stuff from original text)`
- In the Notes section, any multi-line bullet points should have all lines but the last line of the same bullet point end with a space and backslash (` \`)
- The Notes section is the only section that should have bullet points (starting with `* `)
    - the statements in the notes section are required to be bullet points
- If there is a `See Also:` section (typically to other related Expectations), use markdown links

    ```
    See Also:
        [expect_blah](https://greatexpectations.io/expectations/expect_blah)
    ```
- The ending triple quotes should be on their own line with no blank newlines above it, unless it is only a 1-line docstring
