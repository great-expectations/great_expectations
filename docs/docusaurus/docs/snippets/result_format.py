import pandas as pd

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite

# Snippet: example data frame for result_format
# <snippet name="docs/docusaurus/docs/snippets/result_format pandas_df_for_result_format">
dataframe = pd.DataFrame(
    {
        "pk_column": ["zero", "one", "two", "three", "four", "five", "six", "seven"],
        "my_var": ["A", "B", "B", "C", "C", "C", "D", "D"],
        "my_numbers": [1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0],
    }
)
# </snippet>


# NOTE: The following code is only for testing and can be ignored by users.
context = gx.get_context()
datasource = context.data_sources.add_pandas(name="my_pandas_datasource")
data_asset = datasource.add_dataframe_asset(name="my_df")
my_batch_request = data_asset.build_batch_request(options={"dataframe": dataframe})
context.suites.add(ExpectationSuite(name="my_expectation_suite"))
my_validator = context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name="my_expectation_suite",
)

# Expectation-level Configuration
# Snippet: result_format BOOLEAN example
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_boolean_example">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["A", "B"],
    result_format={"result_format": "BOOLEAN_ONLY"},
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_boolean_example_output">
assert validation_result.success is False
assert validation_result.result == {}
# </snippet>


# Snippet: result_format BASIC example with set
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_basic_example_set">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var", value_set=["A", "B"], result_format={"result_format": "BASIC"}
)
# </snippet>
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_basic_example_set_output">
assert validation_result.success is False
assert validation_result.result == {
    "element_count": 8,
    "unexpected_count": 5,
    "unexpected_percent": 62.5,
    "partial_unexpected_list": ["C", "C", "C", "D", "D"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 62.5,
    "unexpected_percent_nonmissing": 62.5,
}
# </snippet>

# Snippet: result_format BASIC example with aggregate
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_basic_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="BASIC"
)
# </snippet>
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_basic_example_agg_output">
assert validation_result.success is True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>


# Snippet: result_format SUMMARY example with set
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_summary_example_set">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["A", "B"],
    result_format={
        "result_format": "SUMMARY",
        "unexpected_index_column_names": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_summary_example_set_output">
assert validation_result.success is False
assert validation_result.result == {
    "element_count": 8,
    "unexpected_count": 5,
    "unexpected_percent": 62.5,
    "partial_unexpected_list": ["C", "C", "C", "D", "D"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 62.5,
    "unexpected_percent_nonmissing": 62.5,
    "partial_unexpected_index_list": [
        {"my_var": "C", "pk_column": "three"},
        {"my_var": "C", "pk_column": "four"},
        {"my_var": "C", "pk_column": "five"},
        {"my_var": "D", "pk_column": "six"},
        {"my_var": "D", "pk_column": "seven"},
    ],
    "partial_unexpected_counts": [
        {"value": "C", "count": 3},
        {"value": "D", "count": 2},
    ],
}
# </snippet>

# Snippet: result_format SUMMARY example with agg
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_summary_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="SUMMARY"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_summary_example_agg_output">
assert validation_result.success is True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>

# Snippet: result_format COMPLETE example with set
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_complete_example_set">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["A", "B"],
    result_format={
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_complete_example_set_output">
assert validation_result.success is False
assert validation_result.result == {
    "element_count": 8,
    "unexpected_count": 5,
    "unexpected_percent": 62.5,
    "partial_unexpected_list": ["C", "C", "C", "D", "D"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 62.5,
    "unexpected_percent_nonmissing": 62.5,
    "partial_unexpected_index_list": [
        {"my_var": "C", "pk_column": "three"},
        {"my_var": "C", "pk_column": "four"},
        {"my_var": "C", "pk_column": "five"},
        {"my_var": "D", "pk_column": "six"},
        {"my_var": "D", "pk_column": "seven"},
    ],
    "partial_unexpected_counts": [
        {"value": "C", "count": 3},
        {"value": "D", "count": 2},
    ],
    "unexpected_list": ["C", "C", "C", "D", "D"],
    "unexpected_index_list": [
        {"my_var": "C", "pk_column": "three"},
        {"my_var": "C", "pk_column": "four"},
        {"my_var": "C", "pk_column": "five"},
        {"my_var": "D", "pk_column": "six"},
        {"my_var": "D", "pk_column": "seven"},
    ],
    "unexpected_index_query": "df.filter(items=[3, 4, 5, 6, 7], axis=0)",
}

# </snippet>

# Snippet: result_format COMPLETE example with agg
# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_complete_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="COMPLETE"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/snippets/result_format result_format_complete_example_agg_output">
assert validation_result.success is True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>


# <snippet name="docs/docusaurus/docs/snippets/result_format.py result_format_checkpoint_example">
# </snippet>
