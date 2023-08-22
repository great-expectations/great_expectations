from typing import Any, Dict, List

import pandas as pd
import great_expectations as gx
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
    IDDict,
)
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.util import filter_properties_dict
from great_expectations.validator.validator import Validator

# Snippet: example data frame for result_format
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/pandas_df_for_result_format">
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
datasource = context.sources.add_pandas(name="my_pandas_datasource")
data_asset = datasource.add_dataframe_asset(name="my_df")
my_batch_request = data_asset.build_batch_request(dataframe=dataframe)
context.add_or_update_expectation_suite("my_expectation_suite")
my_validator = context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name="my_expectation_suite",
)

# Expectation-level Configuration
# Snippet: result_format BOOLEAN example
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_boolean_example">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["A", "B"],
    result_format={"result_format": "BOOLEAN_ONLY"},
)
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_boolean_example_output">
assert validation_result.success == False
assert validation_result.result == {}
# </snippet>


# Snippet: result_format BASIC example with set
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_set">
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var", value_set=["A", "B"], result_format={"result_format": "BASIC"}
)
# </snippet>
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_set_output">
assert validation_result.success == False
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
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="BASIC"
)
# </snippet>
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_basic_example_agg_output">
assert validation_result.success == True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>


# Snippet: result_format SUMMARY example with set
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_set">
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

# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_set_output">
assert validation_result.success == False
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
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="SUMMARY"
)
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_summary_example_agg_output">
assert validation_result.success == True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>

# Snippet: result_format COMPLETE example with set
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_set">
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

# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_set_output">
assert validation_result.success == False
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
# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_agg">
validation_result = my_validator.expect_column_mean_to_be_between(
    column="my_numbers", min_value=0.0, max_value=10.0, result_format="COMPLETE"
)
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_complete_example_agg_output">
assert validation_result.success == True
assert validation_result.result == {"observed_value": 2.75}
# </snippet>


# Checkpoint
# NOTE: The following code is only for testing and can be ignored by users.
context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
test_suite = context.get_expectation_suite(expectation_suite_name="test_suite")

expectation_config = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "my_var",
        "value_set": ["A", "B"],
    },
)
test_suite.add_expectation(expectation_configuration=expectation_config)

test_suite.expectation_suite_name = "test_suite"
context.add_or_update_expectation_suite(
    expectation_suite=test_suite,
)


# <snippet name="tests/integration/docusaurus/reference/core_concepts/result_format/result_format_checkpoint_example">
checkpoint: Checkpoint = Checkpoint(
    name="my_checkpoint",
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=my_batch_request,
    expectation_suite_name="test_suite",
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
    runtime_configuration={
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_column"],
            "return_unexpected_index_query": True,
        },
    },
)
# </snippet>

context.add_or_update_checkpoint(checkpoint=checkpoint)

results: CheckpointResult = checkpoint.run()
evrs: List[ExpectationSuiteValidationResult] = results.list_validation_results()

result_index_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
    "unexpected_index_list"
]
assert result_index_list == [
    {"my_var": "C", "pk_column": "three"},
    {"my_var": "C", "pk_column": "four"},
    {"my_var": "C", "pk_column": "five"},
    {"my_var": "D", "pk_column": "six"},
    {"my_var": "D", "pk_column": "seven"},
]

result_index_query: List[int] = evrs[0]["results"][0]["result"][
    "unexpected_index_query"
]
assert result_index_query == "df.filter(items=[3, 4, 5, 6, 7], axis=0)"
partial_unexpected_counts: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
    "partial_unexpected_counts"
]
assert partial_unexpected_counts == [
    {"value": "C", "count": 3},
    {"value": "D", "count": 2},
]
