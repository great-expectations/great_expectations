from typing import Any, Dict, List

import pandas as pd

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationSuiteValidationResult,
    IDDict,
)
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
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
data_context_config: DataContextConfig = DataContextConfig(
    datasources={  # type: ignore[arg-type]
        "pandas_datasource": {
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "data_connectors": {
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": [
                        "id_key_0",
                        "id_key_1",
                    ],
                }
            },
        },
    },
    expectations_store_name="expectations_store",
    validations_store_name="validations_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    checkpoint_store_name="checkpoint_store",
    store_backend_defaults=InMemoryStoreBackendDefaults(),
)
context = BaseDataContext(project_config=data_context_config)
batch_definition = BatchDefinition(
    datasource_name="pandas_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="my_asset",
    batch_identifiers=IDDict({}),
    batch_spec_passthrough=None,
)
batch = Batch(
    data=dataframe,
    batch_definition=batch_definition,
)
engine = PandasExecutionEngine()
my_validator: Validator = Validator(
    execution_engine=engine,
    data_context=context,
    batches=[
        batch,
    ],
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
checkpoint_dict: dict = {
    "name": "my_checkpoint",
    "config_version": 1.0,
    "class_name": "Checkpoint",  # or SimpleCheckpoint
    "module_name": "great_expectations.checkpoint",
    "template_name": None,
    "run_name_template": "%Y-%M-foo-bar-template-test",
    "expectation_suite_name": None,
    "batch_request": None,
    "profilers": [],
    "action_list": [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {
            "name": "store_evaluation_params",
            "action": {"class_name": "StoreEvaluationParametersAction"},
        },
        {
            "name": "update_data_docs",
            "action": {"class_name": "UpdateDataDocsAction"},
        },
    ],
    "validations": [],
    "runtime_configuration": {
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_column"],
            "return_unexpected_index_query": True,
        },
    },
}
# </snippet>
batch_request = {
    "datasource_name": "pandas_datasource",
    "data_connector_name": "runtime_data_connector",
    "data_asset_name": "IN_MEMORY_DATA_ASSET",
    "runtime_parameters": {
        "batch_data": dataframe,
    },
    "batch_identifiers": {
        "id_key_0": 1234567890,
    },
}

checkpoint_config = CheckpointConfig(**checkpoint_dict)
context.add_or_update_checkpoint(
    **filter_properties_dict(
        properties=checkpoint_config.to_json_dict(),
        clean_falsy=True,
    ),
)
context._save_project_config()

result: CheckpointResult = context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    expectation_suite_name="test_suite",
    batch_request=batch_request,
)
evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

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
