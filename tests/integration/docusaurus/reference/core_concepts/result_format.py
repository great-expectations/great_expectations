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

dataframe: pd.DataFrame = pd.DataFrame(
    {
        "pk_column": [
            "zero",
            "one",
            "two",
            "three",
            "four",
            "five",
            "six",
            "seven",
            "eight",
            "nine",
            "ten",
            "eleven",
            "twelve",
            "thirteen",
            "fourteen",
        ],
        "my_var": [
            "A",
            "B",
            "B",
            "C",
            "C",
            "C",
            "D",
            "D",
            "D",
            "D",
            "E",
            "E",
            "E",
            "E",
            "E",
        ],
    }
)

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
# <snippet>
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["B", "C", "D"],
    result_format={"result_format": "BOOLEAN_ONLY"},
)
# </snippet>
assert validation_result.success == False
assert validation_result.result == {}

# <snippet>
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var", value_set=["B", "C", "D"], result_format={"result_format": "BASIC"}
)
# </snippet>
assert validation_result.success == False
assert validation_result.result == {
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
}

# assert validation_result.result == "hello"

# <snippet>
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["B", "C", "D"],
    result_format={
        "result_format": "SUMMARY",
        "unexpected_index_column_names": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
# </snippet>
assert validation_result.success == False
assert validation_result.result == {
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
    "partial_unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "partial_unexpected_counts": [
        {"value": "E", "count": 5},
        {"value": "A", "count": 1},
    ],
}

# <snippet>
validation_result = my_validator.expect_column_values_to_be_in_set(
    column="my_var",
    value_set=["B", "C", "D"],
    result_format={
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
# </snippet>
assert validation_result.success == False
assert validation_result.result == {
    "element_count": 15,
    "unexpected_count": 6,
    "unexpected_percent": 40.0,
    "partial_unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_column_names": ["pk_column"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 40.0,
    "unexpected_percent_nonmissing": 40.0,
    "partial_unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "partial_unexpected_counts": [
        {"value": "E", "count": 5},
        {"value": "A", "count": 1},
    ],
    "unexpected_list": ["A", "E", "E", "E", "E", "E"],
    "unexpected_index_list": [
        {"my_var": "A", "pk_column": "zero"},
        {"my_var": "E", "pk_column": "ten"},
        {"my_var": "E", "pk_column": "eleven"},
        {"my_var": "E", "pk_column": "twelve"},
        {"my_var": "E", "pk_column": "thirteen"},
        {"my_var": "E", "pk_column": "fourteen"},
    ],
    "unexpected_index_query": [0, 10, 11, 12, 13, 14],
}


# Checkpoint
context.create_expectation_suite(expectation_suite_name="test_suite")
test_suite = context.get_expectation_suite(expectation_suite_name="test_suite")

expectation_config = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "my_var",
        "value_set": ["B", "C", "D"],
    },
)
test_suite.add_expectation(expectation_configuration=expectation_config)

context.save_expectation_suite(
    expectation_suite=test_suite,
    expectation_suite_name="test_suite",
    overwriting_existing=True,
)

# <snippet>
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
            "include_unexpected_rows": True,
            "return_unexpected_index_query": True,
        },
    },
}
# </snippet>
batch_request_for_pandas_unexpected_rows_and_index = {
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
context.add_checkpoint(
    **filter_properties_dict(
        properties=checkpoint_config.to_json_dict(),
        clean_falsy=True,
    ),
)
# noinspection PyProtectedMember
context._save_project_config()

result: CheckpointResult = context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    expectation_suite_name="test_suite",
    batch_request=batch_request_for_pandas_unexpected_rows_and_index,
)
evrs: List[ExpectationSuiteValidationResult] = result.list_validation_results()

result_index_list: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
    "unexpected_index_list"
]
assert result_index_list == [
    {"my_var": "A", "pk_column": "zero"},
    {"my_var": "E", "pk_column": "ten"},
    {"my_var": "E", "pk_column": "eleven"},
    {"my_var": "E", "pk_column": "twelve"},
    {"my_var": "E", "pk_column": "thirteen"},
    {"my_var": "E", "pk_column": "fourteen"},
]

result_index_query: List[int] = evrs[0]["results"][0]["result"][
    "unexpected_index_query"
]
assert result_index_query == [0, 10, 11, 12, 13, 14]
partial_unexpected_counts: List[Dict[str, Any]] = evrs[0]["results"][0]["result"][
    "partial_unexpected_counts"
]
assert partial_unexpected_counts == [
    {"value": "E", "count": 5},
    {"value": "A", "count": 1},
]
