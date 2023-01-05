import pandas as pd

import great_expectations as gx
from great_expectations.core import IDDict
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.execution_engine import PandasExecutionEngine
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


result = my_validator.expect_column_values_to_be_in_set(
    "my_var", ["B", "C", "D"], result_format={"result_format": "BOOLEAN_ONLY"}
)
print(result.result)


result = my_validator.expect_column_values_to_be_in_set(
    "my_var", ["B", "C", "D"], result_format={"result_format": "BASIC"}
)
print(result.result)

result = my_validator.expect_column_values_to_be_in_set(
    "my_var",
    ["B", "C", "D"],
    result_format={
        "result_format": "SUMMARY",
        "unexpected_index_columns": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
print(result.result)


result = my_validator.expect_column_values_to_be_in_set(
    "my_var",
    ["B", "C", "D"],
    result_format={
        "result_format": "COMPLETE",
        "unexpected_index_columns": ["pk_column"],
        "return_unexpected_index_query": True,
    },
)
print(result.result)
