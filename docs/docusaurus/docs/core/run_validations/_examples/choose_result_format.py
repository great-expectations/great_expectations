## This is the set up.

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core import ValidationDefinition
from great_expectations.core.expectation_suite import ExpectationSuite

context = gx.get_context()

datasource_name = "all_csv_files"
path_to_datasource_files = "/Users/rachelreverie/PycharmProjects/pythonProject/core_sandbox/2024_05_23_notebooks/data/taxi_yellow_tripdata_samples"
data_source = context.data_sources.add_pandas_filesystem(
    name=datasource_name, base_directory=path_to_datasource_files
)

asset_name = "csv_files"
csv_asset = data_source.add_csv_asset(asset_name)

batch_definition_name = "2018-06_taxi2"
relative_file_path = "yellow_tripdata_sample_2018-06.csv"
batch_definition = csv_asset.add_batch_definition_path(
    batch_definition_name, relative_file_path
)

suite_name = "my_expectation_suite"
suite = context.suites.add(ExpectationSuite(name=suite_name))

# ColumnMap
expectation = gxe.ExpectColumnValuesToBeInSet(
    column="passenger_count", value_set=[1, 2, 3, 4, 5]
)
suite.add_expectation(expectation)

# ColumnAggregate
expectation = gxe.ExpectColumnMaxToBeBetween(
    column="passenger_count", min_value=4, max_value=5
)
suite.add_expectation(expectation)

validation_definition_name = "my_validation_definition"
validation_definition = ValidationDefinition(
    data=batch_definition, suite=suite, name=validation_definition_name
)

## This is where the procedure starts.

import great_expectations as gx

context = gx.get_context()

validation_name = "my_validation_definition"
validation_definition = context.validation_definitions.get(validation_name)

# BOOLEAN_ONLY Result Format
boolean_result_format_dict = {}
boolean_result_format_dict["result_format"] = "BOOLEAN_ONLY"
boolean_only_result = validation_definition.run(
    result_format=boolean_result_format_dict
)
print(boolean_only_result)

# BASIC Result Format
basic_result_format_dict = {}
basic_result_format_dict["result_format"] = "BASIC"
basic_result = validation_definition.run(result_format=basic_result_format_dict)
print(basic_result)

# SUMMARY Result Format
summary_result_format_dict = {}
summary_result_format_dict["result_format"] = "SUMMARY"
summary_result = validation_definition.run(result_format=summary_result_format_dict)
print(summary_result)

# COMPLETE Result Format
complete_result_format_dict = {}
complete_result_format_dict["result_format"] = "COMPLETE"
complete_result = validation_definition.run(result_format=complete_result_format_dict)
print(complete_result)
