import pathlib
import great_expectations as gx
import tempfile

temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()
data_directory = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
).resolve(strict=True)

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_asset">
import great_expectations as gx

context = gx.data_context.FileDataContext.create(full_path_to_project_directory)

# data_directory is the full path to a directory containing csv files
datasource = context.sources.add_pandas_filesystem(
    name="my_pandas_datasource", base_directory=data_directory
)

# The batching_regex should max files in the data_directory
asset = datasource.add_csv_asset(
    name="csv_asset",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["year", "month"],
)
# </snippet>

assert "my_pandas_datasource" in context.datasources
assert context.datasources["my_pandas_datasource"].get_asset("csv_asset") is not None

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler get_asset">
import great_expectations as gx
import pathlib

context = gx.get_context(
    context_root_dir=(
        pathlib.Path(full_path_to_project_directory) / "great_expectations"
    )
)
asset = context.datasources["my_pandas_datasource"].get_asset("csv_asset")
# </snippet>

assert "my_pandas_datasource" in context.datasources
assert context.datasources["my_pandas_datasource"].get_asset("csv_asset") is not None

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler name_suite">
expectation_suite_name = "insert_the_name_of_your_suite_here"
expectation_suite = context.add_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
batch_request = asset.build_batch_request({"year": "2019", "month": "02"})
# </snippet>

assert context.get_expectation_suite(expectation_suite_name) is not None

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_validator">
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=expectation_suite_name
)
validator.head()
# </snippet>

assert validator.active_batch.metadata["year"] == "2019"
assert validator.active_batch.metadata["month"] == "02"
assert validator.expectation_suite.name == expectation_suite_name

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler create_profiler">

from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)

profiler = UserConfigurableProfiler(profile_dataset=validator)
# </snippet>

assert "passenger_count" in profiler.column_info

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler build_suite">
suite = profiler.build_suite()
# </snippet>

assert suite.expectation_suite_name == expectation_suite_name
assert len(suite.expectations) > 0

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler e2e">
from great_expectations.checkpoint.checkpoint import SimpleCheckpoint

# Review and save our Expectation Suite
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

# Set up and run a Simple Checkpoint for ad hoc validation of our data
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
checkpoint = SimpleCheckpoint(
    f"{validator.active_batch_definition.data_asset_name}_{expectation_suite_name}",
    context,
    **checkpoint_config,
)
checkpoint_result = checkpoint.run()

# Build Data Docs
context.build_data_docs()

# Get the only validation_result_identifier from our SimpleCheckpoint run, and open Data Docs to that page
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)
# </snippet>

assert len(checkpoint_result.list_validation_results()) == 1
assert checkpoint_result.list_validation_results()[0]["success"] == True

suite = None
# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler optional_params">
excluded_expectations = ["expect_column_quantile_values_to_be_between"]
ignored_columns = [
    "rate_code_id",
    "pickup_location_id",
    "payment_type",
    "pickup_datetime",
]
not_null_only = True
table_expectations_only = False
value_set_threshold = "unique"

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=expectation_suite_name
)

profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=excluded_expectations,
    ignored_columns=ignored_columns,
    not_null_only=not_null_only,
    table_expectations_only=table_expectations_only,
    value_set_threshold=value_set_threshold,
)

suite = profiler.build_suite()
# </snippet>

assert suite is not None
suite = None

# <snippet name="tests/integration/docusaurus/expectations/how_to_create_and_edit_expectations_with_a_profiler semantic">
semantic_types_dict = {
    "numeric": ["fare_amount"],
    "value_set": ["rate_code_id", "pickup_location_id", "payment_type"],
    "datetime": ["pickup_datetime"],
}

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name=expectation_suite_name
)

profiler = UserConfigurableProfiler(
    profile_dataset=validator, semantic_types_dict=semantic_types_dict
)
suite = profiler.build_suite()
# </snippet>

assert suite is not None
