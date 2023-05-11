import pathlib
import great_expectations as gx
import tempfile
from great_expectations.core.expectation_configuration import ExpectationConfiguration

from great_expectations.core.expectation_suite import ExpectationSuite

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

# <snippet name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite create_asset">
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

# <snippet name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_asset">
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

# <snippet name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite name_suite">
expectation_suite_name = "my_onboarding_assistant_suite"
expectation_suite = context.add_expectation_suite(
    expectation_suite_name=expectation_suite_name
)
batch_request = asset.build_batch_request({"year": "2019", "month": "02"})
# </snippet>

assert context.get_expectation_suite(expectation_suite_name) is not None


# run the Data Assitant on excluded columns
exclude_column_names = [
    "VendorID",
    "pickup_datetime",
    "dropoff_datetime",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
]

data_assistant_result = context.assistants.onboarding.run(
    batch_request=batch_request,
    exclude_column_names=exclude_column_names,
)

data_assistant_suite: ExpectationSuite = data_assistant_result.get_expectation_suite(
    expectation_suite_name="my_onboarding_assistant_suite"
)
# show the expectations
data_assistant_suite.show_expectations_by_expectation_type()
# pick one Expectation


# want to update the value of one of them
expectation_config_dict: dict = {
    "expect_column_values_to_be_between": {
        "column": "total_amount",
        "domain": "column",
        "max_value": 3004.8,
        "min_value": -59.8,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    }
}

# turn it into an ExpectationConfiguration object
existing_config = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={
        "column": "total_amount",
        "domain": "column",
        "max_value": 3004.8,
        "min_value": -59.8,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    },
)
# update the value (min_value parameter)
updated_config = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={
        "column": "total_amount",
        "domain": "column",
        "max_value": 3004.8,
        #'min_value': -59.8,
        "min_value": -10,
        "mostly": 1.0,
        "strict_max": False,
        "strict_min": False,
    },
)
# add the expectation
data_assistant_suite.add_expectation(updated_config)

# find the expectation and see that it is updated
config_to_search = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
    kwargs={"column": "total_amount"},
)

found_config = data_assistant_suite.find_expectations(config_to_search, "domain")
assert found_config == [updated_config]


# how we can remove the expectation
data_assistant_suite.remove_expectation(
    updated_config, match_type="domain", remove_multiple_matches=False
)

found_config = data_assistant_suite.find_expectations(config_to_search, "domain")
assert found_config == []

# now that you have done this, have the updated ExpectationSuite
context.save_expectation_suite(expectation_suite=data_assistant_suite)
