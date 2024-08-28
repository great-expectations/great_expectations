import pathlib
import tempfile

import great_expectations as gx

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

# <snippet name="docs/docusaurus/docs/snippets/batch_request batch_request">
import great_expectations as gx

context = gx.get_context()

# data_directory is the full path to a directory containing csv files
datasource = context.data_sources.add_pandas_filesystem(
    name="my_pandas_datasource", base_directory=data_directory
)

# The batching_regex should max file names in the data_directory
asset = datasource.add_csv_asset(
    name="csv_asset",
)

batch_definition = asset.add_batch_definition_monthly(
    name="monthly_batch_definition",
    regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
)
batch_request = batch_definition.build_batch_request(
    batch_parameters={"year": "2019", "month": "02"}
)
# </snippet>

assert batch_request.datasource_name == "my_pandas_datasource"
assert batch_request.data_asset_name == "csv_asset"
assert batch_request.options == {"year": "2019", "month": "02"}

# <snippet name="docs/docusaurus/docs/snippets/batch_request options">
options = asset.get_batch_parameters_keys()
print(options)
# </snippet>

assert set(options) == {"path"}
