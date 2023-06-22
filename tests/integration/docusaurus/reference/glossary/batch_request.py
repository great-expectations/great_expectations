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

# <snippet name="tests/integration/docusaurus/reference/glossary/batch_request batch_request">
import great_expectations as gx

context = gx.get_context()

# data_directory is the full path to a directory containing csv files
datasource = context.sources.add_pandas_filesystem(
    name="my_pandas_datasource", base_directory=data_directory
)

# The batching_regex should max file names in the data_directory
asset = datasource.add_csv_asset(
    name="csv_asset",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["year", "month"],
)

batch_request = asset.build_batch_request(options={"year": "2019", "month": "02"})
# </snippet>

assert batch_request.datasource_name == "my_pandas_datasource"
assert batch_request.data_asset_name == "csv_asset"
assert batch_request.options == {"year": "2019", "month": "02"}

# <snippet name="tests/integration/docusaurus/reference/glossary/batch_request options">
options = asset.batch_request_options
print(options)
# </snippet>

assert set(options) == set(["year", "month", "path"])
