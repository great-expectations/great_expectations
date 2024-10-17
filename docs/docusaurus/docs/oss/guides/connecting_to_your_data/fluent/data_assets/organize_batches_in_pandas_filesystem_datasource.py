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
    "first_3_files",
).resolve(strict=True)

import great_expectations as gx

context = gx.get_context()

# data_directory is the full path to a directory containing csv files
context.data_sources.add_pandas_filesystem(
    name="my_datasource", base_directory=data_directory
)

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py my_datasource">
my_datasource = context.data_sources.get("my_datasource")
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py my_batching_regex">
my_batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py my_asset">
my_asset = my_datasource.add_csv_asset(
    name="my_taxi_data_asset", batching_regex=my_batching_regex
)
# </snippet>

assert my_asset.get_batch_parameters_keys() == ("year", "month", "path")

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py my_batch_list">
my_batch_request = my_asset.build_batch_request()
batch = my_asset.get_batch(my_batch_request)
# </snippet>

assert my_batch_request.datasource_name == "my_datasource"
assert my_batch_request.data_asset_name == "my_taxi_data_asset"
assert my_batch_request.options == {}


batch_spec = batch.batch_spec
assert batch_spec.reader_method == "read_csv"
assert batch_spec.reader_options == {}
assert batch.data.dataframe.shape == (10000, 18)

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/data_assets/organize_batches_in_pandas_filesystem_datasource.py print_batch_spec">
print(batch.batch_spec)
# </snippet>
