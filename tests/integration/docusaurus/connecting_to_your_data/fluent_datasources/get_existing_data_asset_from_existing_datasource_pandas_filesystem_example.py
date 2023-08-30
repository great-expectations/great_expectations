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
    "first_3_files",
).resolve(strict=True)

import great_expectations as gx

context = gx.get_context()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_datasource">
# data_directory is the full path to a directory containing csv files
my_datasource = context.sources.add_pandas_filesystem(
    name="my_datasource", base_directory=data_directory
)
# </snippet>

my_asset = my_datasource.add_csv_asset(
    name="my_asset",
    batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    order_by=["year", "month"],
)

import pandas as pd

dataframe = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
my_ephemeral_datasource = context.sources.add_pandas(name="my_pandas_datasource")
my_asset = my_ephemeral_datasource.add_dataframe_asset(name="my_ephemeral_asset")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py build_batch_request_with_dataframe">
my_batch_request = my_asset.build_batch_request(dataframe=dataframe)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_asset">
my_asset = context.get_datasource("my_datasource").get_asset("my_asset")
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_request_options">
print(my_asset.batch_request_options)
# </snippet>

assert my_asset.batch_request_options == ("year", "month", "path")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_request">
my_batch_request = my_asset.build_batch_request()
# </snippet>

assert my_batch_request.datasource_name == "my_datasource"
assert my_batch_request.data_asset_name == "my_asset"
assert my_batch_request.options == {}

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_list">
batches = my_asset.get_batch_list_from_batch_request(my_batch_request)
# </snippet>

assert len(batches) == 3

for batch in batches:
    batch_spec = batch.batch_spec
    assert batch_spec.reader_method == "read_csv"
    assert batch_spec.reader_options == {}
    assert batch.data.dataframe.shape == (10000, 18)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py print_batch_spec">
for batch in batches:
    print(batch.batch_spec)
# </snippet>
