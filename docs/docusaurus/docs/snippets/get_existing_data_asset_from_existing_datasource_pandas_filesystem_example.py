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

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_datasource">
# data_directory is the full path to a directory containing csv files
my_datasource = context.data_sources.add_pandas_filesystem(
    name="my_datasource", base_directory=data_directory
)
# </snippet>

my_asset = my_datasource.add_csv_asset(
    name="my_asset",
)
my_batch_definition = my_asset.add_batch_definition_monthly(
    name="my_batch_definition",
    regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    sort_ascending=True,
)

import pandas as pd

dataframe = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
my_ephemeral_datasource = context.data_sources.add_pandas(name="my_pandas_datasource")
my_asset = my_ephemeral_datasource.add_dataframe_asset(name="my_ephemeral_asset")

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py build_batch_request_with_dataframe">
my_batch_request = my_asset.build_batch_request(options={"dataframe": dataframe})
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_asset">
my_asset = context.data_sources.get("my_datasource").get_asset("my_asset")
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_parameters">
print(my_asset.get_batch_parameters_keys(partitioner=my_batch_definition.partitioner))
# </snippet>

assert my_asset.get_batch_parameters_keys(
    partitioner=my_batch_definition.partitioner
) == ("path", "year", "month")

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_request">
my_batch_request = my_batch_definition.build_batch_request()
# </snippet>

assert my_batch_request.datasource_name == "my_datasource"
assert my_batch_request.data_asset_name == "my_asset"
assert my_batch_request.options == {}

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py my_batch_list">
batch = my_asset.get_batch(my_batch_request)
# </snippet>

batch_spec = batch.batch_spec
assert batch_spec.reader_method == "read_csv"
assert batch_spec.reader_options == {}
assert batch.data.dataframe.shape == (10000, 18)

# Python
# <snippet name="docs/docusaurus/docs/snippets/get_existing_data_asset_from_existing_datasource_pandas_filesystem_example.py print_batch_spec">
print(batch.batch_spec)
# </snippet>
