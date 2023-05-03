"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_one_or_more_files_using_pandas" tests/integration/test_script_runner.py
```
"""
import pathlib


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py define_args">
datasource_name = "my_new_datasource"
path_to_csv_files = "<INSERT_PATH_TO_FILES_HERE>"
# </snippet>

path_to_csv_files = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
    ).resolve(strict=True)
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py create_datasource">
datasource = context.sources.add_pandas_filesystem(
    name=datasource_name, base_directory=path_to_csv_files
)
# </snippet>

assert datasource_name in context.datasources

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_pandas.py add_asset">
asset_name = "my_csv_asset"
filename_as_regex = r"yellow_tripdata_sample_2018-01\.csv"
datasource.add_csv_asset(name=asset_name, batching_regex=filename_as_regex)

print(datasource)
# </snippet>

assert datasource.get_asset(asset_name)
