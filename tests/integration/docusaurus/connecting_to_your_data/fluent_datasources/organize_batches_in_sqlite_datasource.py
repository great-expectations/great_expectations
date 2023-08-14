import pathlib
import great_expectations as gx
import tempfile

temp_dir = tempfile.TemporaryDirectory()
full_path_to_project_directory = pathlib.Path(temp_dir.name).resolve()
yellow_tripdata_db_file = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
    "sqlite",
    "yellow_tripdata_sample_2020_all_months_combined.db",
).resolve(strict=True)

import great_expectations as gx

context = gx.get_context()

sql_connection_string = f"sqlite:///{yellow_tripdata_db_file}"
my_datasource = context.sources.add_sqlite(
    name="my_datasource", connection_string=sql_connection_string
)

my_datasource.add_table_asset(
    name="my_table_asset", table_name="yellow_tripdata_sample_2020", schema_name=None
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_datasource">
my_datasource = context.get_datasource("my_datasource")
my_table_asset = my_datasource.get_asset(asset_name="my_table_asset")
# </snippet>


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_splitter_year_and_month">
my_table_asset.add_splitter_year_and_month(column_name="pickup_datetime")
# </snippet>

my_batch_request = my_table_asset.build_batch_request()
batches = my_table_asset.get_batch_list_from_batch_request(my_batch_request)

assert len(batches) == 12

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_sorters">
my_asset = my_table_asset.add_sorters(["+year", "-month"])
# </snippet>

assert my_asset.batch_request_options == ("year", "month")

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_batch_list">
my_batch_request = my_table_asset.build_batch_request()
batches = my_table_asset.get_batch_list_from_batch_request(my_batch_request)
# </snippet>

assert my_batch_request.datasource_name == "my_datasource"
assert my_batch_request.data_asset_name == "my_table_asset"
assert my_batch_request.options == {}

assert len(batches) == 12

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py print_batch_spec">
for batch in batches:
    print(batch.batch_spec)
# </snippet>
