import pathlib
import tempfile

import great_expectations as gx
from great_expectations.core.partitioners import ColumnPartitionerMonthly

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
my_datasource = context.data_sources.add_sqlite(
    name="my_datasource", connection_string=sql_connection_string
)

my_datasource.add_table_asset(
    name="my_table_asset", table_name="yellow_tripdata_sample_2020", schema_name=None
)

# Python
# <snippet name="docs/docusaurus/docs/snippets/organize_batches_in_sqlite_datasource.py my_datasource">
my_datasource = context.data_sources.get("my_datasource")
my_table_asset = my_datasource.get_asset(name="my_table_asset")
# </snippet>


# Python
# <snippet name="docs/docusaurus/docs/snippets/organize_batches_in_sqlite_datasource.py add_splitter_year_and_month">
partitioner = ColumnPartitionerMonthly(column_name="pickup_datetime")
# </snippet>

my_batch_request = my_table_asset.build_batch_request(partitioner=partitioner)
batch = my_table_asset.get_batch(my_batch_request)


assert my_table_asset.get_batch_parameters_keys(partitioner=partitioner) == (
    "year",
    "month",
)

# Python
# <snippet name="docs/docusaurus/docs/snippets/organize_batches_in_sqlite_datasource.py my_batch_list">
my_batch_request = my_table_asset.build_batch_request(partitioner=partitioner)
batch = my_table_asset.get_batch(my_batch_request)
# </snippet>

assert my_batch_request.datasource_name == "my_datasource"
assert my_batch_request.data_asset_name == "my_table_asset"
assert my_batch_request.options == {}

# Python
# <snippet name="docs/docusaurus/docs/snippets/organize_batches_in_sqlite_datasource.py print_batch_spec">
print(batch.batch_spec)
# </snippet>
