"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_a_sql_table" tests/integration/test_script_runner.py --postgresql
```
"""
import tests.test_utils as test_utils
import great_expectations as gx
import pathlib
import great_expectations as gx

csv_path = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
    "yellow_tripdata_sample_2019-01.csv",
).resolve(strict=True)

"./data/yellow_tripdata_sample_2019-01.csv"

context = gx.get_context()

sql_connection_string = test_utils.get_default_postgres_url()

test_utils.load_data_into_test_database(
    table_name="yellow_tripdata_sample",
    csv_path=csv_path,
    connection_string=sql_connection_string,
)

datasource = context.sources.add_sql(
    name="my_datasource", connection_string=sql_connection_string
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py datasource">
datasource = context.get_datasource("my_datasource")
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py create_datasource">
table_asset = datasource.add_table_asset(
    name="my_asset", table_name="yellow_tripdata_sample"
)
# </snippet>

assert "my_datasource" in context.datasources
assert table_asset
assert table_asset.table_name == "yellow_tripdata_sample"
