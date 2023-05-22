"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_sqlite_data" tests/integration/test_script_runner.py
```
"""

import pathlib
import great_expectations as gx

sqlite_database_path = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    ).resolve(strict=True)
)

my_table_name = "yellow_tripdata_sample_2019_01"


import great_expectations as gx

context = gx.get_context()

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py connection_string">
my_connection_string = "sqlite:///<PATH_TO_DB_FILE>"
# </snippet>

my_connection_string = f"sqlite:///{sqlite_database_path}"

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource_name">
datasource_name = "my_datasource"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource">
datasource = context.sources.add_sqlite(
    name=datasource_name, connection_string=my_connection_string
)
# </snippet>

assert f"connection_string: sqlite:///{sqlite_database_path}" in str(datasource)
assert "name: my_datasource" in str(datasource)
assert "type: sqlite" in str(datasource)


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_name">
asset_name = "my_asset"
asset_table_name = my_table_name
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py table_asset">
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
# </snippet>

assert table_asset

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_query">
asset_name = "my_query_asset"
query = "SELECT * from yellow_tripdata_sample_2019_01"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py query_table_asset">
query_asset = datasource.add_query_asset(name=asset_name, query=query)
# </snippet>

assert query_asset
