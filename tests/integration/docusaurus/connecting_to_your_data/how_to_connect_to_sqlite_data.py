import pathlib
import sqlite3


def get_sqlite_database_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    return pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)


sqlite_database_path = get_sqlite_database_path()
assert sqlite_database_path.is_file()


def get_table_name(sqlite_database_path: pathlib.Path) -> list:
    conn = sqlite3.connect(sqlite_database_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
    return c.fetchone()[0]


my_table_name = get_table_name(sqlite_database_path)
assert my_table_name is not None


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py import">
import great_expectations as gx

context = gx.get_context()
# </snippet>
assert context is not None

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py connection_string">
my_connection_string = f"sqlite:///{sqlite_database_path}"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py datasource_name">
datasource_name = "my_datasource"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py datasource">
datasource = context.sources.add_sqlite(
    name=datasource_name, connection_string=my_connection_string
)
# </snippet>

assert f"connection_string: sqlite:///{sqlite_database_path}" in str(datasource)
assert "name: my_datasource" in str(datasource)
assert "type: sqlite" in str(datasource)


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py asset_name">
asset_name = "my_asset"
asset_table_name = my_table_name
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py table_asset">
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
# </snippet>

assert table_asset is not None


# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py asset_query">
asset_name = "my_query_asset"
asset_query = "SELECT * from yellow_tripdata_sample_2019_01"
# </snippet>

# <snippet name="tests/integration/docusaurus/connecting_to_your_data/how_to_connect_to_sqlite_data.py query_table_asset">
table_asset = datasource.add_query_asset(name=asset_name, query=asset_query)
# </snippet>

assert table_asset is not None
