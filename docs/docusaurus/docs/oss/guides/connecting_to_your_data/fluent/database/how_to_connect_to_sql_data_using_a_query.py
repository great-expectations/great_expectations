"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "how_to_connect_to_sql_data_using_a_query" tests/integration/test_script_runner.py
```
"""

import pathlib
import warnings

import great_expectations as gx
from great_expectations.datasource.fluent import GxDatasourceWarning

sqlite_database_path = pathlib.Path(
    gx.__file__,
    "..",
    "..",
    "tests",
    "test_sets",
    "taxi_yellow_tripdata_samples",
    "sqlite",
    "yellow_tripdata.db",
).resolve(strict=True)


context = gx.get_context()

connection_string = f"sqlite:///{sqlite_database_path}"

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=GxDatasourceWarning)
    # TODO: Remove create_temp_table=True once the bug fix goes in to develop that's equivalent of this PR for 0.18.6:
    #       https://github.com/great-expectations/great_expectations/pull/9148
    datasource = context.data_sources.add_sql(
        name="my_datasource",
        connection_string=connection_string,
        create_temp_table=True,
    )

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query.py datasource">
datasource = context.data_sources.get("my_datasource")
# </snippet>

# Python
# <snippet name="docs/docusaurus/docs/oss/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query.py add_query_asset">
query_asset = datasource.add_query_asset(
    name="my_asset",
    query="SELECT passenger_count, total_amount FROM yellow_tripdata_sample_2019_01",
)
# </snippet>

assert datasource.get_asset_names() == {"my_asset"}

my_asset = datasource.get_asset("my_asset")
assert my_asset

my_batch_request = my_asset.build_batch_request()
batch = my_asset.get_batch(my_batch_request)
assert set(batch.columns()) == {
    "passenger_count",
    "total_amount",
}
