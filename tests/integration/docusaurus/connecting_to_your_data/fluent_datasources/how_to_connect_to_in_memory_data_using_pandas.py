"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_in_memory_data_using_pandas" tests/integration/test_script_runner.py
```
"""

import pathlib
import great_expectations as gx

context = gx.get_context()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py datasource">
datasource = context.sources.add_pandas(name="my_pandas_datasource")
# </snippet>


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py dataframe">
import pandas as pd

dataframe = pd.read_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet"
)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py name">
name = "taxi_dataframe"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py data_asset">
data_asset = datasource.add_dataframe_asset(name=name)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py build_batch_request_with_dataframe">
my_batch_request = data_asset.build_batch_request(dataframe=dataframe)
# </snippet>

assert my_batch_request.datasource_name == "my_pandas_datasource"
assert my_batch_request.data_asset_name == "taxi_dataframe"
assert my_batch_request.options == {}

batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 1
assert set(batches[0].columns()) == {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
}
