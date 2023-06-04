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

my_batch_request = data_asset.build_batch_request(dataframe=dataframe)
assert my_batch_request.datasource_name == "my_pandas_datasource"
assert my_batch_request.data_asset_name == "taxi_dataframe"
assert my_batch_request.options == {}
