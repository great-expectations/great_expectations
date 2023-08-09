"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "how_to_connect_to_in_memory_data_using_pandas" tests/integration/test_script_runner.py
```
"""

import pathlib
import great_expectations as gx
import pyspark.pandas as ps

context = gx.get_context()

spark = gx.core.util.get_or_create_spark_application()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py datasource">
datasource = context.sources.add_spark("my_spark_datasource")
# </snippet>


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py dataframe">
df = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py name">
name = "example_dataframe"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py data_asset">
data_asset = datasource.add_dataframe_asset(name="my_df_asset")
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py build_batch_request_with_dataframe">
my_batch_request = data_asset.build_batch_request(dataframe=dataframe)
# </snippet>

assert my_batch_request.datasource_name == "my_spark_datasource"
assert my_batch_request.data_asset_name == "example_dataframe"
assert my_batch_request.options == {}

batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 1
assert set(batches[0].columns()) == {"a", "b", "c"}
