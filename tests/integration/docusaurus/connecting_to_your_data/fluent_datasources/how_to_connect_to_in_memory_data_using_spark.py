"""
To run this code as a local test, use the following console command:
```
pytest -vv --docs-tests -k "how_to_connect_to_in_memory_data_using_spark" tests/integration/test_script_runner.py
```
"""
import os
import pandas as pd
import great_expectations as gx
from great_expectations.compatibility.not_imported import is_version_greater_or_equal

# Required by pyarrow>=2.0.0 within Spark to suppress UserWarning
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

# Required to make following test compatible with both Pandas > 2.0.0 and Pandas < 2.0.0
if is_version_greater_or_equal(pd.__version__, "2.0.0"):
    pd.DataFrame.iteritems = pd.DataFrame.items

context = gx.get_context()

spark = gx.core.util.get_or_create_spark_application()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py datasource">
datasource = context.sources.add_spark("my_spark_datasource")
# </snippet>


# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py dataframe">
df = pd.DataFrame(
    {
        "a": [1, 2, 3, 4, 5, 6],
        "b": [100, 200, 300, 400, 500, 600],
        "c": ["one", "two", "three", "four", "five", "six"],
    },
    index=[10, 20, 30, 40, 50, 60],
)

dataframe = spark.createDataFrame(data=df)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py name">
name = "my_df_asset"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py data_asset">
data_asset = datasource.add_dataframe_asset(name=name)
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py build_batch_request_with_dataframe">
my_batch_request = data_asset.build_batch_request(dataframe=dataframe)
# </snippet>

assert my_batch_request.datasource_name == "my_spark_datasource"
assert my_batch_request.data_asset_name == "my_df_asset"
assert my_batch_request.options == {}

batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 1
assert set(batches[0].columns()) == {"a", "b", "c"}
