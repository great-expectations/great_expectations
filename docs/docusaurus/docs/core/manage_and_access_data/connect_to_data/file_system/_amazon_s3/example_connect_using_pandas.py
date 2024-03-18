"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -k "connect_to_data/filesystem/_amazon_s3/example_connect_using_pandas" tests/integration/test_script_runner.py
```
"""
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Full example code">
# Import the `great_expectations` library and get a Data Context.
import great_expectations as gx

context = gx.get_context()

# Define the parameters for your Data Source
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Data Source args">
datasource_name = "my_s3_datasource"
bucket_name = "my_bucket"
boto3_options = {}
# </snippet>

# Create the Data Source
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Create Data Source">
datasource = context.sources.add_pandas_s3(
    name=datasource_name,
    bucket=bucket_name,
    boto3_options=boto3_options
)
# </snippet>

# This is a testing line to verify that the Data Source created
# successfully.  You may disregard it.
assert datasource_name in context.datasources

# Define the Data Asset's parameters.
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Data Asset args">
asset_name = "my_taxi_data_asset"
s3_prefix = "data/taxi_yellow_tripdata_samples/"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
# </snippet>

# Add the Data Asset to the Data Source
# <snippet name="docs/docusaurus/docs/core/manage_and_access_data/connect_to_data/file_system/_amazon_s3/example_connect_using_pandas.py Add Data Asset">
data_asset = datasource.add_csv_asset(
    name=asset_name,
    batching_regex=batching_regex,
    s3_prefix=s3_prefix
)
# </snippet>

# These are testing lines to verify that the Data Asset was created
# successfully and added to the Data Source.  You may disregard them.
assert data_asset
assert datasource.get_asset_names() == {"my_taxi_data_asset"}
#< /snippet>