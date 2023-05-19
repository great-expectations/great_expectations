"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_data_on_s3_using_pandas" tests/integration/test_script_runner.py
```
"""
import os
from typing import List

import pandas as pd

from moto import mock_s3
from botocore.client import BaseClient

from great_expectations.compatibility import aws

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py get_context">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py define_add_pandas_s3_args">
datasource_name = "my_s3_datasource"
bucket_name = "my_bucket"
boto3_options = {}
# </snippet>

test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

keys: List[str] = [
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2021-11.csv",
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2021-12.csv",
    "data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2023-01.csv",
]

with mock_s3():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    client: BaseClient = aws.boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket_name)
    for key in keys:
        client.put_object(
            Bucket=bucket_name,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key,
        )

    # Python
    # <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py create_datasource">
    datasource = context.sources.add_pandas_s3(
        name=datasource_name, bucket=bucket_name, boto3_options=boto3_options
    )
    # </snippet>

    assert datasource_name in context.datasources

    # Python
    # <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_data_on_s3_using_pandas.py add_asset">
    asset_name = "my_taxi_data_asset"
    s3_prefix = "data/taxi_yellow_tripdata_samples/"
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv"
    data_asset = datasource.add_csv_asset(
        name=asset_name, batching_regex=batching_regex, s3_prefix=s3_prefix
    )
    # </snippet>

    assert data_asset

    assert datasource.get_asset_names() == {"my_taxi_data_asset"}

    my_batch_request = data_asset.build_batch_request({"year": "2021", "month": "11"})
    batches = data_asset.get_batch_list_from_batch_request(my_batch_request)
    assert len(batches) == 1
    assert set(batches[0].columns()) == {
        "col1",
        "col2",
    }
