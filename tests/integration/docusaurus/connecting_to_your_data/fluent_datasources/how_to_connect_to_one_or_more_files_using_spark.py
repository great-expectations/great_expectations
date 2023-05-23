"""
To run this code as a local test, use the following console command:
```
pytest -v --docs-tests -m integration -k "how_to_connect_to_one_or_more_files_using_spark" tests/integration/test_script_runner.py
```
"""
import pathlib


import great_expectations as gx

context = gx.get_context()

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py define_add_spark_filesystem_args">
datasource_name = "my_new_datasource"
path_to_folder_containing_csv_files = "<INSERT_PATH_TO_FILES_HERE>"
# </snippet>

path_to_folder_containing_csv_files = str(
    pathlib.Path(
        gx.__file__,
        "..",
        "..",
        "tests",
        "test_sets",
        "taxi_yellow_tripdata_samples",
    ).resolve(strict=True)
)

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py create_datasource">
datasource = context.sources.add_spark_filesystem(
    name=datasource_name, base_directory=path_to_folder_containing_csv_files
)
# </snippet>

assert datasource_name in context.datasources

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py define_add_csv_asset_args">
asset_name = "my_taxi_data_asset"
batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
# </snippet>

# Python
# <snippet name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_one_or_more_files_using_spark.py add_asset">
datasource.add_csv_asset(
    name=asset_name, batching_regex=batching_regex, header=True, infer_schema=True
)
# </snippet>

assert datasource.get_asset_names() == {"my_taxi_data_asset"}

my_asset = datasource.get_asset(asset_name)
assert my_asset

my_batch_request = my_asset.build_batch_request({"year": "2019", "month": "03"})
batches = my_asset.get_batch_list_from_batch_request(my_batch_request)
assert len(batches) == 1
assert set(batches[0].columns()) == {
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
}
