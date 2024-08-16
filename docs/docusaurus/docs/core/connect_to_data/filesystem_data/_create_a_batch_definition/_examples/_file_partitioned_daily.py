# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - full_example">
from pathlib import Path

import great_expectations as gx

context = gx.get_context(mode="ephemeral")

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - retrieve Data Asset">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
# file_data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
data_source = context.data_sources.add_spark_filesystem(data_source_name, base_directory=Path("data"))
file_data_asset = data_source.add_directory_csv_asset(data_asset_name, data_directory=Path("folder_with_data"))
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - add Batch Definition">
batch_definition_name = "daily_yellow_tripdata_sample"
batch_definition_regex = r"folder_with_data/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\.csv"

batch_definition = file_data_asset.add_batch_definition_daily(
    name=batch_definition_name, column="pickup_datetime"
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_daily.py - retrieve Batch and verify">
batch = batch_definition.get_batch(
    batch_parameters={"year": "2019", "month": "01", "day": "01"}
)
batch.head()
# </snippet>
# </snippet>
