# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - full_example">
from great_expectations import gx

context = gx.get_context()

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - retrieve Data Asset">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
file_data_asset = context.get_data_source(data_source_name).get_asset(data_asset_name)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - define Batch Definition parameters">
batch_definition_name = "monthly_yellow_tripdata_sample"
batch_definition_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"

batch_definition = file_data_asset.add_batch_definition_monthly(
    name=batch_definition_name, regex=batch_definition_regex
)
# </snippet>

# <snippet name="docs/docusaurus/docs/core/connect_to_data/filesystem_data/_create_a_batch_definition/_examples/_file_partitioned_monthly.py - retrieve Batch and verify">
batch = batch_definition.get_batch(batch_parameters={"year": 2020, "month": 1})
batch.head()
# </snippet>
# </snippet>
