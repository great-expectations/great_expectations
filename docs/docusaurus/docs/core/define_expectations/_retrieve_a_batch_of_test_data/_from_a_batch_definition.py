# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - full example">
import great_expectations as gx

context = gx.get_context()

# Retrieve the Batch Definition from the Data Context
# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve Batch Definition">
data_source_name = "my_filesystem_data_source"
data_asset_name = "my_file_data_asset"
batch_definition_name = "yellow_tripdata_sample_daily"
batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)
# </snippet>

# Retrieve the first valid Batch of data from the Batch Definition
# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve most recent Batch">
batch = batch_definition.get_batch()
# </snippet>

# Or use a Batch Parameter dictionary to specify a Batch to retrieve
# These are sample Batch Parameter dictionaries:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - example Batch Parameters">
yearly_batch_parameters = {"year": 2020}
monthly_batch_parameters = {"year": 2020, "month": 1}
daily_batch_parameters = {"year": 2020, "month": 1, "day": 14}
# </snippet>

# This code retrieves the Batch:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve specific Batch">
batch = batch_definition.get_batch(
    batch_parameters={"year": 2020, "month": 1, "day": 14}
)
# </snippet>

# Verify that the Batch contains records
# <snippet name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - verify populated Batch">
print(batch.head())
# </snippet>
# </snippet>
