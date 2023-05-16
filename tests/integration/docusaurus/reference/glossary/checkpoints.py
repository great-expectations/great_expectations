from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent import DataAsset
from great_expectations.checkpoint import SimpleCheckpoint

# <snippet name="tests/integration/docusaurus/reference/glossary/checkpoints.py setup">
import great_expectations as gx

context = gx.get_context()
# </snippet>

# to open Data Docs, we need validation results which we get by creating a suite and running a checkpoint
datasource: Datasource = context.get_datasource("taxi_datasource")
asset: DataAsset = datasource.get_asset("yellow_tripdata")
batch_request = asset.build_batch_request()
validator = context.get_validator(batch_request=batch_request)

validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)

taxi_suite = validator.get_expectation_suite()
taxi_suite.expectation_suite_name = "taxi_suite"

context.add_expectation_suite(expectation_suite=taxi_suite)

# <snippet name="tests/integration/docusaurus/reference/glossary/checkpoints.py create_and_run">
checkpoint = SimpleCheckpoint(
    name="taxi_checkpoint",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name="taxi_suite",
)
checkpoint.run()
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/glossary/checkpoints.py save">
context.add_checkpoint(checkpoint=checkpoint)
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/glossary/checkpoints.py retrieve_and_run">
checkpoint = context.get_checkpoint("taxi_checkpoint")
checkpoint.run()
# </snippet>

assert True
