import great_expectations as gx
from great_expectations.datasource.fluent import Datasource
from great_expectations.datasource.fluent import DataAsset
from great_expectations.checkpoint import SimpleCheckpoint

context = gx.get_context()

# to open data docs, we need validation results which we get by creating a suite and running a checkpoint
datasource: Datasource = context.get_datasource("taxi_datasource")
asset: DataAsset = datasource.get_asset("yellow_tripdata")
batch_request = asset.build_batch_request()
validator = context.get_validator(batch_request=batch_request)

validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", auto=True)

taxi_suite = validator.get_expectation_suite()
taxi_suite.expectation_suite_name = "taxi_suite"

context.add_expectation_suite(expectation_suite=taxi_suite)

checkpoint = SimpleCheckpoint(
    name="taxi_checkpoint",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name="taxi_suite",
)
checkpoint.run()

# <snippet name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs">
context.build_data_docs()
context.open_data_docs()
# </snippet>

# <snippet name="tests/integration/docusaurus/reference/glossary/data_docs.py data_docs_site">
site_name = "new_site_name"
context.build_data_docs(site_names=site_name)
context.open_data_docs(site_name=site_name)
# </snippet>

assert True
