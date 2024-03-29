# <snippet name="version-0.17.23 docs/docusaurus/versioned_docs/version-0.17.23/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py setup">
import pandas

import great_expectations as gx

context = gx.get_context()
# </snippet>

# <snippet name="version-0.17.23 docs/docusaurus/versioned_docs/version-0.17.23/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py read_dataframe">
df = pandas.read_csv("./data/yellow_tripdata_sample_2019-01.csv")

validator = context.sources.add_pandas("taxi_datasource").read_dataframe(
    df, asset_name="taxi_frame", batch_metadata={"year": "2019", "month": "01"}
)
validator.save_expectation_suite()  # this allows the checkpoint to reference the expectation suite

checkpoint = context.add_or_update_checkpoint(
    name="my_taxi_validator_checkpoint", validator=validator
)

checkpoint_result = checkpoint.run()
# </snippet>

# NOTE: The following code is only for testing and can be ignored
assert checkpoint_result["success"]

# clean-up between examples
context.delete_datasource("taxi_datasource")

# <snippet name="version-0.17.23 docs/docusaurus/versioned_docs/version-0.17.23/guides/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py add_dataframe">
dataframe_asset = context.sources.add_pandas(
    "my_taxi_validator_checkpoint"
).add_dataframe_asset(
    name="taxi_frame", dataframe=df, batch_metadata={"year": "2019", "month": "01"}
)
context.add_or_update_expectation_suite("my_expectation_suite")

batch_request = dataframe_asset.build_batch_request()

checkpoint = context.add_or_update_checkpoint(
    name="my_taxi_dataframe_checkpoint",
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)

checkpoint_result = checkpoint.run()
# </snippet>

# NOTE: The following code is only for testing and can be ignored
assert checkpoint_result["success"]
