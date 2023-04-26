# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py imports">
import pandas

import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint

# </snippet>

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py get_context">
context = gx.get_context()
# </snippet>

# setup
context.add_or_update_expectation_suite("my_expectation_suite")

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py simple_checkpoint_read_dataframe">
df = pandas.read_csv("./data/yellow_tripdata_sample_2019-01.csv")

validator = context.sources.add_pandas("taxi_datasource").read_dataframe(
    df, asset_name="taxi_frame", batch_metadata={"year": "2019", "month": "01"}
)

checkpoint = SimpleCheckpoint(
    name="my_taxi_validator_checkpoint",
    data_context=context,
    validator=validator,
)

checkpoint_result = checkpoint.run()
# </snippet>

# NOTE: The following code is only for testing and can be ignored by
assert checkpoint_result["success"]

# clean-up between examples
context.delete_datasource("taxi_datasource")

# <snippet name="tests/integration/docusaurus/validation/checkpoints/how_to_pass_an_in_memory_dataframe_to_a_checkpoint.py simple_checkpoint_add_dataframe">
dataframe_asset = context.sources.add_pandas(
    "my_taxi_validator_checkpoint"
).add_dataframe_asset(
    df, name="taxi_frame", batch_metadata={"year": "2019", "month": "01"}
)

batch_request = dataframe_asset.build_batch_request()

checkpoint = SimpleCheckpoint(
    name="my_taxi_dataframe_checkpoint",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name="my_expectation_suite",
)

checkpoint_result = checkpoint.run()
# </snippet>

# NOTE: The following code is only for testing and can be ignored by
assert checkpoint_result["success"]
