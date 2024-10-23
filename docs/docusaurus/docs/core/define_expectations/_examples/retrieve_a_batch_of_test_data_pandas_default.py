"""
This is an example script for how to retrieve a Batch of data for testing individual Expectations or engaging in data exploration.

To test, run:
pytest --docs-tests -k "doc_example_retrieve_a_batch_of_test_data_with_pandas_default" tests/integration/test_script_runner.py
"""


def set_up_context_for_example(context):
    pass


# EXAMPLE SCRIPT STARTS HERE:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - full code example">
import great_expectations as gx

context = gx.get_context()
# Hide this
set_up_context_for_example(context)

# Provide the path to a data file:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - provide the path to a data file">
file_path = "./data/folder_with_data/yellow_tripdata_sample_2019-01.csv"
# </snippet>

# Use the `pandas_default` Data Source to read the file:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - read data into Batch with pandas_default Data Source">
sample_batch = context.data_sources.pandas_default.read_csv(file_path)
# </snippet>

# Verify that data was read into `sample_batch`:
# <snippet name="docs/docusaurus/docs/core/define_expectations/_examples/retrieve_a_batch_of_test_data_pandas_default.py - verify data was read into a Batch">
print(sample_batch.head())
# </snippet>
# </snippet>
